package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	m "github.com/linkit360/go-utils/metrics"
	"github.com/linkit360/xmp-api/src/client"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

type Services interface {
	Reload() error
	Apply(map[string]xmp_api_structs.Service)
	Update(xmp_api_structs.Service) error
	GetByCode(string) (xmp_api_structs.Service, error)
	GetById(string) (xmp_api_structs.Service, error)
	GetAll() map[string]xmp_api_structs.Service
	GetJson() string
	ShowLoaded()
}

type ServicesConfig struct {
	FromControlPanel bool   `yaml:"from_control_panel"`
	WebHook          string `yaml:"web_hook" default:""`
}

type services struct {
	sync.RWMutex
	conf      ServicesConfig
	ByCode    map[string]xmp_api_structs.Service
	ByUUID    map[string]xmp_api_structs.Service
	loadError prometheus.Gauge
	notFound  m.Gauge
}

func initServices(appName string, servConfig ServicesConfig) Services {
	svcs := &services{
		conf:      servConfig,
		loadError: m.PrometheusGauge(appName, "services_load", "error", "load services error"),
		notFound:  m.NewGauge(appName, "service", "not_found", "service not found error"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			svcs.notFound.Update()
		}
	}()

	go svcs.catchUpdates(xmp_api_client.ChanServices)

	return svcs
}

func (as *services) catchUpdates(updates <-chan xmp_api_structs.Service) {
	as.loadError.Set(0)
	for s := range updates {
		if err := as.Update(s); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"id":    s.Id,
			}).Error("failed to update service")
			as.loadError.Set(1)
		} else {
			log.WithFields(log.Fields{
				"id": s.Id,
			}).Info("update service done")
		}
		as.ShowLoaded()
	}
}

func (s *services) Update(acceptorService xmp_api_structs.Service) error {
	servJson, _ := json.Marshal(acceptorService)
	log.WithFields(log.Fields{
		"id":      acceptorService.Id,
		"service": string(servJson),
	}).Debug("service")

	if acceptorService.Id == "" {
		return fmt.Errorf("service id is empty%s", "")
	}
	if acceptorService.Code == "" {
		return fmt.Errorf("service code is empty%s", "")
	}
	if err := s.setupContent(acceptorService); err != nil {
		return fmt.Errorf("update content error: %s", err.Error())
	}
	if acceptorService.Status == 0 {
		s.Lock()
		delete(s.ByUUID, acceptorService.Id)
		delete(s.ByCode, acceptorService.Code)
		s.Unlock()
		log.WithFields(log.Fields{
			"id": acceptorService.Id,
		}).Debug("service deleted")
		return nil
	}

	if s.conf.WebHook != "" {
		resp, err := http.Get(s.conf.WebHook)
		if err != nil || resp.StatusCode != 200 {
			fields := log.Fields{
				"webhook": s.conf.WebHook,
			}
			if resp != nil {
				fields["code"] = resp.Status
			}
			if err != nil {
				fields["error"] = err.Error()
			}
			log.WithFields(fields).Error("hook failed")
		} else {
			log.WithFields(log.Fields{
				"webhook": s.conf.WebHook,
			}).Debug("service update webhook done")
		}
	}

	s.Lock()
	defer s.Unlock()
	var contentIds []string
	for _, cids := range acceptorService.Contents {
		contentIds = append(contentIds, cids.Id)
	}
	acceptorService.ContentIds = contentIds
	if s.ByCode == nil {
		s.ByCode = make(map[string]xmp_api_structs.Service)
	}
	if s.ByUUID == nil {
		s.ByUUID = make(map[string]xmp_api_structs.Service)
	}
	s.ByUUID[acceptorService.Id] = acceptorService
	s.ByCode[acceptorService.Code] = acceptorService
	return nil
}

func (s *services) setupContent(acceptorService xmp_api_structs.Service) error {
	// проверить весь контент и обновить только то, что новенькое -
	// в панели управления запрещено редактировать контент
	// поэтому у отредактированных контентов - новый айдишник
	var newContents []xmp_api_structs.Content
	for _, serviceContent := range acceptorService.Contents {
		if _, err := Svc.Contents.GetById(serviceContent.Id); err != nil {
			if serviceContent.Id == "" {
				return fmt.Errorf("ContentId is empty%s", "")
			}
			if serviceContent.Name == "" {
				return fmt.Errorf("Content Name is empty%s", "")
			}
			newContents = append(newContents, serviceContent)
		}
	}
	if len(newContents) > 0 {
		log.WithFields(log.Fields{
			"content": len(newContents),
			"id":      acceptorService.Id,
			"action":  "update service",
		}).Debug("found new content")
	} else {
		log.WithFields(log.Fields{
			"id":     acceptorService.Id,
			"action": "update service",
		}).Debug("no new content")
	}

	if err := Svc.Contents.Update(newContents); err != nil {
		return fmt.Errorf("Update: %s", err.Error())
	}
	return nil
}

type ServiceContent struct {
	ServiceCode string
	ContentCode string
}

type AllowedTime struct {
	From time.Time `json:"from,omitempty"`
	To   time.Time `json:"to,omitempty"`
}

type Days []string

var allowedDays = []string{"", "any", "sun", "mon", "tue", "wed", "thu", "fri", "sat"}

func (scd Days) ok(days []string) bool {
	for _, d := range days {
		found := false
		for _, ad := range allowedDays {
			if d == ad {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (s *services) loadFromCache() (err error) {
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"id, "+
		"price, "+
		"retry_days, "+
		"inactive_days, "+
		"grace_days, "+
		"paid_hours, "+
		"delay_hours, "+
		"minimal_touch_times, "+
		"sms_on_subscribe, "+
		"sms_on_content, "+
		"sms_on_unsubscribe, "+
		"sms_on_rejected, "+
		"sms_on_blacklisted, "+
		"sms_on_postpaid, "+
		"sms_on_charged, "+
		"days, "+
		"allowed_from, "+
		"allowed_to "+
		"FROM %sservices "+
		"WHERE status = $1",
		Svc.dbConf.TablePrefix,
	)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var svcs []xmp_api_structs.Service
	for rows.Next() {
		var srv xmp_api_structs.Service
		if err = rows.Scan(
			&srv.Id,
			&srv.Code,
			&srv.Price,
			&srv.RetryDays,
			&srv.InactiveDays,
			&srv.GraceDays,
			&srv.PaidHours,
			&srv.DelayHours,
			&srv.MinimalTouchTimes,
			&srv.SMSOnSubscribe,
			&srv.SMSOnContent,
			&srv.SMSOnUnsubscribe,
			&srv.SMSOnRejected,
			&srv.SMSOnBlackListed,
			&srv.SMSOnPostPaid,
			&srv.SMSOnCharged,
			&srv.PeriodicDays,
			&srv.PeriodicAllowedFrom,
			&srv.PeriodicAllowedTo,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		var days Days
		if err = json.Unmarshal([]byte(srv.PeriodicDays), &days); err != nil {
			err = fmt.Errorf("json.Unmarshal: %s", err.Error())
			return
		}
		if !days.ok(days) {
			err = fmt.Errorf("send charge days: %s, allowed: %s",
				strings.Join(days, ","), strings.Join(allowedDays, ","))
			return
		}
		svcs = append(svcs, srv)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	log.Debugf("len %d, svcs: %#v", len(svcs), svcs)

	serviceCodes := []string{}
	for _, v := range svcs {
		serviceCodes = append(serviceCodes, v.Code)
	}
	log.Debugf("get service content ids for: %s", strings.Join(serviceCodes, ", "))

	query = fmt.Sprintf("SELECT "+
		"id_service, "+
		"id_content "+
		"FROM %sservice_content "+
		"WHERE status = $1 AND "+
		"id_service = any($2::integer[])", Svc.dbConf.TablePrefix)

	rows, err = Svc.db.Query(query, ACTIVE_STATUS, "{"+strings.Join(serviceCodes, ", ")+"}")
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	serviceContentIds := make(map[string][]string)
	for rows.Next() {
		var serviceContent ServiceContent
		if err = rows.Scan(
			&serviceContent.ServiceCode,
			&serviceContent.ContentCode,
		); err != nil {
			err = fmt.Errorf("rows.Scan %s", err.Error())
			return
		}
		if _, ok := serviceContentIds[serviceContent.ServiceCode]; !ok {
			serviceContentIds[serviceContent.ServiceCode] = []string{}
		}
		serviceContentIds[serviceContent.ServiceCode] =
			append(serviceContentIds[serviceContent.ServiceCode], serviceContent.ContentCode)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Error: %s", err.Error())
		return
	}

	serviceContents := make(map[string]xmp_api_structs.Service, len(svcs))
	for _, v := range svcs {
		if contentIds, ok := serviceContentIds[v.Code]; ok {
			v.ContentIds = contentIds
		}
		serviceContents[v.Id] = v
	}

	s.ByUUID = make(map[string]xmp_api_structs.Service, len(svcs))
	s.ByCode = make(map[string]xmp_api_structs.Service, len(s.ByUUID))
	for _, v := range serviceContents {
		s.ByUUID[v.Id] = v
		s.ByCode[v.Code] = v
	}
	return nil
}

func (s *services) Reload() (err error) {
	if s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}
	s.Lock()
	defer s.Unlock()
	defer s.ShowLoaded()

	s.loadError.Set(0)
	if err = s.loadFromCache(); err != nil {
		s.loadError.Set(1.0)
		err = fmt.Errorf("s.getFromCache: %s", err.Error())
		return
	}

	return nil
}

func (s *services) Apply(svcs map[string]xmp_api_structs.Service) {
	s.ByUUID = make(map[string]xmp_api_structs.Service, len(svcs))
	s.ByCode = make(map[string]xmp_api_structs.Service, len(svcs))
	s.loadError.Set(0)
	for _, v := range svcs {
		if err := s.Update(v); err == nil {
			log.WithField("id", v.Id).Debug("update service ok")
		} else {
			s.loadError.Set(1)
			log.WithFields(log.Fields{
				"id":    v.Id,
				"error": err.Error(),
			}).Error("update service")
		}
	}
}

func (s *services) GetById(serviceId string) (xmp_api_structs.Service, error) {
	if svc, ok := s.ByUUID[serviceId]; ok {
		return svc, nil
	}
	return xmp_api_structs.Service{}, errNotFound()
}

func (s *services) GetByCode(serviceCode string) (xmp_api_structs.Service, error) {
	if svc, ok := s.ByCode[serviceCode]; ok {
		return svc, nil
	}
	return xmp_api_structs.Service{}, errNotFound()
}

func (s *services) GetAll() map[string]xmp_api_structs.Service {
	return s.ByCode
}

func (s *services) ShowLoaded() {
	byCode, _ := json.Marshal(s.ByCode)
	byUUID, _ := json.Marshal(s.ByUUID)

	log.WithFields(log.Fields{
		"len":    len(s.ByUUID),
		"bycode": string(byCode),
		"byuuid": string(byUUID),
	}).Debug("services")
	Svc.Contents.ShowLoaded()
}

func (s *services) GetJson() string {
	sJson, _ := json.Marshal(s.ByUUID)
	return string(sJson)
}
