package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/linkit360/go-utils/metrics"
)

type Service struct {
	Id                  string  `json:"id,omitempty"`            // unique id
	Code                string  `json:"code,omitempty"`          // previous service id
	Price               int     `json:"price,omitempty"`         // в целых рублях
	RetryDays           int     `json:"retry_days,omitempty"`    // for retries - days to keep retries, for periodic - subscription is alive
	InactiveDays        int     `json:"inactive_days,omitempty"` // days of unsuccessful charge turns subscription into inactive state
	GraceDays           int     `json:"grace_days,omitempty"`    // days in end of subscription period where always must be charged OK
	PaidHours           int     `json:"paid_hours,omitempty"`    // rejected rule
	DelayHours          int     `json:"delay_hours,omitempty"`   // repeat charge delay
	SMSOnSubscribe      string  `json:"sms_on_unsubscribe,omitempty"`
	SMSOnCharged        string  `json:"sms_on_charged,omitempty"`
	SMSOnUnsubscribe    string  `json:"sms_on_subscribe,omitempty"` // if empty, do not send
	SMSOnContent        string  `json:"sms_on_content,omitempty"`
	SMSOnRejected       string  `json:"sms_on_rejected,omitempty"`
	SMSOnBlackListed    string  `json:"sms_on_blacklisted,omitempty"`
	SMSOnPostPaid       string  `json:"sms_on_postpaid,omitempty"`
	PeriodicAllowedFrom int     `json:"periodic_allowed_from,omitempty"` // send content in sms allowed from and to times.
	PeriodicAllowedTo   int     `json:"periodic_allowed_to,omitempty"`
	PeriodicDays        string  `json:"periodic_days,omitempty"` // days of week to charge subscriber
	MinimalTouchTimes   int     `json:"minimal_touch_times,omitempty"`
	ContentIds          []int64 `json:"content_ids,omitempty"`
}

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all content ids of given service id
// Reload when changes to service_content or service are done

type Services interface {
	Reload() error
	GetByCode(string) (Service, error)
	GetAll() map[string]Service
}

type ServicesConfig struct {
	FromControlPanel bool   `yaml:"from_control_panel"`
	WebHook          string `yaml:"web_hook" default:"http://localhost:50306/update"`
}

type services struct {
	sync.RWMutex
	conf      ServicesConfig
	ByCode    map[string]Service
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

	return svcs
}

type ServiceContent struct {
	ServiceCode string
	IdContent   int64
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

	var svcs []Service
	for rows.Next() {
		var srv Service
		if err = rows.Scan(
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

	serviceContentIds := make(map[string][]int64)
	for rows.Next() {
		var serviceContent ServiceContent
		if err = rows.Scan(
			&serviceContent.ServiceCode,
			&serviceContent.IdContent,
		); err != nil {
			err = fmt.Errorf("rows.Scan %s", err.Error())
			return
		}
		if _, ok := serviceContentIds[serviceContent.ServiceCode]; !ok {
			serviceContentIds[serviceContent.ServiceCode] = []int64{}
		}
		serviceContentIds[serviceContent.ServiceCode] = append(serviceContentIds[serviceContent.ServiceCode], serviceContent.IdContent)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Error: %s", err.Error())
		return
	}
	s.ByCode = make(map[string]Service, len(svcs))
	for _, v := range svcs {
		if contentIds, ok := serviceContentIds[v.Code]; ok {
			v.ContentIds = contentIds
		}
		s.ByCode[v.Code] = v
	}

	return nil
}

func (s *services) Reload() (err error) {
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

func (s *services) GetByCode(serviceCode string) (Service, error) {
	if svc, ok := s.ByCode[serviceCode]; ok {
		return svc, nil
	}
	return Service{}, errNotFound()
}

func (s *services) GetAll() map[string]Service {
	return s.ByCode
}

func (s *services) ShowLoaded() {
	byCode, _ := json.Marshal(s.ByCode)

	log.WithFields(log.Fields{
		"action": "services",
		"id":     string(byCode),
	}).Debug("")
}
