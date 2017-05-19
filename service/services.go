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

	client "github.com/linkit360/go-acceptor-client"
	acceptor "github.com/linkit360/go-acceptor-structs"
	m "github.com/linkit360/go-utils/metrics"
)

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all content ids of given service id
// Reload when changes to service_content or service are done

type Services interface {
	Reload() error
	GetByCode(string) (acceptor.Service, error)
	GetAll() map[string]acceptor.Service
}

type ServicesConfig struct {
	Enabled          bool `yaml:"enabled"`
	FromControlPanel bool `yaml:"from_control_panel"`
}

type services struct {
	sync.RWMutex
	conf      ServicesConfig
	ById      map[string]acceptor.Service
	loadError prometheus.Gauge
	loadCache prometheus.Gauge
}

func initServices(appName string, servConfig ServicesConfig) Services {
	svcs := &services{
		conf:      servConfig,
		loadError: m.PrometheusGauge(appName, "services_load", "error", "load services error"),
		loadCache: m.PrometheusGauge(appName, "services", "cache", "load services cache"),
	}

	if !svcs.conf.FromControlPanel {
		return svcs
	}

	svcs.loadCache = m.PrometheusGauge(appName, "services", "cache", "cache services used")
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

	var svcs []acceptor.Service
	for rows.Next() {
		var srv acceptor.Service
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
	s.ById = make(map[string]acceptor.Service, len(svcs))
	for _, v := range svcs {
		if contentIds, ok := serviceContentIds[v.Code]; ok {
			v.ContentIds = contentIds
		}
		s.ById[v.Code] = v
	}

	return nil
}

func (s *services) Reload() (err error) {
	s.Lock()
	defer s.Unlock()

	defer log.Debugf("services: %#v", s.ById)

	s.loadCache.Set(0)
	s.loadError.Set(0)
	if s.conf.FromControlPanel {
		var byUuid map[string]acceptor.Service
		byUuid, err = client.GetServices(Svc.conf.ProviderName)
		if err == nil {
			s.loadCache.Set(0)
			return
		}

		s.ById = make(map[string]acceptor.Service, len(byUuid))
		for _, v := range byUuid {
			s.ById[v.Code] = v
		}
		s.loadError.Set(1.0)
		log.Error(err.Error())
	}

	s.loadCache.Set(1.0)
	s.loadError.Set(0)
	if err = s.loadFromCache(); err != nil {
		s.loadError.Set(1.0)
		err = fmt.Errorf("s.getFromCache: %s", err.Error())
		return
	}
	return nil
}

func (s *services) GetByCode(serviceCode string) (acceptor.Service, error) {
	if svc, ok := s.ById[serviceCode]; ok {
		return svc, nil
	}
	return acceptor.Service{}, errNotFound()
}

func (s *services) GetAll() map[string]acceptor.Service {
	return s.ById
}
