package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all content ids of given service id
// Reload when changes to service_content or service are done
type Services struct {
	sync.RWMutex
	ById map[int64]Service
}

type Service struct {
	Id                      int64   `json:"id,omitempty"`
	Price                   float64 `json:"price,omitempty"`
	PaidHours               int     `json:"paid_hours,omitempty"`       // rejected rule
	DelayHours              int     `json:"delay_hours,omitempty"`      // repeat charge delay
	KeepDays                int     `json:"keep_days,omitempty"`        // for retries
	SMSOnUnsubscribe        string  `json:"sms_on_subscribe,omitempty"` // if empty, do not send
	SMSOnContent            string  `json:"sms_on_content,omitempty"`
	SendContentTextTemplate string  `json:"sms_content_template,omitempty"`
	SMSOnSubscribe          string  `json:"sms_on_unsubscribe,omitempty"`
	PeriodicAllowedFrom     int     `json:"periodic_allowed_from,omitempty"` // send content in sms allowed from and to times.
	PeriodicAllowedTo       int     `json:"periodic_allowed_to,omitempty"`
	PeriodicDays            string  `json:"periodic_days,omitempty"` // days of subscription is alive
	InactiveDays            int     `json:"inactive_days,omitempty"` // days of unsuccessful charge turns subscription into inactive state
	GraceDays               int     `json:"grace_days,omitempty"`    // days in end of subscription period where always must be charged OK
	ContentIds              []int64 `json:"content_ids,omitempty"`
}

type ServiceContent struct {
	IdService int64
	IdContent int64
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

func (s *Services) Reload() (err error) {
	s.Lock()
	defer s.Unlock()

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"price, "+
		"paid_hours, "+
		"delay_hours, "+
		"keep_days, "+
		"sms_on_subscribe, "+
		"sms_on_content, "+
		"sms_content_template, "+
		"sms_on_unsubscribe, "+
		"days, "+
		"inactive_days, "+
		"grace_days, "+
		"allowed_from, "+
		"allowed_to, "+
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
			&srv.Id,
			&srv.Price,
			&srv.PaidHours,
			&srv.DelayHours,
			&srv.KeepDays,
			&srv.SMSOnSubscribe,
			&srv.SMSOnContent,
			&srv.SendContentTextTemplate,
			&srv.SMSOnUnsubscribe,
			&srv.PeriodicDays,
			&srv.InactiveDays,
			&srv.GraceDays,
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

	serviceIdsStr := []string{}
	for _, v := range svcs {
		serviceIdsStr = append(serviceIdsStr, strconv.FormatInt(v.Id, 10))
	}
	log.Debugf("get service content ids for: %s", strings.Join(serviceIdsStr, ", "))

	query = fmt.Sprintf("SELECT "+
		"id_service, "+
		"id_content "+
		"FROM %sservice_content "+
		"WHERE status = $1 AND "+
		"id_service = any($2::integer[])", Svc.dbConf.TablePrefix)

	rows, err = Svc.db.Query(query, ACTIVE_STATUS, "{"+strings.Join(serviceIdsStr, ", ")+"}")
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	serviceContentIds := make(map[int64][]int64)
	for rows.Next() {
		var serviceContent ServiceContent
		if err = rows.Scan(
			&serviceContent.IdService,
			&serviceContent.IdContent,
		); err != nil {
			err = fmt.Errorf("rows.Scan %s", err.Error())
			return
		}
		if _, ok := serviceContentIds[serviceContent.IdService]; !ok {
			serviceContentIds[serviceContent.IdService] = []int64{}
		}
		serviceContentIds[serviceContent.IdService] = append(serviceContentIds[serviceContent.IdService], serviceContent.IdContent)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Error: %s", err.Error())
		return
	}

	s.ById = make(map[int64]Service)
	for _, v := range svcs {
		if contentIds, ok := serviceContentIds[v.Id]; ok {
			v.ContentIds = contentIds
		}
		s.ById[v.Id] = v
	}

	log.Debugf("services: %#v", s.ById)

	return nil
}

func (s *Services) Get(serviceId int64) (contentIds []int64) {
	if svc, ok := s.ById[serviceId]; ok {
		return svc.ContentIds
	}
	return []int64{}
}
