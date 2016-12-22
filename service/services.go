package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
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
	Id                      int64
	Price                   float64
	PaidHours               int
	DelayHours              int
	KeepDays                int
	SendNotPaidTextEnabled  bool
	NotPaidText             string
	PeriodicAllowedFrom     int
	PeriodicAllowedTo       int
	SendContentTextTemplate string
	PeriodicDays            []string
	ContentIds              []int64
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

var allowedDays = string{"", "any", "sun", "mon", "tue", "wed", "thu", "fri", "sat"}

func (scd Days) ok(days string) bool {
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
		"not_paid_text, "+
		"send_not_paid_text_enabled, "+
		"allowed_from, "+
		"allowed_to, "+
		"text_template, "+
		"periodic_days "+
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
	var allowedTime string
	var days string
	for rows.Next() {
		var srv Service
		if err = rows.Scan(
			&srv.Id,
			&srv.Price,
			&srv.PaidHours,
			&srv.DelayHours,
			&srv.KeepDays,
			&srv.NotPaidText,
			&srv.SendNotPaidTextEnabled,
			&srv.PeriodicAllowedFrom,
			&srv.PeriodicAllowedTo,
			&srv.SendContentTextTemplate,
			&days,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		var days Days
		if err = json.Unmarshal([]byte(allowedTime), &days); err != nil {
			err = fmt.Errorf("json.Unmarshal: %s", err.Error())
			return
		}
		if !days.ok(days) {
			err = fmt.Errorf("send content days: %s, allowed: %s", strings.Join(",", days), strings.Join(", ", allowedDays))
			return
		}
		srv.PeriodicDays = days

		svcs = append(svcs, srv)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	svcMap := make(map[int64]Service)
	for _, v := range svcs {
		svcMap[v.Id] = v
	}

	serviceIdsStr := []string{}
	for _, v := range svcs {
		serviceIdsStr = append(serviceIdsStr, strconv.FormatInt(v.Id, 10))
	}
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

	var serviceContentAr []ServiceContent
	for rows.Next() {
		var serviceContent ServiceContent
		if err = rows.Scan(
			&serviceContent.IdService,
			&serviceContent.IdContent,
		); err != nil {
			err = fmt.Errorf("rows.Scan %s", err.Error())
			return
		}
		serviceContentAr = append(serviceContentAr, serviceContent)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Error: %s", err.Error())
		return
	}

	s.ById = make(map[int64]Service)

	for _, serviceContent := range serviceContentAr {
		srv, ok := s.ById[serviceContent.IdService]
		if !ok {
			s.ById[serviceContent.IdService] = Service{}
		}
		copy(srv, svcMap[serviceContent.IdService])

		srv.ContentIds = append(srv.ContentIds, serviceContent.IdContent)

		s.ById[serviceContent.IdService] = srv
	}
	return nil
}
func (s *Services) Get(serviceId int64) (contentIds []int64) {
	if svc, ok := s.ById[serviceId]; ok {
		return svc.ContentIds
	}
	return []int64{}
}
