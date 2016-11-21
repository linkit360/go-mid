package service

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

// Tasks:
// Keep in memory all active campaigns
// Allow to get a service_id by campaign hash fastly
// Reload when changes to campaigns are done
type Campaigns struct {
	sync.RWMutex
	ByHash map[string]Campaign
	ByLink map[string]Campaign
}
type Campaign struct {
	Hash        string
	Link        string
	PageWelcome string
	Id          int64
	ServiceId   int64
}

func (s *Campaigns) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("campaign reload...")
	begin := time.Now()
	defer func(err error) {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("campaign reload")
	}(err)

	query := fmt.Sprintf("select id, hash, link, page_welcome, service_id_1 from %scampaigns where status = $1",
		Svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var records []Campaign
	for rows.Next() {
		record := Campaign{}
		if err = rows.Scan(
			&record.Id,
			&record.Hash,
			&record.Link,
			&record.PageWelcome,
			&record.ServiceId,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.ByHash = make(map[string]Campaign, len(records))
	s.ByLink = make(map[string]Campaign, len(records))
	for _, campaign := range records {
		s.ByHash[campaign.Hash] = campaign
		s.ByLink[campaign.Link] = campaign
	}
	log.Debug(fmt.Sprintf("%#v", Svc.Campaigns.ByHash))
	return nil
}
