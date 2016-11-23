package service

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/dispatcherd/src/utils"
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
	Hash             string
	Link             string
	PageWelcome      string
	Id               int64
	ServiceId        int64
	AutoClickRatio   int64
	AutoClickEnabled bool
	Content          []byte
}

func (campaign Campaign) Serve(c *gin.Context) {
	utils.ServeBytes(campaign.Content, c)
}

func (s *Campaigns) Reload() (err error) {
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"hash, "+
		"link, "+
		"page_welcome, "+
		"service_id_1, "+
		"autoclick_enabled, "+
		"autoclick_ratio "+
		"FROM %scampaigns "+
		"WHERE status = $1",
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
			&record.AutoClickEnabled,
			&record.AutoClickRatio,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}

		filePath := Svc.conf.StaticPath +
			"campaign/" + record.Hash + "/" +
			record.PageWelcome + ".html"
		record.Content, err = ioutil.ReadFile(filePath)
		if err != nil {
			loadCampaignError.Set(1.)
			err := fmt.Errorf("ioutil.ReadFile: %s", err.Error())
			log.WithField("error", err.Error()).Error("ioutil.ReadFile serve file error")
			err = nil
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
	return nil
}
