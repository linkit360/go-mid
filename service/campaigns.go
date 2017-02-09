package service

import (
	"database/sql"
	"fmt"
	"html/template"
	"net/http"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

// Tasks:
// Keep in memory all active campaigns
// Allow to get a service_id by campaign hash fastly
// Reload when changes to campaigns are done
type Campaigns struct {
	sync.RWMutex
	ByHash      map[string]Campaign
	ByLink      map[string]Campaign
	ById        map[int64]Campaign
	ByServiceId map[int64]Campaign
}
type Campaign struct {
	Hash             string `json:"hash,omitempty"`
	Link             string `json:"link,omitempty"`
	PageWelcome      string `json:"page_welcome,omitempty"`
	Id               int64  `json:"id,omitempty"`
	ServiceId        int64  `json:"service_id,omitempty"`
	AutoClickRatio   int64  `json:"auto_click_ratio,omitempty"`
	AutoClickEnabled bool   `json:"auto_click_enabled,omitempty"`
	AutoClickCount   int64  `json:"auto_click_count,omitempty"`
	CanAutoClick     bool   `json:"can_auto_click,omitempty"`
	PageSuccess      string `json:"page_success,omitempty"`
	PageError        string `json:"page_error,omitempty"`
}

func (campaign *Campaign) SimpleServe(c *gin.Context) {
	data := struct {
		AutoClick bool
	}{
		AutoClick: campaign.CanAutoClick,
	}
	campaign.incRatio()
	log.WithFields(log.Fields{
		"id":                campaign.Id,
		"count":             campaign.AutoClickCount,
		"ratio":             campaign.AutoClickRatio,
		"autoclick_enabled": campaign.AutoClickEnabled,
		"autoclick":         campaign.CanAutoClick,
	}).Debug("serve")

	c.HTML(http.StatusOK, campaign.PageWelcome+".html", data)
}

func (camp *Campaign) incRatio() {
	if !camp.AutoClickEnabled {
		camp.CanAutoClick = false
		return
	}
	camp.AutoClickCount = camp.AutoClickCount + 1
	if camp.AutoClickCount == camp.AutoClickRatio {
		camp.AutoClickCount = 0
		camp.CanAutoClick = true
	} else {
		camp.CanAutoClick = false
	}
}

func (s *Campaigns) Reload() (err error) {
	s.Lock()
	defer s.Unlock()

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"hash, "+
		"link, "+
		"page_welcome, "+
		"page_success, "+
		"page_error, "+
		"service_id, "+
		"autoclick_enabled, "+
		"autoclick_ratio "+
		"FROM %scampaigns "+
		"WHERE status = $1 ORDER BY service_id DESC",
		Svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	loadCampaignErrorFlag := false
	var records []Campaign
	for rows.Next() {
		record := Campaign{}
		if err = rows.Scan(
			&record.Id,
			&record.Hash,
			&record.Link,
			&record.PageWelcome,
			&record.PageSuccess,
			&record.PageError,
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

		_, err := template.ParseFiles(filePath)
		if err != nil {
			loadCampaignErrorFlag = true
			err := fmt.Errorf("template.ParseFiles: %s", err.Error())
			log.WithField("error", err.Error()).Error("template parse file error")
			err = nil
		}

		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	if loadCampaignErrorFlag == true {
		loadCampaignError.Set(1.)
	} else {
		loadCampaignError.Set(0.)
	}

	s.ByHash = make(map[string]Campaign, len(records))
	s.ByLink = make(map[string]Campaign, len(records))
	s.ById = make(map[int64]Campaign, len(records))
	s.ByServiceId = make(map[int64]Campaign, len(records))
	for _, campaign := range records {
		s.ByHash[campaign.Hash] = campaign
		s.ByLink[campaign.Link] = campaign
		s.ById[campaign.Id] = campaign
		s.ByServiceId[campaign.ServiceId] = campaign
	}
	return nil
}
