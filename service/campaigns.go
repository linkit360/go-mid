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
	ByHash map[string]Campaign
	ByLink map[string]Campaign
}
type Campaign struct {
	Hash             string `json:"hash"`
	Link             string `json:"link"`
	PageWelcome      string `json:"page_welcome"`
	Id               int64  `json:"id"`
	ServiceId        int64  `json:"service_id"`
	AutoClickRatio   int64  `json:"auto_click_ratio"`
	AutoClickEnabled bool   `json:"auto_click_enabled"`
	AutoClickCount   int64  `json:"auto_click_count"`
	CanAutoClick     bool   `json:"can_auto_click"`
}

func (campaign Campaign) Serve(c *gin.Context) {
	data := struct {
		AutoClick bool
	}{
		AutoClick: campaign.CanAutoClick,
	}
	c.HTML(http.StatusOK, campaign.PageWelcome+".html", data)
}

func (camp Campaign) IncRatio() {
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

	loadCampaignErrorFlag := false
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
	for _, campaign := range records {
		s.ByHash[campaign.Hash] = campaign
		s.ByLink[campaign.Link] = campaign
	}
	return nil
}
