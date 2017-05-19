package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	acceptor "github.com/linkit360/go-acceptor-structs"
)

// Tasks:
// Keep in memory all active campaigns
// Allow to get a service_id by campaign hash fastly
// Reload when changes to campaigns are done
type Campaigns struct {
	sync.RWMutex
	ByHash        map[string]Campaign
	ByLink        map[string]Campaign
	ByCode        map[string]Campaign
	ByServiceCode map[string]Campaign
}

type Campaign struct {
	AutoClickCount int64             `json:"-"`
	CanAutoClick   bool              `json:"-"`
	Properties     acceptor.Campaign `json:"campaign"`
}

func (campaign *Campaign) SimpleServe(c *gin.Context, data interface{}) {
	campaign.incRatio()
	log.WithFields(log.Fields{
		"code":              campaign.Properties.Code,
		"count":             campaign.AutoClickCount,
		"ratio":             campaign.Properties.AutoClickRatio,
		"autoclick_enabled": campaign.Properties.AutoClickEnabled,
		"autoclick":         campaign.CanAutoClick,
	}).Debug("serve")

	c.Writer.Header().Set("Content-Type", "text/html; charset-utf-8")
	c.HTML(http.StatusOK, campaign.Properties.PageWelcome+".html", data)
}

func (camp *Campaign) incRatio() {
	if !camp.Properties.AutoClickEnabled {
		camp.CanAutoClick = false
		return
	}
	camp.AutoClickCount = camp.AutoClickCount + 1
	if camp.AutoClickCount == camp.Properties.AutoClickRatio {
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
		"page_thank_you, "+
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
	var campaigns []acceptor.Campaign
	for rows.Next() {
		campaign := acceptor.Campaign{}
		if err = rows.Scan(
			&campaign.Code,
			&campaign.Hash,
			&campaign.Link,
			&campaign.PageWelcome,
			&campaign.PageSuccess,
			&campaign.PageThankYou,
			&campaign.PageError,
			&campaign.ServiceCode,
			&campaign.AutoClickEnabled,
			&campaign.AutoClickRatio,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}

		filePath := Svc.conf.StaticPath +
			"campaign/" + campaign.Hash + "/" +
			campaign.PageWelcome + ".html"

		_, err := template.ParseFiles(filePath)
		if err != nil {
			loadCampaignErrorFlag = true
			err := fmt.Errorf("template.ParseFiles: %s", err.Error())
			log.WithField("error", err.Error()).Error("template parse file error")
			err = nil
		}

		campaigns = append(campaigns, campaign)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	if loadCampaignErrorFlag == true {
		Svc.m.LoadCampaignError.Set(1.)
	} else {
		Svc.m.LoadCampaignError.Set(0.)
	}

	s.ByHash = make(map[string]Campaign, len(campaigns))
	s.ByLink = make(map[string]Campaign, len(campaigns))
	s.ByCode = make(map[string]Campaign, len(campaigns))
	s.ByServiceCode = make(map[string]Campaign, len(campaigns))

	for _, campaign := range campaigns {
		s.ByHash[campaign.Hash] = Campaign{Properties: campaign}
		s.ByLink[campaign.Link] = Campaign{Properties: campaign}
		s.ByCode[campaign.Code] = Campaign{Properties: campaign}
		s.ByServiceCode[campaign.ServiceCode] = Campaign{Properties: campaign}
	}

	s.GetContents()

	return nil
}

func (s *Campaigns) GetContents() {
	byHash, _ := json.Marshal(s.ByHash)
	byLink, _ := json.Marshal(s.ByLink)
	byId, _ := json.Marshal(s.ByCode)
	byServiceCode, _ := json.Marshal(s.ByServiceCode)

	log.WithFields(log.Fields{
		"hash": string(byHash),
		"link": string(byLink),
		"id":   string(byId),
		"sid":  string(byServiceCode),
	}).Debug("loaded")
}
