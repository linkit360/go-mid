package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"

	client "github.com/linkit360/go-acceptor-client"
	acceptor "github.com/linkit360/go-acceptor-structs"
	m "github.com/linkit360/go-utils/metrics"
)

// Tasks:
// Keep in memory all active campaigns
// Allow to get a service_id by campaign hash fastly
// Reload when changes to campaigns are done
type Campaigns interface {
	Reload() error
	GetAll() map[string]Campaign
	GetByLink(string) (Campaign, error)
	GetByCode(string) (Campaign, error)
	GetByHash(string) (Campaign, error)
	GetByServiceCode(string) ([]Campaign, error)
}

type сampaigns struct {
	sync.RWMutex
	conf          CampaignsConfig
	loadError     prometheus.Gauge
	loadCache     prometheus.Gauge
	notFound      m.Gauge
	ByHash        map[string]Campaign
	ByLink        map[string]Campaign
	ByCode        map[string]Campaign
	ByServiceCode map[string][]Campaign
}

type CampaignsConfig struct {
	FromControlPanel bool   `yaml:"from_control_panel"`
	WebHook          string `yaml:"web_hook" default:"http://localhost:50300/updateTemplates"`
}

type Campaign struct {
	AutoClickCount int64             `json:"-"`
	CanAutoClick   bool              `json:"-"`
	Properties     acceptor.Campaign `json:"campaign"`
}

func (s *сampaigns) GetAll() map[string]Campaign {
	return s.ByLink
}

func (s *сampaigns) GetByLink(link string) (camp Campaign, err error) {
	var ok bool
	camp, ok = s.ByLink[link]
	if !ok {
		err = fmt.Errorf("Campaign link %s: not found", link)
		s.notFound.Inc()
		return
	}
	return
}
func (s *сampaigns) GetByCode(code string) (camp Campaign, err error) {
	var ok bool
	camp, ok = s.ByCode[code]
	if !ok {
		err = fmt.Errorf("Campaign code %s: not found", code)
		s.notFound.Inc()
		return
	}
	return
}
func (s *сampaigns) GetByHash(hash string) (camp Campaign, err error) {
	var ok bool
	camp, ok = s.ByHash[hash]
	if !ok {
		err = fmt.Errorf("Campaign hash %s: not found", hash)
		s.notFound.Inc()
		return
	}
	return
}
func (s *сampaigns) GetByServiceCode(serviceCode string) (camps []Campaign, err error) {
	camps = s.ByServiceCode[serviceCode]
	if len(camps) == 0 {
		err = fmt.Errorf("No campaigns with service code %s found", serviceCode)
		s.notFound.Inc()
		return
	}
	return
}

func initCampaigns(appName string, campConfig CampaignsConfig) Campaigns {
	campaigns := &сampaigns{
		conf:      campConfig,
		loadError: m.PrometheusGauge(appName, "campaigns_load", "error", "load campaigns error"),
		loadCache: m.PrometheusGauge(appName, "campaigns", "cache", "load campaigns cache"),
		notFound:  m.NewGauge(appName, "campaign", "not_found", "campaign not found error"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			campaigns.notFound.Update()
		}
	}()

	return campaigns

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

func (s *сampaigns) getByCache() (campaigns map[string]acceptor.Campaign, err error) {
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

	campaigns = make(map[string]acceptor.Campaign)
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

		campaigns[campaign.Code] = campaign
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	return
}

//filePath := Svc.conf.StaticPath +
//"campaign/" + campaign.Hash + "/" +
//campaign.PageWelcome + ".html"
//
//_, err := template.ParseFiles(filePath)
//if err != nil {
//loadCampaignErrorFlag = true
//err := fmt.Errorf("template.ParseFiles: %s", err.Error())
//log.WithField("error", err.Error()).Error("template parse file error")
//err = nil
//}

func (s *сampaigns) Reload() (err error) {
	s.Lock()
	defer s.Unlock()
	s.loadCache.Set(0.)
	s.loadError.Set(0.)

	var controlPanelErr error
	var campaigns map[string]acceptor.Campaign

	if s.conf.FromControlPanel {
		campaigns, controlPanelErr = client.GetCampaigns(Svc.conf.ProviderName)
		if controlPanelErr == nil {
			goto hashes
		}
		s.loadError.Set(1.)
		log.Error(controlPanelErr.Error())
	}

	campaigns, err = s.getByCache()
	if err == nil {
		s.loadError.Set(1.)
		s.loadCache.Set(1.)
		log.Error(err.Error())
		return
	}

hashes:
	s.ByHash = make(map[string]Campaign, len(campaigns))
	s.ByLink = make(map[string]Campaign, len(campaigns))
	s.ByCode = make(map[string]Campaign, len(campaigns))
	s.ByServiceCode = make(map[string][]Campaign)

	for _, campaign := range campaigns {
		s.ByHash[campaign.Hash] = Campaign{Properties: campaign}
		s.ByLink[campaign.Link] = Campaign{Properties: campaign}
		s.ByCode[campaign.Code] = Campaign{Properties: campaign}
		s.ByServiceCode[campaign.ServiceCode] = append(
			s.ByServiceCode[campaign.ServiceCode],
			Campaign{Properties: campaign},
		)
	}

	s.ShowLoaded()

	return nil
}

func (s *сampaigns) ShowLoaded() {
	byHash, _ := json.Marshal(s.ByHash)
	byLink, _ := json.Marshal(s.ByLink)
	byCode, _ := json.Marshal(s.ByCode)
	byServiceCode, _ := json.Marshal(s.ByServiceCode)

	log.WithFields(log.Fields{
		"hash":   string(byHash),
		"link":   string(byLink),
		"code":   string(byCode),
		"sid":    string(byServiceCode),
		"action": "campaigns",
	}).Debug("")
}
