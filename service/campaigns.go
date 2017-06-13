package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/linkit360/go-utils/metrics"
	"github.com/linkit360/go-utils/zip"
	xmp_api_client "github.com/linkit360/xmp-api/src/client"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

type Campaigns interface {
	Apply(campaigns map[string]xmp_api_structs.Campaign)
	Update(xmp_api_structs.Campaign) error
	Download(c xmp_api_structs.Campaign) (err error)
	Reload() error
	GetAll() map[string]Campaign
	GetByLink(string) (Campaign, error)
	GetByCode(string) (Campaign, error)
	GetByHash(string) (Campaign, error)
	GetByServiceCode(string) ([]Campaign, error)
	GetJson() string
	ShowLoaded()
}

type CampaignsConfig struct {
	FromControlPanel bool   `yaml:"from_control_panel"`
	WebHook          string `yaml:"webhook" default:"http://localhost:50300/updateTemplates"`
	LandingsPath     string `yaml:"landing_path"`
	LandingsReload   bool   `yaml:"landing_reload"` // remove old downloaded lp-s and get new
	Bucket           string `yaml:"bucket" default:"xmp-lp"`
}

type Campaign struct {
	AutoClickCount int64 `json:"-"`
	CanAutoClick   bool  `json:"-"`
	xmp_api_structs.Campaign
}

func (camp *Campaign) Load(ac xmp_api_structs.Campaign) {
	cBytes, _ := json.Marshal(ac)
	_ = json.Unmarshal(cBytes, &camp)
	return
}
func (camp *Campaign) SimpleServe(c *gin.Context, data interface{}) {
	camp.incRatio()
	log.WithFields(log.Fields{
		"code":              camp.Code,
		"count":             camp.AutoClickCount,
		"ratio":             camp.AutoClickRatio,
		"autoclick_enabled": camp.AutoClickEnabled,
		"autoclick":         camp.CanAutoClick,
	}).Debug("serve")

	c.Writer.Header().Set("Content-Type", "text/html; charset-utf-8")
	c.HTML(http.StatusOK, camp.Hash+".html", data)
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

type сampaigns struct {
	sync.RWMutex
	conf            CampaignsConfig
	loadError       prometheus.Gauge
	awsSessionError prometheus.Gauge
	notFound        m.Gauge
	ByUUID          map[string]xmp_api_structs.Campaign
	ByHash          map[string]Campaign
	ByLink          map[string]Campaign
	ByCode          map[string]Campaign
	ByServiceCode   map[string][]Campaign
}

func initCampaigns(appName string, campConfig CampaignsConfig) Campaigns {
	campaigns := &сampaigns{
		conf:            campConfig,
		loadError:       m.PrometheusGauge(appName, "campaigns_load", "error", "load campaigns error"),
		awsSessionError: m.PrometheusGauge(appName, "campaigns_aws_session", "error", "aws session campaigns error"),
		notFound:        m.NewGauge(appName, "campaign", "not_found", "campaign not found error"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			campaigns.notFound.Update()
		}
	}()

	go campaigns.catchUpdates(xmp_api_client.ChanCampaigns)

	return campaigns
}
func (camp *сampaigns) catchUpdates(updates <-chan xmp_api_structs.Campaign) {
	camp.loadError.Set(0)
	for s := range updates {
		if err := camp.Update(s); err != nil {
			camp.loadError.Set(1)
			log.WithFields(log.Fields{
				"error":    err.Error(),
				"campaign": fmt.Sprintf("%#v", s),
				"id":       s.Id,
			}).Error("failed to update campaign")
		} else {
			log.WithFields(log.Fields{
				"id": s.Id,
			}).Info("update campaign done")
		}
	}
}

// check content and download it
// content already checked: it hasn't been downloaded yet
func (s *сampaigns) Download(c xmp_api_structs.Campaign) (err error) {
	unzipPath := s.conf.LandingsPath + c.Id + "/"
	log.WithFields(log.Fields{
		"id": c.Id,
		"lp": c.Lp,
	}).Debug("campaign land check..")

	should, err := Svc.downloader.ShouldDownload(unzipPath, s.conf.LandingsReload)
	if err != nil {
		log.WithFields(log.Fields{
			"id":    c.Id,
			"error": err.Error(),
		}).Error("campaign check failed")
		return err
	}
	if !should {
		return nil
	}

	buff, size, err := Svc.downloader.Download(s.conf.Bucket, c.Lp)
	if err != nil {
		log.WithFields(log.Fields{
			"id":    c.Id,
			"error": err.Error(),
		}).Error("campaign download failed")
		return err
	}

	log.WithFields(log.Fields{
		"id":  c.Id,
		"len": size,
	}).Debug("unzip...")

	if _, err = zip.Unzip(buff, size, unzipPath); err != nil {
		err = fmt.Errorf("%s: unzip: %s", c.Id, err.Error())

		log.WithFields(log.Fields{
			"id":    c.Id,
			"error": err.Error(),
		}).Error("failed to unzip object")
		return
	}

	fromPath := unzipPath + "index.html"
	toPath := unzipPath + c.Id + ".html"
	if err := os.Rename(fromPath, toPath); err != nil {
		err = fmt.Errorf("os.Rename: %s", err.Error())
		log.WithFields(log.Fields{
			"id":       c.Id,
			"fromPath": fromPath,
			"toPath":   toPath,
			"error":    err.Error(),
		}).Error("failed to rename index")
	}

	log.WithFields(log.Fields{
		"id":  c.Id,
		"len": len(buff),
	}).Info("unpack campaign done")
	return
}
func (s *сampaigns) GetAll() map[string]Campaign {
	return s.ByLink
}
func (s *сampaigns) Update(ac xmp_api_structs.Campaign) error {
	campJson, _ := json.Marshal(ac)
	log.WithFields(log.Fields{
		"id":   ac.Id,
		"camp": string(campJson),
	}).Debug("campaign")

	if ac.Id == "" {
		return fmt.Errorf("Campaign Id is empty%s", "")
	}
	if ac.Code == "" {
		return fmt.Errorf("Campaign Code is empty%s", "")
	}
	if ac.Hash == "" {
		ac.Hash = ac.Id
	}
	if ac.Status == 0 {
		s.Lock()
		delete(s.ByUUID, ac.Id)
		delete(s.ByHash, ac.Id)
		delete(s.ByLink, ac.Link)
		delete(s.ByCode, ac.Code)
		s.Unlock()
		log.WithFields(log.Fields{
			"id": ac.Id,
		}).Debug("campaign deleted")
		s.webHook()
		return nil
	}
	if c, ok := s.ByUUID[ac.Id]; ok {
		if c.Lp != ac.Lp && c.Lp != "" {
			log.WithFields(log.Fields{
				"id":      ac.Id,
				"from_lp": ac.Lp,
				"to_lp":   c.Lp,
			}).Debug("land has changed")
			if err := s.Download(ac); err != nil {
				return fmt.Errorf("Download: %s", err.Error())
			}
		}
	} else {
		if err := s.Download(ac); err != nil {
			return fmt.Errorf("Download: %s", err.Error())
		}
	}

	s.Lock()
	defer s.Unlock()

	if s.ByHash == nil {
		s.ByHash = make(map[string]Campaign)
	}
	if s.ByLink == nil {
		s.ByLink = make(map[string]Campaign)
	}
	if s.ByCode == nil {
		s.ByCode = make(map[string]Campaign)
		s.ByServiceCode = make(map[string][]Campaign)
		s.ByLink = make(map[string]Campaign)
		s.ByHash = make(map[string]Campaign)
	}

	if s.ByServiceCode == nil {
		s.ByServiceCode = make(map[string][]Campaign)
	}
	if s.ByUUID == nil {
		s.ByUUID[ac.Id] = ac
	}

	serv, err := Svc.Services.GetById(ac.ServiceId)
	if err != nil {
		return fmt.Errorf("unknown service id: %s", ac.ServiceId)
	}
	ac.ServiceCode = serv.Code
	campaign := Campaign{}
	campaign.Load(ac)
	s.ByUUID[campaign.Id] = ac
	s.ByHash[campaign.Hash] = campaign
	s.ByLink[campaign.Link] = campaign
	s.ByCode[campaign.Code] = campaign

	s.ByServiceCode = make(map[string][]Campaign)
	for _, c := range s.ByUUID {
		s.ByServiceCode[c.ServiceCode] = append(s.ByServiceCode[c.ServiceCode], campaign)
	}
	s.webHook()
	return nil
}
func (s *сampaigns) webHook() {
	if s.conf.WebHook != "" {
		resp, err := http.Get(s.conf.WebHook)
		if err != nil || resp.StatusCode != 200 {
			fields := log.Fields{
				"webhook": s.conf.WebHook,
			}
			if resp != nil {
				fields["code"] = resp.Status
			}
			if err != nil {
				fields["error"] = err.Error()
			}
			log.WithFields(fields).Error("hook failed")
		} else {
			log.WithFields(log.Fields{
				"webhook": s.conf.WebHook,
			}).Debug("campaign update webhook done")
		}
	}
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
func (s *сampaigns) Reload() (err error) {
	if s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}
	var campaigns map[string]xmp_api_structs.Campaign
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

	campaigns = make(map[string]xmp_api_structs.Campaign)
	for rows.Next() {
		campaign := xmp_api_structs.Campaign{}
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
		campaign.Id = campaign.Code
		campaigns[campaign.Code] = campaign
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.Apply(campaigns)
	s.ShowLoaded()
	return nil
}
func (s *сampaigns) Apply(campaigns map[string]xmp_api_structs.Campaign) {

	s.ByUUID = make(map[string]xmp_api_structs.Campaign, len(campaigns))
	s.ByCode = make(map[string]Campaign, len(campaigns))
	s.ByServiceCode = make(map[string][]Campaign)
	s.ByLink = make(map[string]Campaign, len(campaigns))
	s.ByHash = make(map[string]Campaign, len(campaigns))
	s.loadError.Set(0)
	for _, ac := range campaigns {
		if err := s.Update(ac); err == nil {
			log.WithField("id", ac.Id).Debug("update campaign ok")
		} else {
			s.loadError.Set(1)
			log.WithFields(log.Fields{
				"id":    ac.Id,
				"error": err.Error(),
			}).Debug("update campaign")
		}
	}
}
func (s *сampaigns) ShowLoaded() {
	byHash, _ := json.Marshal(s.ByHash)
	byLink, _ := json.Marshal(s.ByLink)
	byCode, _ := json.Marshal(s.ByCode)
	byServiceCode, _ := json.Marshal(s.ByServiceCode)

	log.WithFields(log.Fields{
		"hash": string(byHash),
		"link": string(byLink),
		"code": string(byCode),
		"sid":  string(byServiceCode),
	}).Debug("campaigns")
}
func (s *сampaigns) GetJson() string {
	sJson, _ := json.Marshal(s.ByUUID)
	return string(sJson)
}
