package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/linkit360/go-utils/metrics"
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
}

type сampaigns struct {
	sync.RWMutex
	conf          CampaignsConfig
	s3dl          *s3manager.Downloader
	loadError     prometheus.Gauge
	notFound      m.Gauge
	ByUUID        map[string]xmp_api_structs.Campaign
	ByHash        map[string]Campaign
	ByLink        map[string]Campaign
	ByCode        map[string]Campaign
	ByServiceCode map[string][]Campaign
}

func initCampaigns(appName string, campConfig CampaignsConfig) Campaigns {
	campaigns := &сampaigns{
		conf:      campConfig,
		loadError: m.PrometheusGauge(appName, "campaigns_load", "error", "load campaigns error"),
		notFound:  m.NewGauge(appName, "campaign", "not_found", "campaign not found error"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			campaigns.notFound.Update()
		}
	}()

	return campaigns

}

type CampaignsConfig struct {
	FromControlPanel bool          `yaml:"from_control_panel"`
	WebHook          string        `yaml:"webhook" default:"http://localhost:50300/updateTemplates"`
	LandingsPath     string        `yaml:"landing_path"`
	Region           string        `yaml:"region" default:"ap-southeast-1"`
	Bucket           string        `yaml:"bucket" default:"xmp-lp"`
	DownloadTimeout  time.Duration `yaml:"download_timeout" default:"10m"` // 10 minutes
}

type Campaign struct {
	AutoClickCount int64 `json:"-"`
	CanAutoClick   bool  `json:"-"`
	xmp_api_structs.Campaign
}

func (s *Campaign) Load(ac xmp_api_structs.Campaign) {
	cBytes, _ := json.Marshal(ac)
	_ = json.Unmarshal(cBytes, &s)
	return
}

// check content and download it
// content already checked: it hasn't been downloaded yet
func (s *сampaigns) Download(c xmp_api_structs.Campaign) (err error) {
	ctx := context.Background()
	var cancelFn func()
	if s.conf.DownloadTimeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, s.conf.DownloadTimeout)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	defer cancelFn()
	buff := &aws.WriteAtBuffer{}

	var contentLength int64
	contentLength, err = s.s3dl.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: aws.String(s.conf.Bucket),
		Key:    aws.String(c.Lp),
	})

	if err != nil {
		err = fmt.Errorf("Download: %s, error: %s", c.Id, err.Error())
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			log.WithFields(log.Fields{
				"id":      c.Id,
				"timeout": s.conf.DownloadTimeout,
				"error":   err.Error(),
			}).Error("download canceled due to timeout")
		} else if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.WithFields(log.Fields{
				"id":    c.Id,
				"error": err.Error(),
			}).Error("no such object")

		} else {
			log.WithFields(log.Fields{
				"id":    c.Id,
				"error": err.Error(),
			}).Error("failed to download object")
		}
		return
	}

	log.WithFields(log.Fields{
		"id":  c.Id,
		"len": len(buff.Bytes()),
	}).Debug("unzip...")

	if err = unzip(buff.Bytes(), contentLength, s.conf.LandingsPath); err != nil {
		err = fmt.Errorf("%s: unzip: %s", c.Id, err.Error())

		log.WithFields(log.Fields{
			"id":    c.Id,
			"error": err.Error(),
		}).Error("failed to unzip object")
		return
	}
	log.WithFields(log.Fields{
		"id":  c.Id,
		"len": len(buff.Bytes()),
	}).Info("unzip done")
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
	c.HTML(http.StatusOK, camp.PageWelcome+".html", data)
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

func (s *сampaigns) GetAll() map[string]Campaign {
	return s.ByLink
}

func (s *сampaigns) Update(campaign xmp_api_structs.Campaign) error {
	if !s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}
	if c, ok := s.ByUUID[campaign.Id]; ok {
		if c.Lp != campaign.Lp {
			log.WithFields(log.Fields{
				"id":      campaign.Id,
				"from_lp": campaign.Lp,
				"to_lp":   c.Lp,
			}).Debug("landing has changed")
			if err := s.Download(campaign); err != nil {
				return fmt.Errorf("Download: %s", err.Error())
			}
		}
	}
	s.ByUUID[campaign.Id] = campaign
	s.updateOnUUID()
	return nil
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

func (s *сampaigns) getByCache() (campaigns map[string]xmp_api_structs.Campaign, err error) {
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

		campaigns[campaign.Code] = campaign
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	return
}

func (s *сampaigns) Reload() (err error) {
	if s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}

	s.Lock()
	defer s.Unlock()

	s.loadError.Set(0.)
	var campaigns map[string]xmp_api_structs.Campaign
	campaigns, err = s.getByCache()
	if err != nil {
		s.loadError.Set(1.)
		log.Error(err.Error())
		return
	}
	s.Apply(campaigns)
	s.ShowLoaded()
	return nil
}

func (s *сampaigns) Apply(campaigns map[string]xmp_api_structs.Campaign) {
	s.ByUUID = make(map[string]xmp_api_structs.Campaign, len(campaigns))
	for _, ac := range campaigns {
		s.ByUUID[ac.Id] = ac
	}
	s.updateOnUUID()
}

func (s *сampaigns) updateOnUUID() {
	s.ByHash = make(map[string]Campaign, len(s.ByUUID))
	s.ByLink = make(map[string]Campaign, len(s.ByUUID))
	s.ByCode = make(map[string]Campaign, len(s.ByUUID))
	s.ByServiceCode = make(map[string][]Campaign)

	for _, ac := range s.ByUUID {
		campaign := Campaign{}
		campaign.Load(ac)
		s.ByHash[campaign.Hash] = campaign
		s.ByLink[campaign.Link] = campaign
		s.ByCode[campaign.Code] = campaign
		s.ByServiceCode[campaign.ServiceCode] = append(s.ByServiceCode[campaign.ServiceCode], campaign)
	}
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
