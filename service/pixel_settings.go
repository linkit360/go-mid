package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	acceptor "github.com/linkit360/go-acceptor-structs"
	m "github.com/linkit360/go-utils/metrics"
)

type PixelSettings interface {
	Update(acceptor.PixelSetting) error
	Reload() error
	GetByKey(string) (PixelSetting, error)
	GetByCampaignCode(string) (PixelSetting, error)
	ByKeyWithRatio(string) (PixelSetting, error)
}

type PixelSettingsConfig struct {
	FromControlPanel bool `yaml:"from_control_panel"`
}

type pixelSettings struct {
	sync.RWMutex
	conf           PixelSettingsConfig
	notFound       m.Gauge
	ByKey          map[string]*PixelSetting
	ByCampaignCode map[string]PixelSetting
	ByUUID         map[string]acceptor.PixelSetting
}

type PixelSetting struct {
	acceptor.PixelSetting
	Count         int  `json:"-"`
	SkipPixelSend bool `json:"-"`
}

func (ps *PixelSetting) SetPublisher(publisher string) {
	ps.Publisher = publisher
}
func (ps *PixelSetting) SetCampaignCode(campaignCode string) {
	ps.CampaignCode = campaignCode
}
func (ps *PixelSetting) SetOperatorCode(code int64) {
	ps.OperatorCode = code
}

func (pss *pixelSettings) loadPixelSetting(ps acceptor.PixelSetting) (px PixelSetting) {
	psBytes, _ := json.Marshal(ps)
	json.Unmarshal(psBytes, &px)
	return
}

func initPixelSettings(appName string, pixelConf PixelSettingsConfig) PixelSettings {
	ps := &pixelSettings{
		conf:     pixelConf,
		notFound: m.NewGauge(appName, "pixel_setting", "not_found", "pixel setting not found error"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			ps.notFound.Update()
		}
	}()
	return ps
}

func (pss *pixelSettings) Update(ps acceptor.PixelSetting) error {
	if !pss.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}
	if ps.Id == "" {
		return fmt.Errorf("PixelId is empty%s", "")
	}

	pss.ByUUID[ps.Id] = ps
	pss.setAll(pss.getSlice(pss.ByUUID))
	return nil
}

func (pss *pixelSettings) getSlice(in map[string]acceptor.PixelSetting) (res []acceptor.PixelSetting) {
	for _, v := range in {
		res = append(res, v)
	}
	return res
}

func (pss *pixelSettings) GetByKey(key string) (PixelSetting, error) {
	ps, ok := pss.ByKey[key]
	if !ok {
		pss.notFound.Inc()
		return PixelSetting{}, fmt.Errorf("Key %s: not found", key)
	}
	return *ps, nil
}
func (pss *pixelSettings) GetByCampaignCode(code string) (PixelSetting, error) {
	ps, ok := pss.ByCampaignCode[code]
	if !ok {
		pss.notFound.Inc()
		return PixelSetting{}, fmt.Errorf("Code %s: not found", code)
	}
	return ps, nil
}

func (pss *pixelSettings) ByKeyWithRatio(key string) (PixelSetting, error) {
	ps, ok := pss.ByKey[key]
	if !ok {
		pss.notFound.Inc()
		return PixelSetting{}, fmt.Errorf("Key %s: not found", key)
	}
	ps.Count = ps.Count + 1
	if ps.Count == ps.Ratio {
		ps.Count = 0
		ps.SkipPixelSend = false
	} else {
		ps.SkipPixelSend = true
	}
	return *ps, nil
}

func (ps *PixelSetting) CampaignKey() string {
	return strings.ToLower(fmt.Sprintf("%s-%s", ps.CampaignCode, ps.Publisher))
}

func (ps *PixelSetting) OperatorKey() string {
	return strings.ToLower(fmt.Sprintf("%d-%s", ps.OperatorCode, ps.Publisher))
}

func (ps *PixelSetting) CampaignOperatorKey() string {
	return strings.ToLower(fmt.Sprintf("%s-%d-%s", ps.CampaignCode, ps.OperatorCode, ps.Publisher))
}

func (ps *pixelSettings) Reload() (err error) {
	if ps.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}

	ps.Lock()
	defer ps.Unlock()

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"id_campaign, "+
		"operator_code, "+
		"publisher, "+
		"endpoint, "+
		"timeout, "+
		"enabled, "+
		"ratio "+
		"FROM %spixel_settings ",
		Svc.dbConf.TablePrefix,
	)

	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var records []acceptor.PixelSetting
	for rows.Next() {
		ap := acceptor.PixelSetting{}

		if err = rows.Scan(
			&ap.Id,
			&ap.CampaignCode,
			&ap.OperatorCode,
			&ap.Publisher,
			&ap.Endpoint,
			&ap.Timeout,
			&ap.Enabled,
			&ap.Ratio,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, ap)
	}

	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	ps.setAll(records)
	return nil
}

func (ps *pixelSettings) setAll(pixelSet []acceptor.PixelSetting) {
	ps.ByKey = make(map[string]*PixelSetting)
	ps.ByCampaignCode = make(map[string]PixelSetting)
	ps.ByUUID = make(map[string]acceptor.PixelSetting, len(pixelSet))

	for _, ap := range pixelSet {
		p := ps.loadPixelSetting(ap)
		pixelO := p
		ps.ByKey[p.CampaignKey()] = &pixelO
		ps.ByKey[p.OperatorKey()] = &pixelO
		ps.ByKey[p.CampaignOperatorKey()] = &pixelO
		ps.ByUUID[p.Id] = ap

		ps.ByCampaignCode[p.CampaignCode] = p

		log.WithFields(log.Fields{
			"ratio": p.Ratio,
			"ckey":  p.CampaignKey(),
			"opkey": p.OperatorKey(),
			"cokey": p.CampaignOperatorKey(),
		}).Debug("add key")
	}

	byKeyJson, _ := json.Marshal(ps.ByKey)
	log.WithFields(log.Fields{
		"bykey": string(byKeyJson),
	}).Debug("added")
}

type Publishers struct {
	sync.RWMutex
	All map[string]Publisher
}

type Publisher struct {
	Name        string
	RegexString string
	Regex       *regexp.Regexp
}

func (p *Publishers) Reload() (err error) {
	p.Lock()
	defer p.Unlock()

	query := fmt.Sprintf("SELECT "+
		"name, "+
		"regex "+
		"FROM %spublishers ",
		Svc.dbConf.TablePrefix,
	)

	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	loadPublisherErrorFlag := false
	var records []Publisher
	for rows.Next() {
		p := Publisher{}

		if err = rows.Scan(
			&p.Name,
			&p.RegexString,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		p.Name = strings.ToLower(p.Name)
		p.Regex, err = regexp.Compile(p.RegexString)
		if err != nil {
			log.WithField("regex", p.RegexString).Error("wrong regex")
			loadPublisherErrorFlag = true
		} else {
			log.WithField("regex", p.RegexString).Debug("regex ok")
		}
		records = append(records, p)
	}

	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	if loadPublisherErrorFlag == true {
		Svc.m.LoadPublisherRegexError.Set(1.)
		return
	} else {
		Svc.m.LoadPublisherRegexError.Set(0.)
	}

	p.All = make(map[string]Publisher, len(records))
	for _, publisher := range records {
		p.All[publisher.Name] = publisher
	}
	return nil
}
