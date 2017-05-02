package service

// im-memory pixel settings
// kept by key fmt.Sprintf("%d-%s", ps.CampaignId, ps.Publisher)
import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type PixelSettings struct {
	sync.RWMutex
	ByKey        map[string]*PixelSetting
	ByCampaignId map[int64]PixelSetting
}

type PixelSetting struct {
	Id            int64
	CampaignId    int64
	OperatorCode  int64
	Publisher     string
	Endpoint      string
	Timeout       int
	Enabled       bool
	Ratio         int
	Count         int
	SkipPixelSend bool
}

func (pss *PixelSettings) GetByKey(key string) (PixelSetting, error) {
	ps, ok := pss.ByKey[key]
	if !ok {
		return PixelSetting{}, fmt.Errorf("Key %s: not found", key)
	}
	return *ps, nil
}

func (pss *PixelSettings) ByKeyWithRatio(key string) (PixelSetting, error) {
	ps, ok := pss.ByKey[key]
	if !ok {
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
	return strings.ToLower(fmt.Sprintf("%d-%s", ps.CampaignId, ps.Publisher))
}
func (ps *PixelSetting) OperatorKey() string {
	return strings.ToLower(fmt.Sprintf("%d-%s", ps.OperatorCode, ps.Publisher))
}
func (ps *PixelSetting) CampaignOperatorKey() string {
	return strings.ToLower(fmt.Sprintf("%d-%d-%s", ps.CampaignId, ps.OperatorCode, ps.Publisher))
}

func (ps *PixelSettings) Reload() (err error) {
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

	var records []PixelSetting
	for rows.Next() {
		p := PixelSetting{}

		if err = rows.Scan(
			&p.Id,
			&p.CampaignId,
			&p.OperatorCode,
			&p.Publisher,
			&p.Endpoint,
			&p.Timeout,
			&p.Enabled,
			&p.Ratio,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		p.Count = 0
		records = append(records, p)
	}

	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	ps.ByKey = make(map[string]*PixelSetting)
	ps.ByCampaignId = make(map[int64]PixelSetting)
	for _, p := range records {
		pixelC := p
		pixelO := p
		ps.ByKey[p.CampaignKey()] = &pixelC
		ps.ByKey[p.OperatorKey()] = &pixelO
		ps.ByKey[p.CampaignOperatorKey()] = &pixelO

		ps.ByCampaignId[p.CampaignId] = p

		log.WithFields(log.Fields{
			"ratio": p.Ratio,
			"ckey":  p.CampaignKey(),
			"opkey": p.OperatorKey(),
			"cokey": p.CampaignOperatorKey(),
		}).Debug("add key")
	}
	log.WithFields(log.Fields{
		"bykey": fmt.Sprintf("%#v", ps.ByKey),
	}).Debug("added")
	return nil
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
		loadPublisherRegexError.Set(1.)
		return
	} else {
		loadPublisherRegexError.Set(0.)
	}

	p.All = make(map[string]Publisher, len(records))
	for _, publisher := range records {
		p.All[publisher.Name] = publisher
	}
	return nil
}
