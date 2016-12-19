package service

// im-memory pixel settings
// kept by key fmt.Sprintf("%d-%d-%s", ps.CampaignId, ps.OperatorCode, ps.Publisher)
import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
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

func (ps *PixelSetting) Key() string {
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

	ps.ByKey = make(map[string]*PixelSetting, len(records))
	ps.ByCampaignId = make(map[int64]PixelSetting)
	for _, p := range records {
		pixel := p
		ps.ByKey[p.Key()] = &pixel
		ps.ByCampaignId[p.CampaignId] = p.Publisher
	}
	return nil
}
