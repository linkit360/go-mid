package service

// im-memory pixel settings
// kept by key fmt.Sprintf("%d-%d-%s", ps.CampaignId, ps.OperatorCode, ps.Publisher)

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
)

type PixelSettings struct {
	sync.RWMutex
	ByKey map[string]*PixelSetting
}

type PixelSetting struct {
	Id           int64
	CampaignId   int64
	OperatorCode int64
	Publisher    string
	Endpoint     string
	Timeout      int
	Enabled      bool
	Ratio        int
	Count        int
	CanIgnore    bool
}

func (pss *PixelSettings) GetWithRatio(key string) (PixelSetting, error) {
	ps, ok := pss.ByKey[key]
	if !ok {
		return *ps, errors.New("Not Found")
	}
	ps.setIgnore()
	return *ps, nil
}
func (ps *PixelSetting) setIgnore() {
	ps.Count = ps.Count + 1
	if ps.Count == ps.Ratio {
		ps.Count = 0
		ps.CanIgnore = false
	}
	ps.CanIgnore = true
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
		"FROM %spixel_settings "+
		"WHERE enabled = true",
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
	for _, p := range records {
		ps.ByKey[p.Key()] = &p
	}
	return nil
}
