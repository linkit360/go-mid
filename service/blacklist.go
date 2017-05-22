package service

import (
	"database/sql"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/linkit360/go-utils/metrics"
)

type BlackList interface {
	Reload() error
	IsBlacklisted(msisdn string) bool
}

type blackList struct {
	sync.RWMutex
	conf      BlackListConfig
	ByMsisdn  map[string]struct{}
	loadError prometheus.Gauge
	loadCache prometheus.Gauge
}

type BlackListConfig struct {
	Enabled          bool `yaml:"enabled"`
	FromControlPanel bool `yaml:"from_control_panel"`
	GetNewPeriod     int  `yaml:"period"`
}

func initBlackList(appName string, c BlackListConfig) *blackList {
	bl := &blackList{
		conf:      c,
		loadError: m.PrometheusGauge(appName, "blacklist_load", "error", "load blacklist error"),
	}
	return bl
}

func (bl *blackList) getBlackListedDBCache() (msisdns []string, err error) {
	query := fmt.Sprintf("SELECT msisdn FROM %smsisdn_blacklist",
		Svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var msisdn string
		if err = rows.Scan(&msisdn); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		msisdns = append(msisdns, msisdn)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	return
}

func (bl *blackList) Reload() error {
	bl.Lock()
	defer bl.Unlock()

	blackList, err := bl.getBlackListedDBCache()
	if err != nil {
		bl.loadCache.Set(1.0)
		err = fmt.Errorf("bl.getBlackListedDBCache: %s", err.Error())
		log.WithFields(log.Fields{"error": err.Error()}).Error("cannot get blacklist from db cache")
		return err
	}

	bl.ByMsisdn = make(map[string]struct{}, len(blackList))
	for _, msisdn := range blackList {
		bl.ByMsisdn[msisdn] = struct{}{}
	}
	return nil
}

func (bl *blackList) IsBlacklisted(msisdn string) bool {
	_, found := bl.ByMsisdn[msisdn]
	return found
}
