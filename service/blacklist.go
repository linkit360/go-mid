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
	Add(string) error
	Apply(blackList []string)
	Reload() error
	IsBlacklisted(msisdn string) bool
	Len() int
}

type blackList struct {
	sync.RWMutex
	conf      BlackListConfig
	ByMsisdn  map[string]struct{}
	loadError prometheus.Gauge
	loadCache prometheus.Gauge
}

type BlackListConfig struct {
	FromControlPanel bool `yaml:"from_control_panel"`
}

func initBlackList(appName string, c BlackListConfig) *blackList {
	bl := &blackList{
		conf:      c,
		loadError: m.PrometheusGauge(appName, "blacklist_load", "error", "load blacklist error"),
	}
	return bl
}

func (bl *blackList) Add(msisdn string) error {
	if !bl.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}
	bl.ByMsisdn[msisdn] = struct{}{}
	return nil
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
	if bl.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}

	bl.Lock()
	defer bl.Unlock()

	blackList, err := bl.getBlackListedDBCache()
	if err != nil {
		bl.loadCache.Set(1.0)
		err = fmt.Errorf("bl.getBlackListedDBCache: %s", err.Error())
		log.WithFields(log.Fields{"error": err.Error()}).Error("cannot get blacklist from db cache")
		return err
	}

	bl.Apply(blackList)
	return nil
}

func (bl *blackList) Apply(blackList []string) {
	bl.ByMsisdn = make(map[string]struct{}, len(blackList))
	for _, msisdn := range blackList {
		bl.ByMsisdn[msisdn] = struct{}{}
	}
}

func (bl *blackList) IsBlacklisted(msisdn string) bool {
	_, found := bl.ByMsisdn[msisdn]
	return found
}

func (bl *blackList) ShowLoaded() {
	log.WithFields(log.Fields{
		"action": "blacklist",
		"len":    len(bl.ByMsisdn),
	}).Info("")
}

func (bl *blackList) Len() int {
	return len(bl.ByMsisdn)
}
