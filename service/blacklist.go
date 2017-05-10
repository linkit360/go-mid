package service

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	acceptor_client "github.com/linkit360/go-acceptor-client"
	m "github.com/linkit360/go-utils/metrics"
)

type BlackList interface {
	Reload() error
	IsBlacklisted(msisdn string) bool
}

type blackList struct {
	sync.RWMutex
	conf               BlackListConfig
	ByMsisdn           map[string]struct{}
	loadBlacklistError prometheus.Gauge
	loadBlacklistCache prometheus.Gauge
}

type BlackListConfig struct {
	Enabled             bool `yaml:"enabled"`
	FromControlPanel    bool `yaml:"from_control_panel"`
	GetNewPeriodMinutes int  `yaml:"period"`
}

func initBlackList(appName string, c BlackListConfig) *blackList {
	bl := &blackList{
		conf: c,
	}
	bl.loadBlacklistError = m.PrometheusGauge(appName, "blacklist_load", "error", "load blacklist error")

	if !bl.conf.FromControlPanel {
		return bl
	}

	bl.loadBlacklistCache = m.PrometheusGauge(appName, "blacklist", "cache", "cache blacklist used")

	go func() {
		lastSuccessFullTime := time.Now()
		for range time.Tick(time.Duration(bl.conf.GetNewPeriodMinutes) * time.Minute) {
			msisdns, err := acceptor_client.GetNewBlackListed(
				Svc.conf.ProviderName,
				time.Now().Add(-time.Now().Sub(lastSuccessFullTime)).Format("2006-01-02 15:04:05"),
			)
			if err != nil {
				err = fmt.Errorf("acceptor_client.BlackListGetNew: %s", err.Error())
				log.WithFields(log.Fields{
					"error":        err.Error(),
					"provide_name": Svc.conf.ProviderName,
					"time_sent":    time.Now().Add(-time.Now().Sub(lastSuccessFullTime)).Format("2006-01-02 15:04:05"),
				}).Error("cannot get new blacklist from client")
			} else {
				for _, msisdn := range msisdns {
					bl.ByMsisdn[msisdn] = struct{}{}
				}
				if len(msisdns) > 0 {
					log.WithFields(log.Fields{"len": len(msisdns)}).Debug("updated")
				}
				lastSuccessFullTime = time.Now()
			}
		}
	}()

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

func (bl *blackList) getBlackListed() ([]string, error) {
	if !bl.conf.FromControlPanel {
		return []string{}, fmt.Errorf("BlackList disabled: %s", Svc.conf.ProviderName)
	}

	msisdns, err := acceptor_client.GetBlackListed(Svc.conf.ProviderName)
	if err != nil {
		err = fmt.Errorf("acceptor_client.GetBlackList: %s", err.Error())
		log.WithFields(log.Fields{"error": err.Error()}).Error("cannot get blacklist from client")
		return []string{}, err
	}
	return msisdns, nil
}

func (bl *blackList) Reload() error {
	bl.Lock()
	defer bl.Unlock()

	bl.loadBlacklistCache.Set(.0)
	bl.loadBlacklistError.Set(.0)
	blackList, err := bl.getBlackListed()
	if err != nil {
		bl.loadBlacklistCache.Set(1.0)
		bl.loadBlacklistError.Set(.0)
		blackList, err = bl.getBlackListedDBCache()
		if err != nil {
			bl.loadBlacklistCache.Set(1.0)
			err = fmt.Errorf("bl.getBlackListedDBCache: %s", err.Error())
			log.WithFields(log.Fields{"error": err.Error()}).Error("cannot get blacklist from db cache")
			return err
		}
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
