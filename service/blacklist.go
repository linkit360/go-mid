package service

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	acceptor_client "github.com/linkit360/go-acceptor-client"
)

type BlackList struct {
	sync.RWMutex
	conf     BlackListConfig
	ByMsisdn map[string]struct{}
}

type BlackListConfig struct {
	GetNewPeriodMinutes int `yaml:"period"`
}

func initBlackList(c BlackListConfig) *BlackList {
	bl := &BlackList{
		conf: c,
	}

	go func() {
		lastSuccessFullTime := time.Now()
		for range time.Tick(time.Duration(bl.conf.GetNewPeriodMinutes) * time.Minute) {
			msisdns, err := acceptor_client.BlackListGetNew(
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

func (bl *BlackList) getBlackListedDBCache() (msisdns []string, err error) {
	query := fmt.Sprintf("SELECT "+
		"msisdn FROM %smsisdn_blacklist",
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

func (bl *BlackList) getBlackListed() ([]string, error) {

	msisdns, err := acceptor_client.BlackListGet(Svc.conf.ProviderName)
	if err != nil {
		err = fmt.Errorf("acceptor_client.GetBlackList: %s", err.Error())
		log.WithFields(log.Fields{"error": err.Error()}).Error("cannot get blacklist from client")
		return []string{}, err
	}
	return msisdns, nil
}

func (bl *BlackList) Reload() error {
	bl.Lock()
	defer bl.Unlock()

	blackList, err := bl.getBlackListed()
	if err != nil {
		blackList, err = bl.getBlackListedDBCache()
		if err != nil {
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
