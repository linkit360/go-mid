package service

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	m "github.com/linkit360/go-utils/metrics"
	"github.com/linkit360/go-utils/zip"
)

type BlackList interface {
	Add(string) error
	Apply(blackList []string)
	LoadFromAws(bucket, key string) error
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
	FromControlPanel bool   `yaml:"from_control_panel"`
	BlackListBucket  string `yaml:"bucket"`
	BlackListTempDir string `yaml:"zip_temp_dir"`
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

	bl.Lock()
	defer bl.Unlock()
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
	bl.Lock()
	defer bl.Unlock()

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

func (bl *blackList) LoadFromAws(bucket, key string) (err error) {

	buff, size, err := Svc.downloader.Download(bucket, key)
	if err != nil {
		err = fmt.Errorf("Blacklist Download: %s", err.Error())
		return
	}

	fileList, err := zip.Unzip(buff, size, bl.conf.BlackListTempDir)
	if err != nil {
		err = fmt.Errorf("Blacklist unzip: %s", err.Error())
		return
	}
	if len(fileList) != 1 {
		err = fmt.Errorf("unexpected blacklist count of files: %d", len(fileList))
		return
	}
	fid, err := os.Open(fileList[0])
	if err != nil {
		err = fmt.Errorf("os.Open: %s, path: %s", err.Error(), fileList[0])
		return
	}
	defer fid.Close()
	scanner := bufio.NewScanner(fid)
	for scanner.Scan() {
		msisdn := scanner.Text()
		bl.Add(msisdn)
	}
	if err = scanner.Err(); err != nil {
		err = fmt.Errorf("scanner.Err: %s", err.Error())
		return err
	}

	return nil
}
