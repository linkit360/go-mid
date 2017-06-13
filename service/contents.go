package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/linkit360/go-utils/metrics"
	"github.com/linkit360/go-utils/zip"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

type Contents interface {
	Update([]xmp_api_structs.Content) error
	Download(xmp_api_structs.Content) error
	Reload() error
	GetById(string) (xmp_api_structs.Content, error)
	GetJson() string
	ShowLoaded()
}

type contents struct {
	sync.RWMutex
	conf      ContentConfig
	s3dl      *s3manager.Downloader
	ByUUID    map[string]xmp_api_structs.Content
	loadError prometheus.Gauge
}

type ContentConfig struct {
	FromControlPanel bool          `yaml:"from_control_panel"`
	ContentPath      string        `yaml:"content_path"`
	Bucket           string        `yaml:"bucket" default:"xmp-content"`
	DownloadTimeout  time.Duration `yaml:"download_timeout" default:"10m"` // 10 minutes
}

func initContents(appName string, contentConf ContentConfig) Contents {
	contentSvc := &contents{
		conf:      contentConf,
		loadError: m.PrometheusGauge(appName, "content_load", "error", "load content error"),
	}

	return contentSvc
}

// check content and download it
// content already checked: it hasn't been downloaded yet
func (s *contents) Download(c xmp_api_structs.Content) (err error) {

	buff, size, err := Svc.downloader.Download(s.conf.Bucket, c.Id)
	if err != nil {
		log.WithFields(log.Fields{
			"id":    c.Id,
			"error": err.Error(),
		}).Error("campaign download failed")
		return err
	}

	log.WithFields(log.Fields{
		"id":  c.Id,
		"len": len(buff),
	}).Debug("unzip...")
	if _, err = zip.Unzip(buff, size, s.conf.ContentPath); err != nil {
		err = fmt.Errorf("%s: unzip: %s", c.Id, err.Error())

		log.WithFields(log.Fields{
			"id":    c.Id,
			"error": err.Error(),
		}).Error("failed to unzip object")
		return
	}
	log.WithFields(log.Fields{
		"id":  c.Id,
		"len": len(buff),
	}).Info("unzip done")
	return
}

func (s *contents) Update(cc []xmp_api_structs.Content) (err error) {
	if !s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}

	for _, c := range cc {
		if err = s.Download(c); err != nil {
			return fmt.Errorf("Download: %s", err.Error())
		}

		contentPath := s.conf.ContentPath + c.Name
		if _, err := os.Stat(contentPath); os.IsNotExist(err) {
			return fmt.Errorf("Cannot find file: %s", err.Error())
		}

		// only in content need this
		if s.ByUUID == nil {
			s.ByUUID = make(map[string]xmp_api_structs.Content)
		}
		s.ByUUID[c.Id] = c
	}

	return nil
}

func (s *contents) loadFromCache() (err error) {
	query := fmt.Sprintf("SELECT "+
		"%scontent.id, "+
		"object, "+
		"content_name "+
		"FROM %scontent, %sservice_content, %sservices "+
		"WHERE %scontent.status = $1 "+
		"AND xmp_services.id = xmp_service_content.id_service "+
		"AND xmp_service_content.id_content = xmp_content.id",
		Svc.dbConf.TablePrefix,
		Svc.dbConf.TablePrefix,
		Svc.dbConf.TablePrefix,
		Svc.dbConf.TablePrefix,
		Svc.dbConf.TablePrefix,
	)

	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var allContents []xmp_api_structs.Content
	for rows.Next() {
		var c xmp_api_structs.Content
		if err = rows.Scan(
			&c.Id,
			&c.Name,
			&c.Title,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		allContents = append(allContents, c)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.ByUUID = make(map[string]xmp_api_structs.Content, len(allContents))
	for _, content := range allContents {
		s.ByUUID[content.Id] = content
	}
	return nil
}

func (s *contents) Reload() (err error) {
	if s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}

	s.Lock()
	defer s.Unlock()

	s.loadError.Set(0.)
	if err = s.loadFromCache(); err != nil {
		s.loadError.Set(1.)
		return
	}

	return nil
}

func (s *contents) GetById(id string) (xmp_api_structs.Content, error) {
	c, found := s.ByUUID[id]
	if !found {
		return xmp_api_structs.Content{}, fmt.Errorf("Not found: %s", id)
	}
	return c, nil
}

func (s *contents) ShowLoaded() {
	contentJson, _ := json.Marshal(s.ByUUID)
	log.WithField("byid", string(contentJson)).Debug("content")
}

func (s *contents) GetJson() string {
	sJson, _ := json.Marshal(s.ByUUID)
	return string(sJson)
}
