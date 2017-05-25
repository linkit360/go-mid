package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/linkit360/go-utils/metrics"
)

type Contents interface {
	Update(Content) error
	Reload() error
	GetByCode(string) (Content, error)
}

type Content struct {
	Id   string
	Code string
	Path string
	Name string
}

type contents struct {
	sync.RWMutex
	conf      ContentConfig
	ByCode    map[string]Content
	ByUUID    map[string]Content
	loadError prometheus.Gauge
}

type ContentConfig struct {
	FromControlPanel bool `yaml:"from_control_panel"`
}

func initContents(appName string, contentConf ContentConfig) Contents {
	contentSvc := &contents{
		conf:      contentConf,
		loadError: m.PrometheusGauge(appName, "content_load", "error", "load content error"),
	}
	return contentSvc
}

func (s *contents) Update(Content) (err error) {
	if !s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}

	// todo: check on S3 and download
	s.setAll(s.getSlice(s.ByUUID))

	return nil
}

func (s *contents) getSlice(in map[string]Content) (res []Content) {
	for _, v := range in {
		res = append(res, v)
	}
	return res
}

func (s *contents) loadFromCache() (err error) {
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"object, "+
		"content_name "+
		"FROM %scontent "+
		"WHERE status = $1",
		Svc.dbConf.TablePrefix)

	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var allContents []Content
	for rows.Next() {
		var c Content
		if err = rows.Scan(
			&c.Code,
			&c.Path,
			&c.Name,
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

	s.setAll(allContents)
	return nil
}

func (s *contents) setAll(in []Content) {
	s.ByCode = make(map[string]Content, len(in))
	s.ByUUID = make(map[string]Content, len(in))
	for _, content := range in {
		s.ByCode[content.Code] = content
		s.ByUUID[content.Id] = content
	}
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

func (s *contents) GetByCode(code string) (Content, error) {
	c, found := s.ByCode[code]
	if !found {
		return Content{}, errNotFound()
	}
	return c, nil
}

func (s *contents) ShowLoaded() {
	contentJson, _ := json.Marshal(s.ByUUID)
	log.WithField("byid", string(contentJson)).Debug()
}
