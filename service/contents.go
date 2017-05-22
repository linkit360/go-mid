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

// Tasks:
// Keep in memory all active content_ids mapping to their object string (url path to content)
// Allow to get object for given content id
// Reload when changes to content
type Contents interface {
	Reload() error
	Get(int64) (Content, error)
}
type Content struct {
	Id   int64
	Path string
	Name string
}
type contents struct {
	sync.RWMutex
	conf      ContentConfig
	ById      map[int64]Content
	loadError prometheus.Gauge
}

type ContentConfig struct {
	Enabled          bool `yaml:"enabled"`
	FromControlPanel bool `yaml:"from_control_panel"`
}

func initContents(appName string, contentConf ContentConfig) Contents {
	contentSvc := &contents{
		conf:      contentConf,
		loadError: m.PrometheusGauge(appName, "content_load", "error", "load content error"),
	}
	return contentSvc
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

	var contents []Content
	for rows.Next() {
		var c Content
		if err = rows.Scan(
			&c.Id,
			&c.Path,
			&c.Name,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		contents = append(contents, c)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.ById = make(map[int64]Content)
	for _, content := range contents {
		s.ById[content.Id] = content
	}
	return nil
}

func (s *contents) Reload() (err error) {
	s.Lock()
	defer s.Unlock()

	s.loadError.Set(0.)
	if err = s.loadFromCache(); err != nil {
		s.loadError.Set(1.)
		return
	}

	return nil
}

func (s *contents) Get(id int64) (Content, error) {
	c, found := s.ById[id]
	if !found {
		return Content{}, errNotFound()
	}
	return c, nil
}

func (s *contents) ShowLoaded() {
	contentJson, _ := json.Marshal(s.ById)
	log.WithField("byid", string(contentJson)).Debug()
}
