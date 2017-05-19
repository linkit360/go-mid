package service

import (
	"database/sql"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	client "github.com/linkit360/go-acceptor-client"
	acceptor "github.com/linkit360/go-acceptor-structs"
	m "github.com/linkit360/go-utils/metrics"
)

// Tasks:
// Keep in memory all active content_ids mapping to their object string (url path to content)
// Allow to get object for given content id
// Reload when changes to content
type Contents interface {
	Reload() error
	Get(int64) (acceptor.Content, error)
}

type contents struct {
	sync.RWMutex
	conf      ContentConfig
	ById      map[int64]acceptor.Content
	loadError prometheus.Gauge
	loadCache prometheus.Gauge
}

type ContentConfig struct {
	Enabled          bool `yaml:"enabled"`
	FromControlPanel bool `yaml:"from_control_panel"`
}

func initContents(appName string, contentConf ContentConfig) Contents {
	contentSvc := &contents{
		conf:      contentConf,
		loadError: m.PrometheusGauge(appName, "content_load", "error", "load content error"),
		loadCache: m.PrometheusGauge(appName, "content", "cache", "load content cache"),
	}
	if !contentSvc.conf.Enabled {
		log.Info("contents disabled")
		return contentSvc
	}

	if !contentSvc.conf.FromControlPanel {
		return contentSvc
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

	var contents []acceptor.Content
	for rows.Next() {
		var c acceptor.Content
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

	s.ById = make(map[int64]acceptor.Content)
	for _, content := range contents {
		s.ById[content.Id] = content
	}
	return nil
}

func (s *contents) Reload() (err error) {
	s.Lock()
	defer s.Unlock()
	if !s.conf.Enabled {
		return nil
	}

	s.loadCache.Set(0)
	s.loadError.Set(0)
	if s.conf.FromControlPanel {
		s.ById, err = client.GetContents(Svc.conf.ProviderName)
		if err == nil {
			return
		}
	}

	s.loadCache.Set(1.)
	if err = s.loadFromCache(); err != nil {
		s.loadError.Set(1.)
		return
	}

	return nil
}

func (s *contents) Get(id int64) (acceptor.Content, error) {
	c, found := s.ById[id]
	if !found {
		return acceptor.Content{}, errNotFound()
	}
	return c, nil
}
