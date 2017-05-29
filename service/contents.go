package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/prometheus/client_golang/prometheus"

	acceptor "github.com/linkit360/go-acceptor-structs"
	m "github.com/linkit360/go-utils/metrics"
)

type Contents interface {
	Update([]acceptor.Content) error
	Download(acceptor.Content) error
	Reload() error
	GetById(string) (acceptor.Content, error)
}

type contents struct {
	sync.RWMutex
	conf      ContentConfig
	s3dl      *s3manager.Downloader
	ByUUID    map[string]acceptor.Content
	loadError prometheus.Gauge
}

type ContentConfig struct {
	FromControlPanel bool          `yaml:"from_control_panel"`
	ContentPath      string        `yaml:"content_path"`
	Region           string        `yaml:"region" default:"ap-southeast-1"`
	Bucket           string        `yaml:"bucket" default:"xmp-content"`
	DownloadTimeout  time.Duration `yaml:"download_timeout" default:"10m"` // 10 minutes
}

func initContents(appName string, contentConf ContentConfig) Contents {
	contentSvc := &contents{
		conf:      contentConf,
		loadError: m.PrometheusGauge(appName, "content_load", "error", "load content error"),
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(contentConf.Region),
	}))
	contentSvc.s3dl = s3manager.NewDownloader(sess)

	return contentSvc
}

// check content and download it
// content already checked: it hasn't been downloaded yet
func (s *contents) Download(c acceptor.Content) (err error) {
	ctx := context.Background()
	var cancelFn func()
	if s.conf.DownloadTimeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, s.conf.DownloadTimeout)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	defer cancelFn()
	buff := &aws.WriteAtBuffer{}

	var contentLength int64
	contentLength, err = s.s3dl.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: aws.String(s.conf.Bucket),
		Key:    aws.String(c.Id),
	})
	if err != nil {
		err = fmt.Errorf("Download: %s, error: %s", c.Id, err.Error())
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			log.WithFields(log.Fields{
				"id":      c.Id,
				"timeout": s.conf.DownloadTimeout,
				"error":   err.Error(),
			}).Error("download canceled due to timeout")

		} else if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.WithFields(log.Fields{
				"id":    c.Id,
				"error": err.Error(),
			}).Error("no such object")

		} else {
			log.WithFields(log.Fields{
				"id":    c.Id,
				"error": err.Error(),
			}).Error("failed to download object")
		}
		return
	}

	log.WithFields(log.Fields{
		"id":  c.Id,
		"len": len(buff.Bytes()),
	}).Debug("unzip...")
	if err = unzip(buff.Bytes(), contentLength, s.conf.ContentPath); err != nil {
		err = fmt.Errorf("%s: unzip: %s", c.Id, err.Error())

		log.WithFields(log.Fields{
			"id":    c.Id,
			"error": err.Error(),
		}).Error("failed to unzip object")
		return
	}
	log.WithFields(log.Fields{
		"id":  c.Id,
		"len": len(buff.Bytes()),
	}).Info("unzip done")
	return
}

func (s *contents) Update(cc []acceptor.Content) (err error) {
	if !s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}
	for _, c := range cc {
		if err = s.Download(c); err != nil {
			return fmt.Errorf("Download: %s", err.Error())
		}
		c.Name = s.conf.ContentPath + "/" + c.Name

		if _, err := os.Stat(c.Name); os.IsNotExist(err) {
			return fmt.Errorf("Unzip: %s", err.Error())
		}

		s.ByUUID[c.Id] = c
	}

	return nil
}

func (s *contents) getSlice(in map[string]acceptor.Content) (res []acceptor.Content) {
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

	var allContents []acceptor.Content
	for rows.Next() {
		var c acceptor.Content
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

	s.ByUUID = make(map[string]acceptor.Content, len(allContents))
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

func (s *contents) GetById(id string) (acceptor.Content, error) {
	c, found := s.ByUUID[id]
	if !found {
		return acceptor.Content{}, fmt.Errorf("Not found: %s", id)
	}
	return c, nil
}

func (s *contents) ShowLoaded() {
	contentJson, _ := json.Marshal(s.ByUUID)
	log.WithField("byid", string(contentJson)).Debug()
}
