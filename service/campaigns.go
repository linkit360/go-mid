package service

import (
	"database/sql"
	"errors"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

// Tasks:
// Keep in memory all active campaigns
// Allow to get a service_id by campaign hash fastly
// Reload when changes to campaigns are done
type Campaigns struct {
	sync.RWMutex
	ByHash map[string]*Campaign
	ByLink map[string]*Campaign
}
type Campaign struct {
	Hash             string
	Link             string
	PageWelcome      string
	Id               int64
	ServiceId        int64
	AutoClickRatio   int64
	AutoClickEnabled bool
	AutoClickCount   int64
	CanAutoClick     bool
	Content          []byte
}

func (campaign *Campaign) Serve(c *gin.Context) {
	campaign.IncRatio()
	t, err := template.New("campaignlp").Parse(string(campaign.Content))
	if err != nil {
		campaignServeError.Inc()
		c.Error(err)

		log.WithField("error", err.Error()).Error("parse file")
		http.Redirect(c.Writer, c.Request, Svc.conf.RedirectUrl, 303)
	}
	data := struct {
		AutoClick bool
	}{
		AutoClick: campaign.CanAutoClick,
	}
	err = t.Execute(os.Stdout, data)
	if err != nil {
		campaignServeError.Inc()
		c.Error(err)

		log.WithField("error", err.Error()).Error("execute template")
		http.Redirect(c.Writer, c.Request, Svc.conf.RedirectUrl, 303)
	}
}

func (s *Campaigns) ByHashWithRatio(hash string) (Campaign, error) {
	camp, ok := s.ByHash[hash]
	if !ok {
		return *camp, errors.New("Not Found")
	}
	camp.IncRatio()
	return *camp, nil
}

func (camp *Campaign) IncRatio() {
	camp.AutoClickCount = camp.AutoClickCount + 1
	if camp.AutoClickCount == camp.AutoClickRatio {
		camp.AutoClickCount = 0
		camp.CanAutoClick = false
	}
	camp.CanAutoClick = true
}

func (s *Campaigns) Reload() (err error) {
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"hash, "+
		"link, "+
		"page_welcome, "+
		"service_id_1, "+
		"autoclick_enabled, "+
		"autoclick_ratio "+
		"FROM %scampaigns "+
		"WHERE status = $1",
		Svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var records []Campaign
	for rows.Next() {
		record := Campaign{}
		if err = rows.Scan(
			&record.Id,
			&record.Hash,
			&record.Link,
			&record.PageWelcome,
			&record.ServiceId,
			&record.AutoClickEnabled,
			&record.AutoClickRatio,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}

		filePath := Svc.conf.StaticPath +
			"campaign/" + record.Hash + "/" +
			record.PageWelcome + ".html"
		record.Content, err = ioutil.ReadFile(filePath)
		if err != nil {
			loadCampaignError.Set(1.)
			err := fmt.Errorf("ioutil.ReadFile: %s", err.Error())
			log.WithField("error", err.Error()).Error("serve file error")
			err = nil
		}

		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.ByHash = make(map[string]*Campaign, len(records))
	s.ByLink = make(map[string]*Campaign, len(records))
	for _, campaign := range records {
		s.ByHash[campaign.Hash] = &campaign
		s.ByLink[campaign.Link] = &campaign
	}
	return nil
}
