package service

import (
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

type UniqueUrls struct {
	sync.RWMutex
	ByUrl map[string]ContentSentProperties
}

func (uuc *UniqueUrls) Get(uniqueUrl string) (ContentSentProperties, error) {
	v, found := uuc.ByUrl[uniqueUrl]
	if !found {
		property, err := uuc.loadUniqueUrl(uniqueUrl)
		if err != nil {
			return ContentSentProperties{}, fmt.Errorf("uuc.loadUniqueUrl: %s", err.Error())
		}
		return property, nil
	}
	return v, nil
}

func (uuc *UniqueUrls) Set(r ContentSentProperties) {
	log.WithFields(log.Fields{
		"cache": uuc,
	}).Debug("set url cache")
	_, found := uuc.ByUrl[r.UniqueUrl]
	if !found {
		uuc.Lock()
		defer uuc.Unlock()
		uuc.ByUrl[r.UniqueUrl] = r
		log.WithFields(log.Fields{
			"tid": r.Tid,
			"key": r.UniqueUrl,
		}).Debug("set url cache")
	}
}

func (uuc *UniqueUrls) Delete(r ContentSentProperties) {
	delete(uuc.ByUrl, r.UniqueUrl)
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": r.UniqueUrl,
	}).Debug("deleted url cache")
}

// warm cache for unique urls
func (uuc *UniqueUrls) Reload() (err error) {
	uuc.Lock()
	defer uuc.Unlock()

	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("load uniq urls failed")
			} else {
				log.WithFields(fields).Debug("load uniq urls ")
			}
		}()
	}()
	query := fmt.Sprintf("SELECT "+
		"sent_at, "+
		"msisdn, "+
		"tid, "+
		"id_campaign, "+
		"id_service, "+
		"id_content, "+
		"id_subscription, "+
		"country_code, "+
		"operator_code, "+
		"content_path, "+
		"content_name, "+
		"unique_url "+
		"FROM %scontent_unique_urls",
		Svc.dbConf.TablePrefix)

	rows, err := Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	prop := []ContentSentProperties{}
	for rows.Next() {
		var p ContentSentProperties
		if err = rows.Scan(
			&p.SentAt,
			&p.Msisdn,
			&p.Tid,
			&p.CampaignCode,
			&p.ServiceCode,
			&p.ContentCode,
			&p.SubscriptionId,
			&p.CountryCode,
			&p.OperatorCode,
			&p.ContentPath,
			&p.ContentName,
			&p.UniqueUrl,
		); err != nil {
			err = fmt.Errorf("Rows.Next: %s", err.Error())
			return
		}
		prop = append(prop, p)
	}

	if rows.Err() != nil {
		err = fmt.Errorf("Rows.Err: %s", err.Error())
		return
	}

	uuc.ByUrl = make(map[string]ContentSentProperties, len(prop))
	log.WithField("count", len(prop)).Debug("loaded uniq urls")
	for _, r := range prop {
		uuc.ByUrl[r.UniqueUrl] = r
	}
	return
}
func (uuc *UniqueUrls) loadUniqueUrl(uniqueUrl string) (p ContentSentProperties, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("load uniq url failed")
			} else {
				fields["tid"] = p.Tid
				log.WithFields(fields).Debug("load uniq url ")
			}
		}()
	}()
	query := fmt.Sprintf("SELECT "+
		"sent_at, "+
		"msisdn, "+
		"tid, "+
		"id_campaign, "+
		"id_service, "+
		"id_content, "+
		"id_subscription, "+
		"country_code, "+
		"operator_code, "+
		"content_path, "+
		"content_name, "+
		"unique_url "+
		"FROM %scontent_unique_urls "+
		"WHERE unique_url = $1 "+
		"LIMIT 1",
		Svc.dbConf.TablePrefix)

	rows, err := Svc.db.Query(query, uniqueUrl)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(
			&p.SentAt,
			&p.Msisdn,
			&p.Tid,
			&p.CampaignCode,
			&p.ServiceCode,
			&p.ContentCode,
			&p.SubscriptionId,
			&p.CountryCode,
			&p.OperatorCode,
			&p.ContentPath,
			&p.ContentName,
			&p.UniqueUrl,
		); err != nil {
			err = fmt.Errorf("Rows.Next: %s", err.Error())
			return
		}
	}

	if rows.Err() != nil {
		err = fmt.Errorf("Rows.Err: %s", err.Error())
		return
	}

	if p.Tid == "" || p.ContentCode == "" {
		err = fmt.Errorf("Not found: %s", uniqueUrl)
	}

	return
}
