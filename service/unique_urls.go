package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/linkit360/go-utils/structs"
	log "github.com/sirupsen/logrus"
)

type UniqueUrls struct {
	sync.RWMutex
	ByUrl map[string]structs.ContentSentProperties
}

func (uuc *UniqueUrls) Get(uniqueUrl string) (structs.ContentSentProperties, error) {
	v, found := uuc.ByUrl[uniqueUrl]
	if !found {
		property, err := uuc.loadUniqueUrl(uniqueUrl)
		if err != nil {
			return structs.ContentSentProperties{}, fmt.Errorf("uuc.loadUniqueUrl: %s", err.Error())
		}
		return property, nil
	}
	return v, nil
}

func (uuc *UniqueUrls) Set(r structs.ContentSentProperties) {
	_, found := uuc.ByUrl[r.UniqueUrl]
	if !found {
		uuc.Lock()
		defer uuc.Unlock()
		if uuc.ByUrl == nil {
			uuc.ByUrl = make(map[string]structs.ContentSentProperties)
		}
		uuc.ByUrl[r.UniqueUrl] = r
		log.WithFields(log.Fields{
			"tid": r.Tid,
			"key": r.UniqueUrl,
		}).Debug("set url cache")
	}
}

func (uuc *UniqueUrls) Delete(r structs.ContentSentProperties) {
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

	prop := []structs.ContentSentProperties{}
	for rows.Next() {
		var p structs.ContentSentProperties
		if err = rows.Scan(
			&p.SentAt,
			&p.Msisdn,
			&p.Tid,
			&p.CampaignCode,
			&p.ServiceCode,
			&p.ContentId,
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

	uuc.ByUrl = make(map[string]structs.ContentSentProperties, len(prop))
	log.WithField("count", len(prop)).Debug("loaded uniq urls")
	for _, r := range prop {
		uuc.ByUrl[r.UniqueUrl] = r
	}
	return
}
func (uuc *UniqueUrls) loadUniqueUrl(uniqueUrl string) (p structs.ContentSentProperties, err error) {
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
			&p.ContentId,
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

	if p.Tid == "" || p.ContentId == "" {
		err = fmt.Errorf("Not found: %s", uniqueUrl)
	}

	return
}
