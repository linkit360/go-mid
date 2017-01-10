package service

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
)

// cache for unique urls
type UniqueUrlCache struct {
	cache *cache.Cache
}

func (uuc *UniqueUrlCache) init() {
	prev, err := uuc.loadUniqueUrls()
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load uniq urls")
	}
	log.WithField("count", len(prev)).Debug("loaded uniq urls")
	uuc = &UniqueUrlCache{
		cache: cache.New(8760*time.Hour, time.Hour),
	}
	for _, v := range prev {
		key := v.Msisdn + strconv.FormatInt(v.ServiceId, 10)
		uuc.cache.Set(key, v, 8760*time.Hour)
	}
}

func (uuc *UniqueUrlCache) Get(uniqueUrl string) (ContentSentProperties, error) {
	v, found := uuc.cache.Get(uniqueUrl)
	if !found {
		property, err := uuc.loadUniqueUrl(uniqueUrl)
		if err != nil {
			return ContentSentProperties{}, fmt.Errorf("uuc.loadUniqueUrl: %s", err.Error())
		}
		return property, nil
	}
	if property, ok := v.(ContentSentProperties); ok {
		return property, nil
	}
	return ContentSentProperties{}, fmt.Errorf("Cannot decifer: %s, %v", uniqueUrl, v)
}

func (uuc *UniqueUrlCache) Set(r ContentSentProperties) {
	_, found := uuc.cache.Get(r.UniqueUrl)
	if !found {
		uuc.cache.Set(r.UniqueUrl, r, 8760*time.Hour)
		log.WithFields(log.Fields{
			"tid": r.Tid,
			"key": r.UniqueUrl,
		}).Debug("set url cache")
	}
}

func (uuc *UniqueUrlCache) Delete(r ContentSentProperties) {
	uuc.cache.Delete(r.UniqueUrl)
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": r.UniqueUrl,
	}).Debug("deleted url cache")
}

// warm cache for unique urls
func (uuc *UniqueUrlCache) loadUniqueUrls() (prop []ContentSentProperties, err error) {
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
				fields["count"] = len(prop)
				log.WithFields(fields).Debug("load uniq urls ")
			}
		}()
	}()
	query := fmt.Sprintf("SELECT "+
		"sent_at, "+
		"msisdn, "+
		"tid, "+
		"content_path, "+
		"unique_url, "+
		"content_name, "+
		"id_campaign, "+
		"id_service, "+
		"operator_code, "+
		"country_code "+
		"FROM %scontent_unique_urls",
		Svc.dbConf.TablePrefix)

	rows, err := Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return prop, err
	}
	defer rows.Close()

	for rows.Next() {
		var p ContentSentProperties
		if err := rows.Scan(
			&p.SentAt,
			&p.Msisdn,
			&p.Tid,
			&p.ContentPath,
			&p.UniqueUrl,
			&p.ContentName,
			&p.CampaignId,
			&p.ServiceId,
			&p.OperatorCode,
			&p.CountryCode,
		); err != nil {
			err = fmt.Errorf("Rows.Next: %s", err.Error())
			return prop, err
		}
		prop = append(prop, p)
	}

	if rows.Err() != nil {
		err = fmt.Errorf("Rows.Err: %s", err.Error())
		return
	}
	return
}
func (uuc *UniqueUrlCache) loadUniqueUrl(uniqueUrl string) (p ContentSentProperties, err error) {
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
		"content_path, "+
		"unique_url, "+
		"content_name, "+
		"id_campaign, "+
		"id_service, "+
		"operator_code, "+
		"country_code, "+
		"FROM %scontent_unique_urls WHERE unique_url = $1 LIMIT 1",
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
			&p.ContentPath,
			&p.UniqueUrl,
			&p.ContentName,
			&p.CampaignId,
			&p.ServiceId,
			&p.OperatorCode,
			&p.CountryCode,
		); err != nil {
			err = fmt.Errorf("Rows.Next: %s", err.Error())
			return
		}
	}

	if rows.Err() != nil {
		err = fmt.Errorf("Rows.Err: %s", err.Error())
		return
	}

	if p.Tid == "" || p.ContentId == 0 {
		err = fmt.Errorf("Not found: %s", uniqueUrl)
	}

	return
}
