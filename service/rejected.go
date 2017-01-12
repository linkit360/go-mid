package service

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
)

func initPrevSubscriptionsCache() {
	prev, err := loadPreviousSubscriptions(Svc.conf.OperatorCode)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load previous subscriptions")
	}
	log.WithField("count", len(prev)).Debug("loaded previous subscriptions")
	Svc.Rejected = cache.New(24*time.Hour, time.Minute)
	for _, v := range prev {
		key := v.Msisdn + strconv.FormatInt(v.CampaignId, 10)
		Svc.Rejected.Set(key, struct{}{}, time.Now().Sub(v.CreatedAt))
	}
}

func SetMsisdnCampaignCache(campaignId int64, msisdn string) {
	key := msisdn + strconv.FormatInt(campaignId, 10)
	Svc.Rejected.Set(key, struct{}{}, 24*time.Hour)
	log.WithField("key", key).Debug("rejected set")
}

func GetMsisdnCampaignCache(campaignId int64, msisdn string) int64 {
	key := msisdn + strconv.FormatInt(campaignId, 10)
	_, found := Svc.Rejected.Get(key)
	if !found {
		log.WithFields(log.Fields{"id": campaignId, "key": key}).Debug("rejected get")
		return campaignId
	}
	for id, _ := range Svc.Campaigns.ById {
		_, found := Svc.Rejected.Get(msisdn + strconv.FormatInt(id, 10))
		if !found {
			log.WithFields(log.Fields{"id": id, "key": key}).Debug("rejected get")
			return id
		}
	}
	log.WithFields(log.Fields{"id": 0, "key": key}).Debug("rejected get")
	return 0
}

type PreviuosSubscription struct {
	Id         int64
	CreatedAt  time.Time
	Msisdn     string
	ServiceId  int64
	CampaignId int64
}

func loadPreviousSubscriptions(operatorCode int64) (records []PreviuosSubscription, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("load previous subscriptions failed")
			} else {
				fields["count"] = len(records)
				log.WithFields(fields).Debug("load previous subscriptions ")
			}
		}()
	}()

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"msisdn, "+
		"id_service, "+
		"id_campaign, "+
		"created_at "+
		"FROM %ssubscriptions "+
		"WHERE "+
		"(CURRENT_TIMESTAMP - 24 * INTERVAL '1 hour' ) < created_at AND "+
		"result IN ('', 'paid', 'failed') AND "+
		"operator_code = $1",
		Svc.dbConf.TablePrefix)

	prev := []PreviuosSubscription{}
	rows, err := Svc.db.Query(query, operatorCode)
	if err != nil {

		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return prev, err
	}
	defer rows.Close()

	for rows.Next() {
		var p PreviuosSubscription
		if err := rows.Scan(
			&p.Id,
			&p.Msisdn,
			&p.ServiceId,
			&p.CampaignId,
			&p.CreatedAt,
		); err != nil {

			err = fmt.Errorf("Rows.Next: %s", err.Error())
			return prev, err
		}
		prev = append(prev, p)
	}

	if rows.Err() != nil {

		err = fmt.Errorf("Rows.Err: %s", err.Error())
		return prev, err
	}
	return prev, nil
}
