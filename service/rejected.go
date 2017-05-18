package service

import (
	"fmt"
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
	Svc.RejectedByCampaign = cache.New(24*time.Hour, time.Minute)
	Svc.RejectedByService = cache.New(24*time.Hour, time.Minute)
	for _, v := range prev {
		Svc.RejectedByCampaign.Set(
			v.Msisdn+"-"+v.CampaignCode,
			struct{}{}, time.Now().Sub(v.CreatedAt),
		)

		Svc.RejectedByService.Set(
			v.Msisdn+"-"+v.ServiceCode,
			struct{}{}, time.Now().Sub(v.CreatedAt),
		)
	}
}

func SetMsisdnCampaignCache(campaignCode, msisdn string) {
	key := msisdn + "-" + campaignCode
	Svc.RejectedByCampaign.Set(key, struct{}{}, 24*time.Hour)
	log.WithField("key", key).Debug("rejected set")
}

func GetMsisdnCampaignCache(campaignCode, msisdn string) string {
	key := msisdn + "-" + campaignCode
	_, found := Svc.RejectedByCampaign.Get(key)
	if !found {
		log.WithFields(log.Fields{"id": campaignCode, "key": key}).Debug("rejected get")
		return campaignCode
	}
	for code, _ := range Svc.Campaigns.ByCode {
		_, found := Svc.RejectedByCampaign.Get(msisdn + "-" + code)
		if !found {
			log.WithFields(log.Fields{"id": code, "key": key}).Debug("rejected get")
			return code
		}
	}
	log.WithFields(log.Fields{"id": 0, "key": key}).Debug("rejected get")
	return ""
}

func SetMsisdnServiceCache(serviceCode, msisdn string) {
	key := msisdn + "-" + serviceCode
	Svc.RejectedByService.Set(key, struct{}{}, 24*time.Hour)
	log.WithField("key", key).Debug("rejected set")
}

func IsMsisdnRejectedByService(serviceCode, msisdn string) bool {
	key := msisdn + "-" + serviceCode
	_, found := Svc.RejectedByService.Get(key)
	return found
}

type PreviuosSubscription struct {
	Id           int64
	CreatedAt    time.Time
	Msisdn       string
	ServiceCode  string
	CampaignCode string
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
			&p.ServiceCode,
			&p.CampaignCode,
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
