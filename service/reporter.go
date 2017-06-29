package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/linkit360/go-utils/amqp"
	m "github.com/linkit360/go-utils/metrics"
	xmp_api "github.com/linkit360/xmp-api/src/client"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

type Collector interface {
	SaveState()
	GetAggregate(time.Time, time.Time) ([]xmp_api_structs.Aggregate, error)
}

type Collect struct {
	Tid               string `json:"tid,omitempty"`
	CampaignUUID      string `json:"campaign_id,omitempty"`
	OperatorCode      int64  `json:"operator_code,omitempty"`
	Msisdn            string `json:"msisdn,omitempty"`
	TransactionResult string `json:"transaction_result,omitempty"`
	Price             int    `json:"price,omitempty"`
	AttemptsCount     int    `json:"attempts_count,omitempty"`
}

type collectorService struct {
	sync.RWMutex
	state         CollectorState
	db            *sql.DB
	m             *ReporterMetrics
	adReport      map[string]OperatorAgregate // map[campaign][operator]acceptor.Aggregate
	consume       *Consumers
	hitCh         <-chan amqp_driver.Delivery
	transactionCh <-chan amqp_driver.Delivery
	pixelCh       <-chan amqp_driver.Delivery
	outflowCh     <-chan amqp_driver.Delivery
}

type OperatorAgregate map[int64]adAggregate       // by operator code
type CampaignAgregate map[string]OperatorAgregate // by campaign code

type CollectorState struct {
	LastSendTime time.Time     `json:"last_send_time"`
	FilePath     string        `json:"file_path"`
	Archive      []interface{} `json:"archive"`
}

type ReporterMetrics struct {
	Success m.Gauge
	Errors  m.Gauge

	ErrorCampaignIdEmpty   m.Gauge
	ErrorOperatorCodeEmpty m.Gauge

	BreatheDuration prometheus.Summary
	SendDuration    prometheus.Summary
	AggregateSum    prometheus.Summary
}

func initReporterMetrics(appName string) *ReporterMetrics {
	mm := &ReporterMetrics{
		ErrorCampaignIdEmpty:   m.NewGauge(appName+"_reporter", "campaign_id", "empty", "errors"),
		ErrorOperatorCodeEmpty: m.NewGauge(appName+"_reporter", "operator_code", "empty", "errors"),
		Success:                m.NewGauge(appName, "reporter", "success", "success"),
		Errors:                 m.NewGauge(appName, "reporter", "errors", "errors"),
		BreatheDuration:        m.NewSummary(appName+"_breathe_duration_seconds", "breathe duration seconds"),
		SendDuration:           m.NewSummary(appName+"_send_duration_seconds", "send duration seconds"),
		AggregateSum:           m.NewSummary(appName+"_aggregatae_sum", "aggregate sum"),
	}

	go func() {
		for range time.Tick(time.Minute) {
			mm.Success.Update()
			mm.Errors.Update()
			mm.ErrorCampaignIdEmpty.Update()
			mm.ErrorOperatorCodeEmpty.Update()
		}
	}()

	return mm
}

type adAggregate struct {
	LpHits                 *counter `json:"lp_hits,omitempty"`
	LpMsisdnHits           *counter `json:"lp_msisdn_hits,omitempty"`
	MoTotal                *counter `json:"mo,omitempty"`
	MoChargeSuccess        *counter `json:"mo_charge_success,omitempty"`
	MoChargeSum            *counter `json:"mo_charge_sum,omitempty"`
	MoChargeFailed         *counter `json:"mo_charge_failed,omitempty"`
	MoRejected             *counter `json:"mo_rejected,omitempty"`
	Outflow                *counter `json:"outflow,omitempty"`
	RenewalTotal           *counter `json:"renewal,omitempty"`
	RenewalChargeSuccess   *counter `json:"renewal_charge_success,omitempty"`
	RenewalChargeSum       *counter `json:"renewal_charge_sum,omitempty"`
	RenewalFailed          *counter `json:"renewal_failed,omitempty"`
	InjectionTotal         *counter `json:"injection,omitempty"`
	InjectionChargeSuccess *counter `json:"injection_charge_success,omitempty"`
	InjectionChargeSum     *counter `json:"injection_charge_sum,omitempty"`
	InjectionFailed        *counter `json:"injection_failed,omitempty"`
	ExpiredTotal           *counter `json:"expired,omitempty"`
	ExpiredChargeSuccess   *counter `json:"expired_charge_success,omitempty"`
	ExpiredChargeSum       *counter `json:"expired_charge_sum,omitempty"`
	ExpiredFailed          *counter `json:"expired_failed,omitempty"`
	Pixels                 *counter `json:"pixels,omitempty"`
}

type counter struct {
	count int64
}

func (c *counter) Inc() {
	c.count++
}
func (c *counter) Add(amount int) {
	c.count = c.count + int64(amount)
}
func (c *counter) Set(amount int) {
	c.count = int64(amount)
}
func (a *adAggregate) Sum() int64 {
	return a.LpHits.count +
		a.LpMsisdnHits.count +
		a.MoTotal.count +
		a.MoChargeSuccess.count +
		a.MoChargeSum.count +
		a.MoChargeFailed.count +
		a.MoRejected.count +

		a.RenewalTotal.count +
		a.RenewalChargeSuccess.count +
		a.RenewalChargeSum.count +
		a.RenewalFailed.count +

		a.ExpiredTotal.count +
		a.ExpiredChargeSuccess.count +
		a.ExpiredChargeSum.count +
		a.ExpiredFailed.count +

		a.InjectionTotal.count +
		a.InjectionChargeSuccess.count +
		a.InjectionChargeSum.count +
		a.InjectionFailed.count +

		a.Outflow.count +
		a.Pixels.count
}
func newAdAggregate() adAggregate {
	return adAggregate{
		LpHits:                 &counter{},
		LpMsisdnHits:           &counter{},
		MoTotal:                &counter{},
		MoChargeSuccess:        &counter{},
		MoChargeSum:            &counter{},
		MoChargeFailed:         &counter{},
		MoRejected:             &counter{},
		Outflow:                &counter{},
		RenewalTotal:           &counter{},
		RenewalChargeSuccess:   &counter{},
		RenewalChargeSum:       &counter{},
		RenewalFailed:          &counter{},
		InjectionTotal:         &counter{},
		InjectionChargeSuccess: &counter{},
		InjectionChargeSum:     &counter{},
		InjectionFailed:        &counter{},
		ExpiredTotal:           &counter{},
		ExpiredChargeSuccess:   &counter{},
		ExpiredChargeSum:       &counter{},
		ExpiredFailed:          &counter{},
		Pixels:                 &counter{},
	}
}

func (a *adAggregate) generateReport(instanceId, campaignUUID string, operatorCode int64, reportAt time.Time) xmp_api_structs.Aggregate {
	campaignCode := "0"
	camp, err := Svc.Campaigns.GetByUUID(campaignUUID)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
			"uuid":  campaignUUID,
		}).Error("cannot get campaign code by uuid")
	}
	campaignCode = camp.Code
	return xmp_api_structs.Aggregate{
		ReportAt:               reportAt.UTC().Unix(),
		InstanceId:             instanceId,
		CampaignId:             campaignUUID,
		CampaignCode:           campaignCode,
		OperatorCode:           operatorCode,
		LpHits:                 a.LpHits.count,
		LpMsisdnHits:           a.LpMsisdnHits.count,
		MoTotal:                a.MoTotal.count,
		MoChargeSuccess:        a.MoChargeSuccess.count,
		MoChargeSum:            a.MoChargeSum.count,
		MoChargeFailed:         a.MoChargeFailed.count,
		MoRejected:             a.MoRejected.count,
		Outflow:                a.Outflow.count,
		RenewalTotal:           a.RenewalTotal.count,
		RenewalChargeSuccess:   a.RenewalChargeSuccess.count,
		RenewalChargeSum:       a.RenewalChargeSum.count,
		RenewalFailed:          a.RenewalFailed.count,
		InjectionTotal:         a.InjectionTotal.count,
		InjectionChargeSuccess: a.InjectionChargeSuccess.count,
		InjectionChargeSum:     a.InjectionChargeSum.count,
		InjectionFailed:        a.InjectionFailed.count,
		ExpiredTotal:           a.ExpiredTotal.count,
		ExpiredChargeSuccess:   a.ExpiredChargeSuccess.count,
		ExpiredChargeSum:       a.ExpiredChargeSum.count,
		ExpiredFailed:          a.ExpiredFailed.count,
		Pixels:                 a.Pixels.count,
	}
}

func initReporter(appName, stateFilePath string, queue QueuesConfig, consumerConf amqp.ConsumerConfig) Collector {
	as := &collectorService{}

	as.loadState(stateFilePath)
	as.m = initReporterMetrics(appName)
	as.consume = &Consumers{
		Hit:         amqp.InitConsumer(consumerConf, queue.ReporterHit, as.hitCh, as.processHit),
		Transaction: amqp.InitConsumer(consumerConf, queue.ReporterTransaction, as.transactionCh, as.processTransactions),
		Pixel:       amqp.InitConsumer(consumerConf, queue.ReporterPixel, as.pixelCh, as.processPixel),
		Outflow:     amqp.InitConsumer(consumerConf, queue.ReporterOutflow, as.outflowCh, as.processOutflow),
	}

	as.adReport = make(map[string]OperatorAgregate)
	go func() {
		for range time.Tick(time.Second) {
			as.send()
		}
	}()
	return as
}
func (as *collectorService) SaveState() {
	if !Svc.conf.Enabled.Reporter {
		return
	}
	if err := as.saveState(); err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
			"file":  as.state.FilePath,
		}).Fatal("cannot save state")
	}
}
func (as *collectorService) saveState() error {
	stateJson, err := json.Marshal(as.state)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return err
	}

	if err := ioutil.WriteFile(as.state.FilePath, stateJson, 0644); err != nil {
		err = fmt.Errorf("ioutil.WriteFile: %s", err.Error())
		return err
	}
	return nil
}

func (as *collectorService) loadState(filePath string) error {
	defer func() {
		as.state.FilePath = filePath
	}()

	logCtx := log.WithFields(log.Fields{
		"action": "load collector state",
		"file":   filePath,
	})
	stateJson, err := ioutil.ReadFile(filePath)
	if err != nil {
		err = fmt.Errorf("ioutil.ReadFile: %s", err.Error())
		logCtx.WithField("path", filePath).Error(err.Error())
		return err
	}
	if err = json.Unmarshal(stateJson, &as.state); err != nil {
		err = fmt.Errorf("json.Unmarshal: %s", err.Error())
		logCtx.WithField("path", filePath).Error(err.Error())
		return err
	}
	logCtx.WithField("path", filePath).Debug("checking time")
	if as.state.LastSendTime.IsZero() {
		as.state.LastSendTime = time.Now().UTC()
		logCtx.WithField("path", filePath).Warn("invalid time")
	}
	logCtx.Infof("%s, count: %s", as.state.LastSendTime.String(), len(as.state.Archive))
	return nil
}

func (as *collectorService) send() {
	as.Lock()
	defer as.Unlock()

	begin := time.Now()
	var data []interface{}
	aggregateSum := int64(.0)

	for campaignUUID, operatorAgregate := range as.adReport {
		for operatorCode, coa := range operatorAgregate {
			if coa.Sum() == 0 {
				continue
			}

			aggregateSum = aggregateSum + coa.Sum()

			data = append(data, coa.generateReport(
				Svc.xmpAPIConf.InstanceId,
				campaignUUID,
				operatorCode,
				time.Now(),
			))

		}
	}
	as.state.Archive = append(as.state.Archive, data...)
	as.breathe()

	if len(as.state.Archive) > 0 {
		log.WithFields(log.Fields{"took": time.Since(begin)}).Info("prepare")
		var resp struct {
			Ok    bool   `json:"ok,omitempty"`
			Error string `json:"error,omitempty"`
		}

		err := xmp_api.Call("aggregate", &resp, as.state.Archive...)
		if err != nil {
			as.m.Errors.Inc()
			log.WithFields(log.Fields{"error": err.Error()}).Error("cannot send data")
		} else if !resp.Ok {
			as.m.Errors.Inc()
			log.WithFields(log.Fields{"reason": resp.Error}).Warn("haven't received the data")
		} else {
			queueJson, _ := json.Marshal(as.state.Archive)
			log.WithFields(log.Fields{
				"count": len(as.state.Archive),
				"data":  string(queueJson),
			}).Debug("sent")
			as.state.Archive = []interface{}{}
		}
	}

	as.m.SendDuration.Observe(time.Since(begin).Seconds())
	as.m.AggregateSum.Observe(float64(aggregateSum))
}

// clean stats of a second.
func (as *collectorService) breathe() {
	begin := time.Now()
	for campaignUUID, operatorAgregate := range as.adReport {
		for operatorCode, _ := range operatorAgregate {
			delete(as.adReport[campaignUUID], operatorCode)
		}
		delete(as.adReport, campaignUUID)
	}
	log.WithFields(log.Fields{"took": time.Since(begin)}).Debug("breathe")
	as.m.BreatheDuration.Observe(time.Since(begin).Seconds())
}

// api call to get data from database
func (as *collectorService) GetAggregate(from, to time.Time) (res []xmp_api_structs.Aggregate, err error) {
	log.WithFields(log.Fields{
		"from": from.Format("2006-01-02"),
		"to":   to.Format("2006-01-02"),
	}).Debug("aggregate api get req")

	agg := make(map[string]CampaignAgregate) // time.Time (date) - campaign - operator code

	query := fmt.Sprintf("SELECT "+
		"date(sent_at), "+
		"id_campaign, "+
		"operator_code, "+
		"result, "+
		"sum(price), "+
		"count(*) "+
		"FROM %stransactions "+
		"WHERE sent_at > $1 AND sent_at < $2 "+
		"GROUP BY date(sent_at), id_campaign, operator_code, result",
		Svc.dbConf.TablePrefix,
	)
	var rows *sql.Rows

	rows, err = Svc.db.Query(query, from, to)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}

	var campaignUUID string
	var operatorCode int64
	var rowsCount int
	defer rows.Close()
	for rows.Next() {
		var sentAt string
		var result string
		var sum int
		var count int
		if err = rows.Scan(&sentAt, &campaignUUID, &operatorCode, &result, &sum, &count); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		sentAt = sentAt[0:10]
		rowsCount++
		if _, ok := agg[sentAt]; !ok {
			agg[sentAt] = CampaignAgregate{}
		}
		if _, ok := agg[sentAt][campaignUUID]; !ok {
			agg[sentAt][campaignUUID] = OperatorAgregate{}
		}
		if _, ok := agg[sentAt][campaignUUID][operatorCode]; !ok {
			agg[sentAt][campaignUUID][operatorCode] = newAdAggregate()
		}

		switch result {
		case "paid":
			agg[sentAt][campaignUUID][operatorCode].MoChargeSuccess.Set(count)
			agg[sentAt][campaignUUID][operatorCode].MoChargeSum.Set(sum)
			agg[sentAt][campaignUUID][operatorCode].MoTotal.Add(count)
		case "failed":
			agg[sentAt][campaignUUID][operatorCode].MoTotal.Add(count)
			agg[sentAt][campaignUUID][operatorCode].MoChargeFailed.Set(count)
		case "retry_paid":
			agg[sentAt][campaignUUID][operatorCode].RenewalChargeSuccess.Set(count)
			agg[sentAt][campaignUUID][operatorCode].RenewalChargeSum.Set(sum)
			agg[sentAt][campaignUUID][operatorCode].RenewalTotal.Add(count)
		case "retry_failed":
			agg[sentAt][campaignUUID][operatorCode].RenewalTotal.Add(count)
			agg[sentAt][campaignUUID][operatorCode].RenewalFailed.Set(count)
		case "injection_paid":
			agg[sentAt][campaignUUID][operatorCode].InjectionChargeSuccess.Set(count)
			agg[sentAt][campaignUUID][operatorCode].InjectionChargeSum.Set(sum)
			agg[sentAt][campaignUUID][operatorCode].InjectionTotal.Add(count)
		case "injection_failed":
			agg[sentAt][campaignUUID][operatorCode].InjectionTotal.Add(count)
			agg[sentAt][campaignUUID][operatorCode].InjectionFailed.Set(count)
		case "expired_paid":
			agg[sentAt][campaignUUID][operatorCode].MoChargeSuccess.Set(count)
			agg[sentAt][campaignUUID][operatorCode].MoChargeSum.Set(sum)
			agg[sentAt][campaignUUID][operatorCode].MoTotal.Add(count)
		case "expired_failed":
			agg[sentAt][campaignUUID][operatorCode].MoTotal.Add(count)
			agg[sentAt][campaignUUID][operatorCode].MoChargeFailed.Set(count)
		}
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	log.WithFields(log.Fields{
		"from":  from.Format("2006-01-02"),
		"to":    to.Format("2006-01-02"),
		"table": "transactions",
		"count": rowsCount,
	}).Debug("aggregate api get req")
	rowsCount = 0

	//============================
	query = fmt.Sprintf("SELECT "+
		"date(sent_at), "+
		"id_campaign, "+
		"operator_code, "+
		"count(*) "+
		"FROM %spixel_transactions "+
		"WHERE sent_at > $1 AND sent_at < $2 "+
		"GROUP BY date(sent_at), id_campaign, operator_code",
		Svc.dbConf.TablePrefix,
	)
	rows, err = Svc.db.Query(query, from, to)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var sentAt string
		var count int
		if err = rows.Scan(&sentAt, &campaignUUID, &operatorCode, &count); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		sentAt = sentAt[0:10]
		rowsCount++
		if _, ok := agg[sentAt]; !ok {
			agg[sentAt] = CampaignAgregate{}
		}
		if _, ok := agg[sentAt][campaignUUID]; !ok {
			agg[sentAt][campaignUUID] = OperatorAgregate{}
		}
		if _, ok := agg[sentAt][campaignUUID][operatorCode]; !ok {
			agg[sentAt][campaignUUID][operatorCode] = newAdAggregate()
		}
		agg[sentAt][campaignUUID][operatorCode].Pixels.Set(count)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	log.WithFields(log.Fields{
		"from":  from.Format("2006-01-02"),
		"to":    to.Format("2006-01-02"),
		"table": "pixel_transactions",
		"count": rowsCount,
	}).Debug("aggregate api get req")
	rowsCount = 0

	//============================
	query = fmt.Sprintf("SELECT "+
		"date(sent_at), "+
		"id_campaign, "+
		"operator_code, "+
		"CASE length(msisdn) WHEN 0 THEN false ELSE true END msisdn_present, "+
		"count(*) "+
		"FROM %scampaigns_access "+
		"WHERE sent_at > $1 AND sent_at < $2 "+
		"GROUP BY date(sent_at), msisdn_present, id_campaign, operator_code",
		Svc.dbConf.TablePrefix,
	)
	rows, err = Svc.db.Query(query, from, to)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var sentAt string
		var present bool
		var count int
		if err = rows.Scan(&sentAt, &campaignUUID, &operatorCode, &present, &count); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		rowsCount++
		sentAt = sentAt[0:10]
		if _, ok := agg[sentAt]; !ok {
			agg[sentAt] = CampaignAgregate{}
		}
		if _, ok := agg[sentAt][campaignUUID]; !ok {
			agg[sentAt][campaignUUID] = OperatorAgregate{}
		}
		if _, ok := agg[sentAt][campaignUUID][operatorCode]; !ok {
			agg[sentAt][campaignUUID][operatorCode] = newAdAggregate()
		}
		agg[sentAt][campaignUUID][operatorCode].LpHits.Add(count)
		if present {
			agg[sentAt][campaignUUID][operatorCode].LpMsisdnHits.Add(count)
		}
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	log.WithFields(log.Fields{
		"from":  from.Format("2006-01-02"),
		"to":    to.Format("2006-01-02"),
		"table": "campaigns_access",
		"count": rowsCount,
	}).Debug("aggregate api get req")
	rowsCount = 0

	res = []xmp_api_structs.Aggregate{}
	for dateSent, agByCampaign := range agg {
		for campaignUUID, agByOperatorCode := range agByCampaign {
			for operatorCode, ag := range agByOperatorCode {
				var reportAt time.Time
				reportAt, err = time.Parse("2006-01-02", dateSent)
				if err != nil {
					err = fmt.Errorf("time.Parse: %s", err.Error())
					return
				}
				res = append(res, ag.generateReport(
					Svc.xmpAPIConf.InstanceId,
					campaignUUID,
					operatorCode,
					reportAt,
				))
			}
		}

	}
	log.WithFields(log.Fields{
		"from": from.Format("2006-01-02"),
		"to":   to.Format("2006-01-02"),
		"len":  len(res),
	}).Debug("aggregate api get req")
	return
}

// map[campaign][operator]acceptor.Aggregate
func (as *collectorService) check(r Collect) error {
	if r.CampaignUUID == "" {
		as.m.Errors.Inc()
		as.m.ErrorCampaignIdEmpty.Inc()
		log.WithField("collect", fmt.Sprintf("%#v", r)).Error("campaign code is empty")
		return fmt.Errorf("CampaignIdEmpty: %#v", r)
	}

	if r.OperatorCode == 0 {
		as.m.Errors.Inc()
		as.m.ErrorOperatorCodeEmpty.Inc()
		log.WithField("collect", fmt.Sprintf("%#v", r)).Error("operator code is empty")
	}
	as.Lock()
	defer as.Unlock()

	// operator code == 0
	// unknown operator in access campaign
	if as.adReport == nil {
		as.adReport = make(map[string]OperatorAgregate)
	}
	_, found := as.adReport[r.CampaignUUID]
	if !found {
		as.adReport[r.CampaignUUID] = OperatorAgregate{}
	}
	_, found = as.adReport[r.CampaignUUID][r.OperatorCode]
	if !found {
		as.adReport[r.CampaignUUID][r.OperatorCode] = newAdAggregate()
	}
	as.m.Success.Inc()
	return nil
}
func (as *collectorService) incHit(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()

	as.adReport[r.CampaignUUID][r.OperatorCode].LpHits.Inc()
	if r.Msisdn != "" {
		as.adReport[r.CampaignUUID][r.OperatorCode].LpMsisdnHits.Inc()
	}
	return nil
}
func (as *collectorService) incTransaction(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()

	if r.AttemptsCount == 0 {
		as.adReport[r.CampaignUUID][r.OperatorCode].MoTotal.Inc()
		if r.TransactionResult == "paid" {
			as.adReport[r.CampaignUUID][r.OperatorCode].MoChargeSuccess.Inc()
			as.adReport[r.CampaignUUID][r.OperatorCode].MoChargeSum.Add(r.Price)
		}
		if r.TransactionResult == "rejected" {
			as.adReport[r.CampaignUUID][r.OperatorCode].MoRejected.Inc()
		}
		if r.TransactionResult == "failed" {
			as.adReport[r.CampaignUUID][r.OperatorCode].MoChargeFailed.Inc()
		}
		log.WithField("tid", r.Tid).Debug("mo")
		return nil
	}

	if strings.Contains(r.TransactionResult, "retry") {
		as.adReport[r.CampaignUUID][r.OperatorCode].RenewalTotal.Inc()

		if strings.Contains(r.TransactionResult, "retry_paid") {
			as.adReport[r.CampaignUUID][r.OperatorCode].RenewalChargeSuccess.Inc()
			as.adReport[r.CampaignUUID][r.OperatorCode].RenewalChargeSum.Add(r.Price)
		}

		if strings.Contains(r.TransactionResult, "retry_failed") {
			as.adReport[r.CampaignUUID][r.OperatorCode].RenewalFailed.Inc()
		}
		log.WithField("tid", r.Tid).Debug("retry")
		return nil
	}

	if strings.Contains(r.TransactionResult, "injection") {
		as.adReport[r.CampaignUUID][r.OperatorCode].InjectionTotal.Inc()

		if strings.Contains(r.TransactionResult, "injection_paid") {
			as.adReport[r.CampaignUUID][r.OperatorCode].InjectionChargeSuccess.Inc()
			as.adReport[r.CampaignUUID][r.OperatorCode].InjectionChargeSum.Add(r.Price)
		}

		if strings.Contains(r.TransactionResult, "injection_failed") {
			as.adReport[r.CampaignUUID][r.OperatorCode].InjectionFailed.Inc()
		}
		log.WithField("tid", r.Tid).Debug("injection")
		return nil
	}

	if strings.Contains(r.TransactionResult, "expired") {
		as.adReport[r.CampaignUUID][r.OperatorCode].ExpiredTotal.Inc()

		if strings.Contains(r.TransactionResult, "expired_paid") {
			as.adReport[r.CampaignUUID][r.OperatorCode].ExpiredChargeSuccess.Inc()
			as.adReport[r.CampaignUUID][r.OperatorCode].ExpiredChargeSum.Add(r.Price)
		}

		if strings.Contains(r.TransactionResult, "expired_failed") {
			as.adReport[r.CampaignUUID][r.OperatorCode].ExpiredFailed.Inc()
		}
		log.WithField("tid", r.Tid).Debug("expired")
		return nil
	}

	return nil
}
func (as *collectorService) incOutflow(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()

	if strings.Contains(r.TransactionResult, "inact") ||
		strings.Contains(r.TransactionResult, "purge") ||
		strings.Contains(r.TransactionResult, "cancel") {
		log.WithField("tid", r.Tid).Debug("outflow")
		as.adReport[r.CampaignUUID][r.OperatorCode].Outflow.Inc()
	}
	return nil
}
func (as *collectorService) incPixel(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()
	log.WithField("tid", r.Tid).Debug("pixel")
	as.adReport[r.CampaignUUID][r.OperatorCode].Pixels.Inc()
	return nil
}

type EventNotifyReporter struct {
	EventName string  `json:"event_name,omitempty"`
	EventData Collect `json:"event_data,omitempty"`
}

func (as *collectorService) processHit(deliveries <-chan amqp_driver.Delivery) {

	for msg := range deliveries {
		var c EventNotifyReporter

		if !Svc.conf.Enabled.Reporter {
			goto ack
		}
		if err := json.Unmarshal(msg.Body, &c); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"body":  string(msg.Body),
				"msg":   "dropped",
			}).Error("failed")
		} else {
			as.incHit(c.EventData)
		}
	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"mo":    string(msg.Body),
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
func (as *collectorService) processPixel(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var c EventNotifyReporter

		if !Svc.conf.Enabled.Reporter {
			goto ack
		}
		if err := json.Unmarshal(msg.Body, &c); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"body":  string(msg.Body),
				"msg":   "dropped",
			}).Error("failed")
		} else {
			as.incPixel(c.EventData)
		}
	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"mo":    string(msg.Body),
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
func (as *collectorService) processTransactions(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var c EventNotifyReporter
		if !Svc.conf.Enabled.Reporter {
			goto ack
		}
		if err := json.Unmarshal(msg.Body, &c); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"body":  string(msg.Body),
				"msg":   "dropped",
			}).Error("failed")
		} else {
			as.incTransaction(c.EventData)
		}
	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"mo":    string(msg.Body),
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
func (as *collectorService) processOutflow(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var c EventNotifyReporter
		if !Svc.conf.Enabled.Reporter {
			goto ack
		}
		if err := json.Unmarshal(msg.Body, &c); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"body":  string(msg.Body),
				"msg":   "dropped",
			}).Error("failed")
		} else {
			as.incOutflow(c.EventData)
		}
	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"mo":    string(msg.Body),
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
