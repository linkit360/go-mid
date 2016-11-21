package service

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

const ACTIVE_STATUS = 1

func init() {
	log.SetLevel(log.DebugLevel)
}

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all content ids of given service id
// Reload when changes to service_content or service are done
type Services struct {
	sync.RWMutex
	ById map[int64]Service
}
type Service struct {
	Id         int64
	Price      float64
	PaidHours  int
	DelayHours int
	ContentIds []int64
}
type Content struct {
	Id   int64
	Path string
	Name string
}
type ServiceContent struct {
	IdService int64
	IdContent int64
}

func (s *Services) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("services reload...")
	begin := time.Now()
	defer func(err error) {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("service reload")
	}(err)

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"paid_hours, "+
		"delay_hours, "+
		"price "+
		"from %sservices where status = $1",
		Svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var svcs []Service
	for rows.Next() {
		var srv Service
		if err = rows.Scan(
			&srv.Id,
			&srv.PaidHours,
			&srv.DelayHours,
			&srv.Price,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		svcs = append(svcs, srv)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	svcMap := make(map[int64]Service)
	for _, v := range svcs {
		svcMap[v.Id] = v
	}

	serviceIdsStr := []string{}
	for _, v := range svcs {
		serviceIdsStr = append(serviceIdsStr, strconv.FormatInt(v.Id, 10))
	}
	query = fmt.Sprintf("select id_service, id_content from %sservice_content where status = $1"+
		" and id_service = any($2::integer[])", Svc.dbConf.TablePrefix)
	rows, err = Svc.db.Query(query, ACTIVE_STATUS, "{"+strings.Join(serviceIdsStr, ", ")+"}")
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var serviceContentAr []ServiceContent
	for rows.Next() {
		var serviceContent ServiceContent
		if err = rows.Scan(
			&serviceContent.IdService,
			&serviceContent.IdContent,
		); err != nil {
			err = fmt.Errorf("rows.Scan %s", err.Error())
			return
		}
		serviceContentAr = append(serviceContentAr, serviceContent)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Error: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.ById = make(map[int64]Service)

	for _, serviceContent := range serviceContentAr {
		srv, ok := s.ById[serviceContent.IdService]
		if !ok {
			s.ById[serviceContent.IdService] = Service{}
		}
		srv.ContentIds = append(srv.ContentIds, serviceContent.IdContent)
		srv.Id = serviceContent.IdService
		svc := svcMap[serviceContent.IdService]
		srv.Price = svc.Price
		srv.DelayHours = svc.DelayHours
		srv.PaidHours = svc.PaidHours
		s.ById[serviceContent.IdService] = srv

	}
	return nil
}
func (s *Services) Get(serviceId int64) (contentIds []int64) {
	if svc, ok := s.ById[serviceId]; ok {
		return svc.ContentIds
	}
	return []int64{}
}

// Tasks:
// Keep in memory all active content_ids mapping to their object string (url path to content)
// Allow to get object for given content id
// Reload when changes to content
type Contents struct {
	sync.RWMutex
	ById map[int64]Content
}

func (s *Contents) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("content reload...")
	begin := time.Now()
	defer func(err error) {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("content reload")
	}(err)

	query := fmt.Sprintf("select "+
		"id, "+
		"object, "+
		"content_name "+
		"from %scontent where status = $1",
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

	s.Lock()
	defer s.Unlock()

	s.ById = make(map[int64]Content)
	for _, content := range contents {
		s.ById[content.Id] = content
	}
	return nil
}

// Tasks:
// Keep in memory all active campaigns
// Allow to get a service_id by campaign hash fastly
// Reload when changes to campaigns are done
type Campaigns struct {
	sync.RWMutex
	ByHash map[string]Campaign
}
type Campaign struct {
	Hash      string
	Id        int64
	ServiceId int64
}

func (s *Campaigns) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("campaign reload...")
	begin := time.Now()
	defer func(err error) {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("campaign reload")
	}(err)

	query := fmt.Sprintf("select id, hash, service_id_1 from %scampaigns where status = $1",
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
			&record.ServiceId,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.ByHash = make(map[string]Campaign, len(records))
	for _, campaign := range records {
		s.ByHash[campaign.Hash] = campaign
	}
	log.Debug(fmt.Sprintf("%#v", Svc.Campaigns.ByHash))
	return nil
}

// When updating from database, reading is forbidden
// Map structure: map [ msisdn + service_id ] []content_id
// where
// * msisdn + service_id -- is a sentCOntent key (see below) (could be changed to msisdn)
// * content_id is content that was shown to msisdn
type SentContents struct {
	sync.RWMutex
	ByKey map[string]map[int64]struct{}
}

// sent content Data that neded to build in-memory cache of used content-ids
// and alos need for recording "got content"
type ContentSentProperties struct {
	Msisdn         string `json:"msisdn"`
	Tid            string `json:"tid"`
	Price          int    `json:"price"`
	ContentPath    string `json:"content_path"`
	ContentName    string `json:"content_name"`
	CapmaignHash   string `json:"capmaign_hash"`
	CampaignId     int64  `json:"campaign_id"`
	ContentId      int64  `json:"content_id"`
	ServiceId      int64  `json:"service_id"`
	SubscriptionId int64  `json:"subscription_id"`
	CountryCode    int64  `json:"country_code"`
	OperatorCode   int64  `json:"operator_code"`
	PaidHours      int    `json:"paid_hours"`
	DelayHours     int    `json:"delay_hours"`
	Publisher      string `json:"publisher"`
	Pixel          string `json:"pixel"`
	Error          string `json:"error"`
}

// Used to get a key of used content ids
// when key == msisdn, then uniq content exactly
// when key == msisdn + service+id, then unique content per sevice
func (t ContentSentProperties) key() string {
	return t.Msisdn + "-" + strconv.FormatInt(t.ServiceId, 10)
}

// Load sent contents to filter content that had been seen by the msisdn.
// created at == before date specified in config
func (s *SentContents) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("content_sent reload...")
	begin := time.Now()
	defer func(err error) {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("content_sent reload")
	}(err)

	query := fmt.Sprintf("select msisdn, id_service, id_content "+
		"from %scontent_sent "+
		"where sent_at > (CURRENT_TIMESTAMP - INTERVAL '"+
		strconv.Itoa(Svc.UniqueDays)+" days')",
		Svc.dbConf.TablePrefix)

	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var records []ContentSentProperties
	for rows.Next() {
		record := ContentSentProperties{}

		if err = rows.Scan(
			&record.Msisdn,
			&record.ServiceId,
			&record.ContentId,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.ByKey = make(map[string]map[int64]struct{})
	for _, sentContent := range records {
		if _, ok := s.ByKey[sentContent.key()]; !ok {
			s.ByKey[sentContent.key()] = make(map[int64]struct{})
		}
		s.ByKey[sentContent.key()][sentContent.ContentId] = struct{}{}
	}
	return nil
}

// Get content ids that was seen by msisdn
// Attention: filtered by service id also,
// so if we would have had content id on one service and the same content id on another service as a content id
// then it had used as different contens! And will shown
func (s *SentContents) Get(msisdn string, serviceId int64) (contentIds map[int64]struct{}) {
	var ok bool
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	log.WithFields(log.Fields{
		"key": t.key(),
	}).Debug("get contents")
	if contentIds, ok = s.ByKey[t.key()]; ok {
		return contentIds
	}
	return nil
}

// When there is no content avialabe for the msisdn, reset the content counter
// Breakes after reloading sent content table (on the restart of the application)
func (s *SentContents) Clear(msisdn string, serviceId int64) {
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	log.WithFields(log.Fields{
		"key": t.key(),
	}).Debug("reset cache")
	delete(s.ByKey, t.key())
}

// After we have chosen the content to show,
// we notice it in sent content table (another place)
// and also we need to update in-memory cache of used content id for this msisdn and service id
func (s *SentContents) Push(msisdn string, serviceId int64, contentId int64) {
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	log.WithFields(log.Fields{
		"key": t.key(),
	}).Debug("push contentid")
	if _, ok := s.ByKey[t.key()]; !ok {
		s.ByKey[t.key()] = make(map[int64]struct{})
	}
	s.ByKey[t.key()][contentId] = struct{}{}
}
