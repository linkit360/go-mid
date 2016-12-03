package service

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"
)

const ACTIVE_STATUS = 1

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

// When updating from database, reading is forbidden
// Map structure: map [ msisdn + service_id ] []content_id
// where
// * msisdn + service_id -- is a sentCOntent key (see below) (could be changed to msisdn)
// * content_id is content that was shown to msisdn
type SentContents struct {
	sync.RWMutex
	ByKey map[string]map[int64]struct{}
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
	s.Lock()
	defer s.Unlock()

	query := fmt.Sprintf("SELECT "+
		"msisdn, "+
		"id_service, "+
		"id_content "+
		"FROM %scontent_sent "+
		"WHERE sent_at > (CURRENT_TIMESTAMP - INTERVAL '"+
		strconv.Itoa(Svc.conf.UniqueDays)+" days')",
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
	if contentIds, ok = s.ByKey[t.key()]; ok {
		return contentIds
	}
	return nil
}

// When there is no content avialabe for the msisdn, reset the content counter
// Breakes after reloading sent content table (on the restart of the application)
func (s *SentContents) Clear(msisdn string, serviceId int64) {
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	delete(s.ByKey, t.key())
}

// After we have chosen the content to show,
// we notice it in sent content table (another place)
// and also we need to update in-memory cache of used content id for this msisdn and service id
func (s *SentContents) Push(msisdn string, serviceId int64, contentId int64) {
	t := ContentSentProperties{Msisdn: msisdn, ServiceId: serviceId}
	if _, ok := s.ByKey[t.key()]; !ok {
		s.ByKey[t.key()] = make(map[int64]struct{})
	}
	s.ByKey[t.key()][contentId] = struct{}{}
}
