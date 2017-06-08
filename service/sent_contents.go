package service

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"

	"github.com/linkit360/go-utils/structs"
)

const ACTIVE_STATUS = 1

type SentContents struct {
	sync.RWMutex
	ByKey map[string]map[string]struct{}
}

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

	var records []structs.ContentSentProperties
	for rows.Next() {
		record := structs.ContentSentProperties{}

		if err = rows.Scan(
			&record.Msisdn,
			&record.ServiceCode,
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

	s.ByKey = make(map[string]map[string]struct{})
	for _, sentContent := range records {
		if _, ok := s.ByKey[sentContent.Key()]; !ok {
			s.ByKey[sentContent.Key()] = make(map[string]struct{})
		}
		s.ByKey[sentContent.Key()][sentContent.ContentId] = struct{}{}
	}
	return nil
}

// Get content ids that was seen by msisdn
// Attention: filtered by service id also,
// so if we would have had content id on one service and the same content id on another service as a content id
// then it had used as different contens! And will shown
func (s *SentContents) Get(msisdn, serviceCode string) (contentCodes map[string]struct{}) {
	var ok bool
	t := structs.ContentSentProperties{Msisdn: msisdn, ServiceCode: serviceCode}
	if contentCodes, ok = s.ByKey[t.Key()]; ok {
		return contentCodes
	}
	return nil
}

// When there is no content avialabe for the msisdn, reset the content counter
// Breakes after reloading sent content table (on the restart of the application)
func (s *SentContents) Clear(msisdn, serviceCode string) {
	s.Lock()
	defer s.Unlock()

	t := structs.ContentSentProperties{Msisdn: msisdn, ServiceCode: serviceCode}
	delete(s.ByKey, t.Key())
}

// After we have chosen the content to show,
// we notice it in sent content table (another place)
// and also we need to update in-memory cache of used content id for this msisdn and service id
func (s *SentContents) Push(msisdn, serviceCode string, contentCode string) {
	s.Lock()
	defer s.Unlock()

	if s.ByKey == nil {
		s.ByKey = make(map[string]map[string]struct{})
	}
	t := structs.ContentSentProperties{Msisdn: msisdn, ServiceCode: serviceCode}
	if _, ok := s.ByKey[t.Key()]; !ok {
		s.ByKey[t.Key()] = make(map[string]struct{})
	}
	s.ByKey[t.Key()][contentCode] = struct{}{}
}
