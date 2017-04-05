package service

// campaign ids by keyword in sms (yondu,etc)

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type KeyWords struct {
	sync.RWMutex
	ByKeyWord map[string]int64
}
type KeyWord struct {
	KeyWord    string `json:"key_word"`
	CampaignId int64  `json:"id_campaign"`
}

func (kws *KeyWords) Reload() error {
	kws.Lock()
	defer kws.Unlock()

	query := fmt.Sprintf("SELECT "+
		"keyword, "+
		"id_campaign "+
		"FROM %scampaigns_keywords",
		Svc.dbConf.TablePrefix)
	var err error
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var keywords []KeyWord
	for rows.Next() {
		var kw KeyWord
		if err = rows.Scan(
			&kw.KeyWord,
			&kw.CampaignId,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		log.Debugf("%#v", kw)
		keywords = append(keywords, kw)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	kws.ByKeyWord = make(map[string]int64, len(keywords))
	for _, kw := range keywords {
		kws.ByKeyWord[strings.ToLower(kw.KeyWord)] = kw.CampaignId
	}
	return nil
}
