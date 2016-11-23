package service

import (
	"database/sql"
	"fmt"
	"sync"
)

type PostPaid struct {
	sync.RWMutex
	ByMsisdn map[string]struct{}
}

func (pp *PostPaid) Reload() error {
	pp.Lock()
	defer pp.Unlock()

	query := fmt.Sprintf("SELECT"+
		" msisdn "+
		"FROM %smsisdn_postpaid",
		Svc.dbConf.TablePrefix)

	var err error
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var postPaidList []string
	for rows.Next() {
		var msisdn string
		if err = rows.Scan(&msisdn); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		postPaidList = append(postPaidList, msisdn)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	pp.ByMsisdn = make(map[string]struct{}, len(postPaidList))
	for _, msisdn := range postPaidList {
		pp.ByMsisdn[msisdn] = struct{}{}
	}
	return nil
}
func (pp *PostPaid) Push(msisdn string) {
	pp.Lock()
	defer pp.Unlock()
	pp.ByMsisdn[msisdn] = struct{}{}
}

func (pp *PostPaid) Remove(msisdn string) {
	pp.Lock()
	defer pp.Unlock()
	delete(pp.ByMsisdn, msisdn)
}
