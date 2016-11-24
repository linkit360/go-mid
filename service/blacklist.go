package service

// very large table.
// probably, need another type of searching blacklisted numbers
import (
	"database/sql"
	"fmt"
	"sync"
)

type BlackList struct {
	sync.RWMutex
	ByMsisdn map[string]struct{}
}

func (bl *BlackList) Reload() error {
	bl.Lock()
	defer bl.Unlock()

	query := fmt.Sprintf("SELECT "+
		"msisdn "+
		"FROM %smsisdn_blacklist",
		Svc.dbConf.TablePrefix)
	var err error
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var blackList []string
	for rows.Next() {
		var msisdn string
		if err = rows.Scan(&msisdn); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		blackList = append(blackList, msisdn)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	bl.ByMsisdn = make(map[string]struct{}, len(blackList))
	for _, msisdn := range blackList {
		bl.ByMsisdn[msisdn] = struct{}{}
	}
	return nil
}
