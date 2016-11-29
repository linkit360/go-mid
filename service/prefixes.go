package service

import (
	"database/sql"
	"fmt"
	"sync"
)

// Tasks:
// Keep in memory all active operator prefixes
// Reload when changes to prefixes are done
type Prefixes struct {
	sync.RWMutex
	OperatorCodeByPrefix map[string]int64
}

type prefix struct {
	Prefix       string
	OperatorCode int64
}

func (pp *Prefixes) Reload() error {
	pp.Lock()
	defer pp.Unlock()

	query := fmt.Sprintf("SELECT "+
		"operator_code, "+
		"prefix "+
		"FROM %soperator_msisdn_prefix",
		Svc.dbConf.TablePrefix,
	)

	var err error
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var prefixes []prefix
	for rows.Next() {
		var p prefix
		if err = rows.Scan(
			&p.OperatorCode,
			&p.Prefix,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		prefixes = append(prefixes, p)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	pp.OperatorCodeByPrefix = make(map[string]int64, len(prefixes))
	for _, p := range prefixes {
		pp.OperatorCodeByPrefix[p.Prefix] = p.OperatorCode
	}
	return nil
}
