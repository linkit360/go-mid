package service

import (
	"database/sql"
	"fmt"
	"sync"

	acceptor "github.com/linkit360/go-acceptor-structs"
)

// required only in pixels to send operator name (not code as we use)

type Operators struct {
	sync.RWMutex
	ByCode map[int64]acceptor.Operator
}

func (ops *Operators) Reload() error {
	ops.Lock()
	defer ops.Unlock()

	var err error
	query := fmt.Sprintf("SELECT "+
		"name, "+
		"code,  "+
		"( SELECT %scountries.name as country_name FROM %scountries WHERE country_code = code ) "+
		"FROM %soperators",
		Svc.dbConf.TablePrefix,
		Svc.dbConf.TablePrefix,
		Svc.dbConf.TablePrefix,
	)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var operators []acceptor.Operator
	for rows.Next() {
		var operator acceptor.Operator
		if err = rows.Scan(
			&operator.Name,
			&operator.Code,
			&operator.CountryName,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		operators = append(operators, operator)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}
	ops.ByCode = make(map[int64]acceptor.Operator, len(operators))
	for _, op := range operators {
		ops.ByCode[op.Code] = op
	}
	return nil
}
