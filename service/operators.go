package service

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

// Tasks:
// Keep in memory all operators names and configuration
// Reload when changes to operators table are done

type Operators struct {
	sync.RWMutex
	ByCode map[int64]Operator
	ByName map[string]Operator
}

type Operator struct {
	Name        string
	Rps         int
	Settings    string
	Code        int64
	CountryName string
}

func (ops *Operators) Reload() error {
	ops.Lock()
	defer ops.Unlock()

	var err error
	query := fmt.Sprintf("SELECT "+
		"name, "+
		"code,  "+
		"rps, "+
		"settings, "+
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

	var operators []Operator
	for rows.Next() {
		var operator Operator
		if err = rows.Scan(
			&operator.Name,
			&operator.Code,
			&operator.Rps,
			&operator.Settings,
			&operator.CountryName,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		operator.Name = strings.ToLower(operator.Name)
		operator.CountryName = strings.ToLower(operator.CountryName)
		operators = append(operators, operator)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	ops.ByName = make(map[string]Operator, len(operators))
	for _, op := range operators {
		name := strings.ToLower(op.Name)
		ops.ByName[name] = op
	}

	ops.ByCode = make(map[int64]Operator, len(operators))
	for _, op := range operators {
		ops.ByCode[op.Code] = op
	}
	return nil
}
