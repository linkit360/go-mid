package service

import (
	"database/sql"
	"encoding/json"
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
	Name          string
	Rps           int
	Settings      string
	Code          int64
	CountryName   string
	MsisdnHeaders []string
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
		"( SELECT %scountries.name as country_name FROM %scountries WHERE country_code = code ), "+
		"msisdn_headers "+
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

	operatorLoadHeaderError := false
	var operators []Operator
	for rows.Next() {
		var operator Operator
		var headers string
		if err = rows.Scan(
			&operator.Name,
			&operator.Code,
			&operator.Rps,
			&operator.Settings,
			&operator.CountryName,
			&headers,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		decodedHeaders := make([]string, 0)
		if err := json.Unmarshal([]byte(headers), &decodedHeaders); err != nil {
			err = fmt.Errorf("json.Unmarshal: %s", err.Error())
			operatorLoadHeaderError = true
		} else {
			operator.MsisdnHeaders = decodedHeaders
		}
		operator.Name = strings.ToLower(operator.Name)
		operator.CountryName = strings.ToLower(operator.CountryName)
		operators = append(operators, operator)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	if operatorLoadHeaderError == false {
		loadOperatorHeaderError.Set(0.)
	} else {
		loadOperatorHeaderError.Set(1.)
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
