package service

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	m "github.com/linkit360/go-utils/metrics"

	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

// required only in pixels to send operator name (not code as we use)

type Operators interface {
	Reload() error
	Apply(map[int64]xmp_api_structs.Operator)
	Update(xmp_api_structs.Operator) error
	GetByCode(int64) (xmp_api_structs.Operator, error)
}

type OperatorsConfig struct {
	FromControlPanel bool `yaml:"from_control_panel"`
}
type operators struct {
	sync.RWMutex
	conf     OperatorsConfig
	notFound m.Gauge
	ByCode   map[int64]xmp_api_structs.Operator
}

func initOperators(appName string, opConf OperatorsConfig) Operators {
	ops := &operators{
		conf:     opConf,
		notFound: m.NewGauge(appName, "operator", "not_found", "operator not found error"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			ops.notFound.Update()
		}
	}()
	return ops
}

func (s *operators) GetByCode(code int64) (xmp_api_structs.Operator, error) {
	if op, ok := s.ByCode[code]; ok {
		return op, nil
	}
	return xmp_api_structs.Operator{}, errNotFound()
}

func (s *operators) Apply(operators map[int64]xmp_api_structs.Operator) {
	s.ByCode = make(map[int64]xmp_api_structs.Operator, len(operators))
	for _, ac := range operators {
		s.ByCode[ac.Code] = ac
	}
}

func (s *operators) Update(operator xmp_api_structs.Operator) error {
	if !s.conf.FromControlPanel {
		return fmt.Errorf("Disabled%s", "")
	}
	s.ByCode[operator.Code] = operator
	return nil
}

func (ops *operators) Reload() error {
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

	var operators []xmp_api_structs.Operator
	for rows.Next() {
		var operator xmp_api_structs.Operator
		if err = rows.Scan(
			&operator.Name,
			&operator.Code,
			&operator.CountryName,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		operator.CountryName = strings.ToLower(operator.CountryName)
		operator.Name = strings.ToLower(operator.Name)
		operators = append(operators, operator)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}
	ops.ByCode = make(map[int64]xmp_api_structs.Operator, len(operators))
	for _, op := range operators {
		ops.ByCode[op.Code] = op
	}
	return nil
}
