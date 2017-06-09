package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	m "github.com/linkit360/go-utils/metrics"
	"github.com/prometheus/client_golang/prometheus"

	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

// required only in pixels to send operator name (not code as we use)

type Operators interface {
	Reload() error
	Apply(map[int64]xmp_api_structs.Operator)
	Update(xmp_api_structs.Operator) error
	GetByCode(int64) (xmp_api_structs.Operator, error)
	GetJson() string
	ShowLoaded()
}

type OperatorsConfig struct {
	FromControlPanel bool `yaml:"from_control_panel"`
}
type operators struct {
	sync.RWMutex
	conf      OperatorsConfig
	notFound  m.Gauge
	loadError prometheus.Gauge
	ByCode    map[int64]xmp_api_structs.Operator
}

func initOperators(appName string, opConf OperatorsConfig) Operators {
	ops := &operators{
		conf:      opConf,
		notFound:  m.NewGauge(appName, "operator", "not_found", "operator not found error"),
		loadError: m.PrometheusGauge(appName, "operator", "load_error", "operator load error"),
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
	s.loadError.Set(1)
	for _, ac := range operators {
		if ac.Name == "" {
			s.loadError.Set(1)
			log.Error("operator name is empty")
			ac.Name = strings.ToLower(ac.Name)
		}
		if ac.Code == 0 {
			s.loadError.Set(1)
			log.Error("operator code is empty")
			continue
		}
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
		"code  "+
		"FROM %soperators",
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
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
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
func (ops *operators) GetJson() string {
	sJson, _ := json.Marshal(ops.ByCode)
	return string(sJson)
}

func (ops *operators) ShowLoaded() {
	byCode, _ := json.Marshal(ops.ByCode)

	log.WithFields(log.Fields{
		"len":    len(ops.ByCode),
		"bycode": string(byCode),
	}).Debug("operators")
}
