package service

import (
	m "github.com/vostrok/utils/metrics"
)

var (
	calls m.Gauge
	errs  m.Gauge
)

func initMetrics() {
	calls = m.NewGauge("", "service", "call", "Number of calls")
	errs = m.NewGauge("", "service", "call_errors", "Number of errors inside calls")
}
