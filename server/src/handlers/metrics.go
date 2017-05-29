package handlers

import (
	"time"

	m "github.com/linkit360/go-utils/metrics"
)

var (
	success          m.Gauge
	errors           m.Gauge
	notFound         m.Gauge
	urlCacheNotFound m.Gauge
	operatorNotFound m.Gauge
	unknownPrefix    m.Gauge
	keyWordNotFound  m.Gauge
)

func midMetric(appname, name string) m.Gauge {
	return m.NewGauge("", appname, name, name)
}

func InitMetrics(appName string) {
	success = m.NewGauge("", "", "success", "success")
	errors = m.NewGauge("", "", "errors", "errors")
	notFound = midMetric(appName, "404")
	urlCacheNotFound = midMetric(appName, "uniqueurl_not_found")
	operatorNotFound = midMetric(appName, "operator_not_found")
	unknownPrefix = midMetric(appName, "prefix_unknown")
	keyWordNotFound = midMetric(appName, "keyword_not_found")

	go func() {
		for range time.Tick(time.Minute) {
			success.Update()
			errors.Update()
			notFound.Update()
			urlCacheNotFound.Update()
			operatorNotFound.Update()
			unknownPrefix.Update()
			keyWordNotFound.Update()
		}
	}()
}
