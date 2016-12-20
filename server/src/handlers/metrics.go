package handlers

import (
	"time"

	m "github.com/vostrok/utils/metrics"
)

var (
	notFound             m.Gauge
	campaignNotFound     m.Gauge
	operatorNotFound     m.Gauge
	unknownPrefix        m.Gauge
	pixelSettingNotFound m.Gauge
	keyWordNotFound      m.Gauge
)

func inmemMetric(appname, name string) m.Gauge {
	return m.NewGauge("", appname, name, name)
}

func InitMetrics(appName string) {
	notFound = inmemMetric(appName, "404")
	campaignNotFound = inmemMetric(appName, "campaign_not_found")
	operatorNotFound = inmemMetric(appName, "operator_not_found")
	unknownPrefix = inmemMetric(appName, "prefix_unknown")
	pixelSettingNotFound = inmemMetric(appName, "pixel_setting_not_found")
	keyWordNotFound = inmemMetric(appName, "keyword_not_found")

	go func() {
		for range time.Tick(time.Minute) {
			notFound.Update()
			campaignNotFound.Update()
			operatorNotFound.Update()
			unknownPrefix.Update()
			pixelSettingNotFound.Update()
			keyWordNotFound.Update()
		}
	}()
}
