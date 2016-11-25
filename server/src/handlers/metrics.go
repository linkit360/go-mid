package handlers

import (
	m "github.com/vostrok/utils/metrics"
	"time"
)

var (
	notFound             m.Gauge
	campaignNotFound     m.Gauge
	operatorNotFound     m.Gauge
	pixelSettingNotFound m.Gauge
)

func init() {
	notFound = m.NewGauge("", "", "nil", "not found")
	campaignNotFound = m.NewGauge("", "service", "campaign_not_found", "campaign not found")
	operatorNotFound = m.NewGauge("", "operator", "not_found", "operator not found")
	pixelSettingNotFound = m.NewGauge("", "pixel_setting", "not_found", "pixel setting not found")

	go func() {
		for range time.Tick(time.Minute) {
			notFound.Update()
			campaignNotFound.Update()
			operatorNotFound.Update()
			pixelSettingNotFound.Update()
		}
	}()
}
