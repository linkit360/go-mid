package service

import (
	"database/sql"
	"fmt"
	"net"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/gin-gonic/gin"
	"github.com/vostrok/utils/cqr"
	"github.com/vostrok/utils/db"
	m "github.com/vostrok/utils/metrics"
)

var Svc MemService

type MemService struct {
	db              *sql.DB
	dbConf          db.DataBaseConfig
	conf            Config
	cqrConfig       []cqr.CQRConfig
	privateIPRanges []IpRange
	Campaigns       *Campaigns
	Services        *Services
	Contents        *Contents
	SentContents    *SentContents
	IpRanges        *IpRanges
	Operators       *Operators
	Prefixes        *Prefixes
	BlackList       *BlackList
	PostPaid        *PostPaid
	PixelSettings   *PixelSettings
	KeyWords        *KeyWords
}

type Config struct {
	UniqueDays      int       `yaml:"unique_days" default:"10"`
	StaticPath      string    `yaml:"static_path" default:""`
	RedirectUrl     string    `yaml:"redirect_url" default:"http://id.slypee.com" `
	CampaignWebHook string    `yaml:"campaign_web_hook" default:"http://localhost:50300/updateTemplates"`
	PrivateIpRanges []IpRange `yaml:"private_networks"`
}

func Init(
	svcConf Config,
	dbConf db.DataBaseConfig,

) {
	log.SetLevel(log.DebugLevel)
	initMetrics()

	Svc.db = db.Init(dbConf)
	Svc.dbConf = dbConf
	Svc.conf = svcConf

	Svc.privateIPRanges = loadPrivateIpRanges(svcConf.PrivateIpRanges)

	Svc.Campaigns = &Campaigns{}
	Svc.Services = &Services{}
	Svc.Contents = &Contents{}
	Svc.SentContents = &SentContents{}
	Svc.IpRanges = &IpRanges{}
	Svc.Operators = &Operators{}
	Svc.Prefixes = &Prefixes{}
	Svc.BlackList = &BlackList{}
	Svc.PostPaid = &PostPaid{}
	Svc.PixelSettings = &PixelSettings{}
	Svc.KeyWords = &KeyWords{}

	Svc.cqrConfig = []cqr.CQRConfig{
		{
			Tables:  []string{"campaigns"},
			Data:    Svc.Campaigns,
			WebHook: Svc.conf.CampaignWebHook,
		},
		{
			Tables: []string{"service", "service_content"},
			Data:   Svc.Services,
		},
		{
			Tables: []string{"content"},
			Data:   Svc.Contents,
		},
		{
			Tables: []string{"content_sent"},
			Data:   Svc.SentContents,
		},
		{
			Tables: []string{"operator_ip", "operator"},
			Data:   Svc.IpRanges,
		},
		{
			Tables: []string{"operators", "countries"},
			Data:   Svc.Operators,
		},
		{
			Tables: []string{"operator_msisdn_prefix"},
			Data:   Svc.Prefixes,
		},
		{
			Tables: []string{"msisdn_blacklist"},
			Data:   Svc.BlackList,
		},
		{
			Tables: []string{"msisdn_postpaid"},
			Data:   Svc.PostPaid,
		},
		{
			Tables: []string{"pixel_setting"},
			Data:   Svc.PixelSettings,
		},
		{
			Tables: []string{"keyword"},
			Data:   Svc.KeyWords,
		},
	}
	if err := cqr.InitCQR(Svc.cqrConfig); err != nil {
		log.Fatal("cqr.InitCQR: " + err.Error())
	}
}

func AddTablesHandler(r *gin.Engine) {
	r.GET("tables", tablesHandler)
}
func tablesHandler(c *gin.Context) {
	var tableNames = make(map[string]string)
	for _, v := range Svc.cqrConfig {
		for _, v := range v.Tables {
			tableNames[v] = "http://localhost:50308/cqr?t=" + v
		}

	}
	log.WithFields(log.Fields{
		"tables": fmt.Sprintf("%#v", tableNames),
	}).Debug("api tables")

	c.IndentedJSON(200, tableNames)
}
func AddCQRHandlers(r *gin.Engine) {
	cqr.AddCQRHandler(reloadCQRFunc, r)
}
func reloadCQRFunc(c *gin.Context) {
	cqr.CQRReloadFunc(Svc.cqrConfig, c)(c)
}
func loadPrivateIpRanges(ipConf []IpRange) []IpRange {
	var ipRanges []IpRange
	for _, v := range ipConf {
		v.Start = net.ParseIP(v.IpFrom)
		v.End = net.ParseIP(v.IpTo)
		ipRanges = append(ipRanges, v)
	}
	log.WithField("privateNetworks", ipRanges).Info("private networks loaded")
	return ipRanges
}

var (
	loadCampaignError       prometheus.Gauge
	loadOperatorHeaderError prometheus.Gauge
)

func initMetrics() {
	loadCampaignError = m.PrometheusGauge("campaign", "load", "error", "load campaign error")
	loadOperatorHeaderError = m.PrometheusGauge("operator", "load_headers", "error", "operator load headers error")
}
