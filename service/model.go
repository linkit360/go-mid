package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	cache "github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"

	acceptor "github.com/linkit360/go-acceptor-client"
	"github.com/linkit360/go-utils/cqr"
	"github.com/linkit360/go-utils/db"
	m "github.com/linkit360/go-utils/metrics"
)

var Svc MemService

var errNotFound = func() error {
	Svc.m.NotFound.Inc()
	return errors.New("Not found")
}

type MemService struct {
	db                 *sql.DB
	dbConf             db.DataBaseConfig
	conf               Config
	m                  *serviceMetrics
	cqrConfig          []cqr.CQRConfig
	privateIPRanges    []IpRange
	Campaigns          *Campaigns
	Services           Services
	Contents           Contents
	SentContents       *SentContents
	IpRanges           *IpRanges
	Operators          *Operators
	Prefixes           *Prefixes
	BlackList          BlackList
	PostPaid           *PostPaid
	PixelSettings      *PixelSettings
	Publishers         *Publishers
	KeyWords           *KeyWords
	RejectedByCampaign *cache.Cache
	RejectedByService  *cache.Cache
	UniqueUrls         *UniqueUrls
	Destinations       *Destinations
	RedirectStatCounts *RedirectStatCounts
}

type Config struct {
	ProviderName    string          `yaml:"provider_name"`
	OperatorCode    int64           `yaml:"operator_code" default:"41001"`
	UniqueDays      int             `yaml:"unique_days" default:"10"`
	StaticPath      string          `yaml:"static_path" default:""`
	BlackList       BlackListConfig `yaml:"blacklist"`
	Services        ServicesConfig  `yaml:"services"`
	Contents        ContentConfig   `yaml:"contents"`
	CampaignWebHook string          `yaml:"campaign_web_hook" default:"http://localhost:50300/updateTemplates"`
	PrivateIpRanges []IpRange       `yaml:"private_networks"`
	Enabled         EnabledConfig   `yaml:"enabled"`
}

type EnabledConfig struct {
	Campaigns          bool `yaml:"campaigns" default:"true"`
	Contents           bool `yaml:"contents" default:"true"`
	SentContents       bool `yaml:"sent_contents" default:"true"`
	IpRanges           bool `yaml:"ip_ranges" default:"true"`
	Operators          bool `yaml:"operators" default:"true"`
	Prefixes           bool `yaml:"prefixes" default:"true"`
	PostPaid           bool `yaml:"postpaid" default:"true"`
	PixelSettings      bool `yaml:"pixel_settings" default:"true"`
	Publishers         bool `yaml:"publishers" default:"true"`
	KeyWords           bool `yaml:"keywords" default:"true"`
	UniqueUrls         bool `yaml:"unique_urls" default:"true"`
	Destinations       bool `yaml:"destinations"`
	RedirectStatCounts bool `yaml:"redirect_stats_count"`
}

func Init(
	appName string,
	svcConf Config,
	dbConf db.DataBaseConfig,
	acceptorClientConf acceptor.ClientConfig,

) {

	if err := acceptor.Init(acceptorClientConf); err != nil {
		log.Error("cannot init acceptor client")
	}

	log.SetLevel(log.DebugLevel)
	Svc.m = initMetrics(appName)

	Svc.db = db.Init(dbConf)
	Svc.dbConf = dbConf
	Svc.conf = svcConf

	initPrevSubscriptionsCache()

	Svc.privateIPRanges = loadPrivateIpRanges(svcConf.PrivateIpRanges)

	Svc.Campaigns = &Campaigns{}
	Svc.Services = initServices(appName, svcConf.Services)
	Svc.Contents = initContents(appName, svcConf.Contents)
	Svc.SentContents = &SentContents{}
	Svc.IpRanges = &IpRanges{}
	Svc.Operators = &Operators{}
	Svc.Prefixes = &Prefixes{}
	Svc.BlackList = initBlackList(appName, svcConf.BlackList)
	Svc.PostPaid = &PostPaid{}
	Svc.PixelSettings = &PixelSettings{}
	Svc.Publishers = &Publishers{}
	Svc.KeyWords = &KeyWords{}
	Svc.UniqueUrls = &UniqueUrls{}
	Svc.Destinations = &Destinations{}
	Svc.RedirectStatCounts = &RedirectStatCounts{}

	Svc.cqrConfig = []cqr.CQRConfig{
		{
			Tables:  []string{"campaigns"},
			Data:    Svc.Campaigns,
			WebHook: Svc.conf.CampaignWebHook,
			Enabled: Svc.conf.Enabled.Campaigns,
		},
		{
			Tables:  []string{"service", "service_content"},
			Data:    Svc.Services,
			Enabled: Svc.conf.Services.Enabled,
		},
		{
			Tables:  []string{"content"},
			Data:    Svc.Contents,
			Enabled: Svc.conf.Enabled.Contents,
		},
		{
			Tables:  []string{"content_sent"},
			Data:    Svc.SentContents,
			Enabled: Svc.conf.Enabled.SentContents,
		},
		{
			Tables:  []string{"operator_ip", "operator"},
			Data:    Svc.IpRanges,
			Enabled: Svc.conf.Enabled.IpRanges,
		},
		{
			Tables:  []string{"operator", "countries"},
			Data:    Svc.Operators,
			Enabled: Svc.conf.Enabled.Operators,
		},
		{
			Tables:  []string{"operator_msisdn_prefix"},
			Data:    Svc.Prefixes,
			Enabled: Svc.conf.Enabled.Prefixes,
		},
		{
			Tables:  []string{"msisdn_blacklist"},
			Data:    Svc.BlackList,
			Enabled: Svc.conf.BlackList.Enabled,
		},
		{
			Tables:  []string{"msisdn_postpaid"},
			Data:    Svc.PostPaid,
			Enabled: Svc.conf.Enabled.PostPaid,
		},
		{
			Tables:  []string{"pixel_setting"},
			Data:    Svc.PixelSettings,
			Enabled: Svc.conf.Enabled.PixelSettings,
		},
		{
			Tables:  []string{"publishers"},
			Data:    Svc.Publishers,
			Enabled: Svc.conf.Enabled.Publishers,
		},
		{
			Tables:  []string{"keyword"},
			Data:    Svc.KeyWords,
			Enabled: Svc.conf.Enabled.KeyWords,
		},
		{
			Tables:  []string{"content_unique_urls"},
			Data:    Svc.UniqueUrls,
			Enabled: Svc.conf.Enabled.UniqueUrls,
		},
		{
			Tables:  []string{"partners", "destinations"},
			Data:    Svc.Destinations,
			Enabled: Svc.conf.Enabled.Destinations,
		},
		{
			Tables:  []string{"destinations", "destinations_hits"},
			Data:    Svc.RedirectStatCounts,
			Enabled: Svc.conf.Enabled.RedirectStatCounts,
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
	log.Info("private networks loaded")
	return ipRanges
}

type serviceMetrics struct {
	NotFound                m.Gauge
	LoadCampaignError       prometheus.Gauge
	LoadOperatorHeaderError prometheus.Gauge
	LoadPublisherRegexError prometheus.Gauge
}

func initMetrics(appName string) *serviceMetrics {
	sm := &serviceMetrics{
		NotFound:                m.NewGauge(appName, "its", "not_found", "inmemory cann't find something"),
		LoadCampaignError:       m.PrometheusGauge(appName, "campaign_load", "error", "load campaign error"),
		LoadOperatorHeaderError: m.PrometheusGauge(appName, "operator_load_headers", "error", "operator load headers error"),
		LoadPublisherRegexError: m.PrometheusGauge(appName, "publisher_load_regex", "error", "publisher load regex error"),
	}

	go func() {
		for range time.Tick(time.Minute) {
			sm.NotFound.Update()
		}
	}()

	return sm
}

func exec(context, query string, params_optional ...[]interface{}) {
	begin := time.Now().UTC()
	res, err := Svc.db.Exec(query)
	if err != nil {
		log.WithFields(log.Fields{
			"query":   query,
			"context": context,
			"error":   err.Error(),
		}).Error("cann't run query")

	} else {
		count, _ := res.RowsAffected()
		log.WithFields(log.Fields{
			"count": count,
			"took":  time.Since(begin),
		}).Info(context)
	}
}
