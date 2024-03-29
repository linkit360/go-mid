package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	cache "github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/aws"
	qconf "github.com/linkit360/go-utils/config"
	"github.com/linkit360/go-utils/cqr"
	"github.com/linkit360/go-utils/db"
	m "github.com/linkit360/go-utils/metrics"
	xmp_api "github.com/linkit360/xmp-api/src/client"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

var Svc MemService

var errNotFound = func() error {
	Svc.m.NotFound.Inc()
	return errors.New("Not found")
}

type MemService struct {
	db                 *sql.DB
	cqrConfig          []cqr.CQRConfig
	m                  *serviceMetrics
	dbConf             db.DataBaseConfig
	downloader         aws.S3
	conf               Config
	xmpAPIConf         xmp_api.ClientConfig
	reporter           Collector
	Campaigns          Campaigns
	Services           Services
	Contents           Contents
	SentContents       *SentContents
	Operators          Operators
	BlackList          BlackList
	PostPaid           *PostPaid
	PixelSettings      PixelSettings
	Publishers         *Publishers
	KeyWords           *KeyWords
	RejectedByCampaign *cache.Cache
	RejectedByService  *cache.Cache
	UniqueUrls         *UniqueUrls
	Destinations       *Destinations
	RedirectStatCounts *RedirectStatCounts
}

type Config struct {
	CountryName   string              `yaml:"country_name"` // get them from control panel, otherwise from config
	StateFilePath string              `yaml:"state_file_path"`
	UniqueDays    int                 `yaml:"unique_days" default:"10"`
	StaticPath    string              `yaml:"static_path" default:""`
	Region        string              `yaml:"region" default:"ap-southeast-1"`
	Queue         QueuesConfig        `yaml:"queue"`
	Services      ServicesConfig      `yaml:"service"`
	Campaigns     CampaignsConfig     `yaml:"campaign"`
	Contents      ContentConfig       `yaml:"content"`
	BlackList     BlackListConfig     `yaml:"blacklist"`
	Pixel         PixelSettingsConfig `yaml:"pixel"`
	Operator      OperatorsConfig     `yaml:"operator"`
	Enabled       EnabledConfig       `yaml:"enabled"`
}

type QueuesConfig struct {
	ReporterHit         qconf.ConsumeQueueConfig `yaml:"reporter_hit"`
	ReporterTransaction qconf.ConsumeQueueConfig `yaml:"reporter_transaction"`
	ReporterPixel       qconf.ConsumeQueueConfig `yaml:"reporter_pixel"`
	ReporterOutflow     qconf.ConsumeQueueConfig `yaml:"reporter_outflow"`
}

type EnabledConfig struct {
	Services           bool `yaml:"services"`
	Campaigns          bool `yaml:"campaigns"`
	Contents           bool `yaml:"contents"`
	BlackList          bool `yaml:"blacklist"`
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
	Reporter           bool `yaml:"reporter"`
}

type Consumers struct {
	Hit         *amqp.Consumer `yaml:"hit"`
	Transaction *amqp.Consumer `yaml:"transaction"`
	Pixel       *amqp.Consumer `yaml:"pixel"`
	Outflow     *amqp.Consumer `yaml:"outflow"`
}

func Init(
	appName string,
	xmpAPIConf xmp_api.ClientConfig,
	awsConfig aws.Config,
	svcConf Config,
	consumerConf amqp.ConsumerConfig,
	dbConf db.DataBaseConfig,
) {
	if err := xmp_api.Init(xmpAPIConf); err != nil {
		log.Error("cannot init acceptor client")
	}

	log.SetLevel(log.DebugLevel)
	Svc.m = initMetrics(appName)

	Svc.db = db.Init(dbConf)
	Svc.dbConf = dbConf
	Svc.conf = svcConf
	Svc.xmpAPIConf = xmpAPIConf
	Svc.downloader = aws.New(awsConfig)

	initPrevSubscriptionsCache()

	Svc.reporter = initReporter(appName, svcConf.StateFilePath, svcConf.Queue, consumerConf)

	Svc.Campaigns = initCampaigns(appName, svcConf.Campaigns)
	Svc.Services = initServices(appName, svcConf.Services)
	Svc.Contents = initContents(appName, svcConf.Contents)
	Svc.PixelSettings = initPixelSettings(appName, svcConf.Pixel)
	Svc.SentContents = &SentContents{}
	Svc.Operators = initOperators(appName, svcConf.Operator)
	Svc.BlackList = initBlackList(appName, svcConf.BlackList)
	Svc.PostPaid = &PostPaid{}
	Svc.Publishers = &Publishers{}
	Svc.KeyWords = &KeyWords{}
	Svc.UniqueUrls = &UniqueUrls{}
	Svc.Destinations = &Destinations{}
	Svc.RedirectStatCounts = &RedirectStatCounts{}

	Svc.cqrConfig = []cqr.CQRConfig{
		{
			Tables:  []string{"operator"},
			Data:    Svc.Operators,
			Enabled: Svc.conf.Enabled.Operators,
		},
		{
			Tables: []string{"service", "service_content"},
			Data:   Svc.Services,
			//WebHook: Svc.conf.Services.WebHook,
			Enabled: Svc.conf.Enabled.Services, // always enabled
		},
		{
			Tables:  []string{"campaigns"},
			Data:    Svc.Campaigns,
			WebHook: Svc.conf.Campaigns.WebHook,
			Enabled: Svc.conf.Enabled.Campaigns, // always enabled
		},
		{
			Tables:  []string{"content"},
			Data:    Svc.Contents,
			Enabled: Svc.conf.Enabled.Contents,
		},
		{
			Tables:  []string{"pixel_setting"},
			Data:    Svc.PixelSettings,
			Enabled: Svc.conf.Enabled.PixelSettings,
		},
		{
			Tables:  []string{"msisdn_blacklist"},
			Data:    Svc.BlackList,
			Enabled: Svc.conf.Enabled.BlackList,
		},
		{
			Tables:  []string{"msisdn_postpaid"},
			Data:    Svc.PostPaid,
			Enabled: Svc.conf.Enabled.PostPaid,
		},
		{
			Tables:  []string{"publishers"},
			Data:    Svc.Publishers,
			Enabled: Svc.conf.Enabled.Publishers,
		},
		{
			Tables:  []string{"content_sent"},
			Data:    Svc.SentContents,
			Enabled: Svc.conf.Enabled.SentContents,
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
		log.Info("cqr.InitCQR: " + err.Error())
	}

	if xmpAPIConf.Enabled {
		var xmpConfig xmp_api_structs.HandShake
		log.Debug("xmp_api.Call..")

		if err := xmp_api.Call("initialization", &xmpConfig); err != nil {
			log.Fatal("xmp_api.Call: " + err.Error())
		}

		f := log.Fields{
			"blacklist": xmpConfig.BlackList,
			"services":  fmt.Sprintf("%#v", xmpConfig.Services),
			"campaigns": fmt.Sprintf("%#v", xmpConfig.Campaigns),
			"operators": fmt.Sprintf("%#v", xmpConfig.Operators),
			//"pixels":    len(xmpConfig.Pixels),
			"ok": xmpConfig.Ok,
		}
		if xmpConfig.Error != "" {
			f["error"] = xmpConfig.Error
			log.WithFields(f).Error("xmp_api.Call ERROR")
		} else {
			log.WithFields(f).Info("xmp_api.Call OK")
		}

		if svcConf.BlackList.FromControlPanel && xmpConfig.BlackList != "" {
			if err := Svc.BlackList.LoadFromAws(svcConf.BlackList.BlackListBucket, xmpConfig.BlackList); err != nil {
				log.WithFields(log.Fields{
					"key":    xmpConfig.BlackList,
					"bucket": svcConf.BlackList.BlackListBucket,
					"error":  err.Error(),
				}).Error("load blacklist")
				if err = Svc.BlackList.Reload(); err != nil {
					log.WithFields(log.Fields{
						"error": err.Error(),
					}).Error("load blacklist from db failed")
				} else {
					log.WithFields(log.Fields{
						"len": Svc.BlackList.Len(),
					}).Debug("load blacklist from db")
				}
			}
		}

		if svcConf.Services.FromControlPanel {
			Svc.Services.Apply(xmpConfig.Services)
			Svc.Services.ShowLoaded()
		}
		if svcConf.Campaigns.FromControlPanel {
			Svc.Campaigns.Apply(xmpConfig.Campaigns)
			Svc.Campaigns.ShowLoaded()
		}
		if svcConf.Operator.FromControlPanel {
			Svc.Operators.Apply(xmpConfig.Operators)
			Svc.Operators.ShowLoaded()
		}
		if svcConf.Pixel.FromControlPanel {
			//Svc.PixelSettings.Apply(xmpConfig.Pixels)
		}
		Svc.conf.CountryName = strings.ToLower(xmpConfig.Country.Name)
	}
}

func OnExit() {
	Svc.reporter.SaveState()
}

func AddTablesHandler(r *gin.Engine) {
	r.GET("tables", tablesHandler)
}

func AddAPIGetAgregateHandler(e *gin.Engine) {
	e.Group("api").GET("/aggregate/get", getAggregateHandler)
}
func AddStatusHandler(e *gin.Engine) {
	e.Group("status").GET("/get", getStatus)
}

func getStatus(c *gin.Context) {
	opt, ok := c.GetQuery("t")

	if !ok {
		log.WithFields(log.Fields{
			"blacklist": Svc.BlackList.Len(),
			"services":  Svc.Services.GetJson(),
			"content":   Svc.Contents.GetJson(),
			"campaigns": Svc.Campaigns.GetJson(),
			"operators": Svc.Operators.GetJson(),
			"pixels":    Svc.PixelSettings.GetJson(),
		}).Info("status")

		c.JSON(200, gin.H{
			"blacklist": Svc.BlackList.Len(),
			"services":  Svc.Services.GetJson(),
			"content":   Svc.Contents.GetJson(),
			"campaigns": Svc.Campaigns.GetJson(),
			"operators": Svc.Operators.GetJson(),
			"pixels":    Svc.PixelSettings.GetJson(),
		})
		return
	}

	switch opt {
	case "blacklist":
		log.WithFields(log.Fields{
			"blacklist": Svc.BlackList.Len(),
		}).Info("status")

		c.JSON(200, gin.H{
			"blacklist": Svc.BlackList.Len(),
		})
		return
	case "services":
		Svc.Services.ShowLoaded()

		c.JSON(200, gin.H{
			"services": Svc.Services.GetJson(),
		})
		return
	case "content":
		Svc.Contents.ShowLoaded()

		c.JSON(200, gin.H{
			"content": Svc.Contents.GetJson(),
		})
		return
	case "campaigns":
		Svc.Campaigns.ShowLoaded()

		c.JSON(200, gin.H{
			"campaigns": Svc.Campaigns.GetJson(),
		})
		return
	case "operators":
		Svc.Operators.ShowLoaded()

		c.JSON(200, gin.H{
			"operators": Svc.Operators.GetJson(),
		})
		return
	case "pixels":
		log.WithFields(log.Fields{
			"pixels": Svc.PixelSettings.GetJson(),
		}).Info("status")

		c.JSON(200, gin.H{
			"pixels": Svc.PixelSettings.GetJson(),
		})
		return
	}
}

func getAggregateHandler(c *gin.Context) {

	fromTimeString, ok := c.GetQuery("from")
	if !ok {
		c.JSON(500, gin.H{"error": "From bound required (time from to)"})
		return
	}
	from, err := time.Parse("2006-01-02", fromTimeString)
	if err != nil {
		c.JSON(500, gin.H{"error": "Error parse time: " + err.Error()})
		return
	}
	toTimeString, ok := c.GetQuery("to")
	if !ok {
		c.JSON(500, gin.H{"error": "To bound required (time from to)"})
		return
	}
	to, err := time.Parse("2006-01-02", toTimeString)
	if err != nil {
		c.JSON(500, gin.H{"error": "Error parse time: " + err.Error()})
		return
	}

	res, err := Svc.reporter.GetAggregate(from, to)
	if err != nil {
		c.JSON(500, gin.H{"error": "Error while get aggregate: " + err.Error()})
		return
	}

	c.JSON(200, res)
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

type serviceMetrics struct {
	NotFound                m.Gauge
	LoadPublisherRegexError prometheus.Gauge
}

func initMetrics(appName string) *serviceMetrics {
	sm := &serviceMetrics{
		NotFound:                m.NewGauge(appName, "its", "not_found", "mid cann't find something"),
		LoadPublisherRegexError: m.PrometheusGauge(appName, "publisher_load_regex", "error", "publisher load regex error"),
	}

	go func() {
		for range time.Tick(time.Minute) {
			sm.NotFound.Update()
		}
	}()

	return sm
}

func GetCountry() string {
	return Svc.conf.CountryName
}
