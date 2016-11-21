package service

import (
	"database/sql"

	log "github.com/Sirupsen/logrus"

	"github.com/gin-gonic/gin"
	"github.com/vostrok/utils/cqr"
	"github.com/vostrok/utils/db"
	m "github.com/vostrok/utils/metrics"
)

var Svc MemService

type MemService struct {
	db           *sql.DB
	dbConf       db.DataBaseConfig
	UniqueDays   int
	cqrConfig    []cqr.CQRConfig
	Campaigns    *Campaigns
	Services     *Services
	Contents     *Contents
	SentContents *SentContents
	IpRanges     *IpRanges
	Operators    *Operators
	Prefixes     *Prefixes
}

func Init(
	appName string,
	uniqueDays int,
	dbConf db.DataBaseConfig,

) {
	log.SetLevel(log.DebugLevel)

	Svc.db = db.Init(dbConf)
	Svc.dbConf = dbConf
	Svc.UniqueDays = uniqueDays
	m.Init(appName)

	Svc.Campaigns = &Campaigns{}
	Svc.Services = &Services{}
	Svc.Contents = &Contents{}
	Svc.SentContents = &SentContents{}
	Svc.IpRanges = &IpRanges{}
	Svc.Operators = &Operators{}
	Svc.Prefixes = &Prefixes{}

	Svc.cqrConfig = []cqr.CQRConfig{
		{
			Tables: []string{"campaigns"},
			Data:   Svc.Campaigns,
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
			Tables: []string{"operator_ip", "operators"},
			Data:   Svc.IpRanges,
		},
		{
			Tables: []string{"operators"},
			Data:   Svc.Operators,
		},
		{
			Tables: []string{"operator_msisdn_prefix"},
			Data:   Svc.Prefixes,
		},
	}
	cqr.InitCQR(Svc.cqrConfig)
	initMetrics()

}
func AddCQRHandlers(r *gin.Engine) {
	cqr.AddCQRHandler(reloadCQRFunc, r)
}
func reloadCQRFunc(c *gin.Context) {
	cqr.CQRReloadFunc(Svc.cqrConfig, c)(c)
}
