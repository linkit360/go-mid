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
