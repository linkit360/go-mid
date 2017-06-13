package src

import (
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"runtime"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-mid/server/src/config"
	"github.com/linkit360/go-mid/server/src/handlers"
	"github.com/linkit360/go-mid/service"
	m "github.com/linkit360/go-utils/metrics"
)

func Run() {
	appConfig := config.LoadConfig()

	handlers.InitMetrics(appConfig.AppName)

	service.Init(
		appConfig.AppName,
		appConfig.XMPAPIConf,
		appConfig.AWS,
		appConfig.Service,
		appConfig.Consumer,
		appConfig.DbConf,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	go runGin(appConfig)
	runRPC(appConfig)
}

func runGin(appConfig config.AppConfig) {
	r := gin.New()

	service.AddCQRHandlers(r)
	service.AddTablesHandler(r)
	service.AddAPIGetAgregateHandler(r)
	service.AddStatusHandler(r)
	m.AddHandler(r)

	r.Run(":" + appConfig.Server.HttpPort)
	log.WithField("port", appConfig.Server.HttpPort).Info("service port")
}

func runRPC(appConfig config.AppConfig) {

	l, err := net.Listen("tcp", "127.0.0.1:"+appConfig.Server.RPCPort)
	if err != nil {
		log.Fatal("netListen ", err.Error())
	} else {
		log.WithField("port", appConfig.Server.RPCPort).Info("rpc port")
	}

	server := rpc.NewServer()
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	server.RegisterName("Campaign", &handlers.Campaign{})
	server.RegisterName("Service", &handlers.Service{})
	server.RegisterName("SentContent", &handlers.ContentSent{})
	server.RegisterName("UniqueUrls", &handlers.UniqueUrls{})
	server.RegisterName("Content", &handlers.Content{})
	server.RegisterName("Operator", &handlers.Operator{})
	server.RegisterName("BlackList", &handlers.BlackList{})
	server.RegisterName("RejectedByCampaign", &handlers.RejectedByCampaign{})
	server.RegisterName("RejectedByService", &handlers.RejectedByService{})
	server.RegisterName("PostPaid", &handlers.PostPaid{})
	server.RegisterName("PixelSetting", &handlers.PixelSetting{})
	server.RegisterName("Publisher", &handlers.Publisher{})
	server.RegisterName("Destinations", &handlers.Destinations{})
	server.RegisterName("RedirectStatCounts", &handlers.RedirectStatCounts{})

	for {
		if conn, err := l.Accept(); err == nil {
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		} else {
			log.WithField("error", err.Error()).Error("accept")
		}
	}
}

func OnExit() {
	log.WithField("pid", os.Getpid()).Info("on exit")
	service.OnExit()
}
