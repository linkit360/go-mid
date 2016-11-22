package src

// server has metrics, config app
// and handles rpc method to get smth by smth
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/inmem/server/src/config"
	"github.com/vostrok/inmem/server/src/handlers"
	"github.com/vostrok/inmem/service"
	"github.com/vostrok/utils/metrics"
)

func Run() {
	appConfig := config.LoadConfig()

	service.Init(
		appConfig.Name,
		appConfig.Service,
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
	metrics.AddHandler(r)

	r.Run(":" + appConfig.Server.HttpPort)
	log.WithField("port", appConfig.Server.HttpPort).Info("service port")
}

func runRPC(appConfig config.AppConfig) {

	l, err := net.Listen("tcp", ":"+appConfig.Server.RPCPort)
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
	server.RegisterName("Content", &handlers.Content{})
	server.RegisterName("Operator", &handlers.Operator{})
	server.RegisterName("IPInfo", &handlers.IPInfo{})
	server.RegisterName("BlackList", &handlers.BlackList{})
	server.RegisterName("PostPaid", &handlers.PostPaid{})

	for {
		if conn, err := l.Accept(); err == nil {
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		} else {
			log.WithField("error", err.Error()).Error("accept")
		}
	}
}
