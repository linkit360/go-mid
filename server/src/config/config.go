package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-mid/service"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/aws"
	"github.com/linkit360/go-utils/db"
	xmp_api "github.com/linkit360/xmp-api/src/client"
)

type ServerConfig struct {
	Host     string `default:"127.0.0.1" yaml:"host"`
	RPCPort  string `default:"50307" yaml:"rpc_port"`
	HttpPort string `default:"50308" yaml:"http_port"`
}

type AppConfig struct {
	AppName    string               `yaml:"app_name"`
	Server     ServerConfig         `yaml:"server"`
	AWS        aws.Config           `yaml:"aws"`
	XMPAPIConf xmp_api.ClientConfig `yaml:"xmp_api"`
	Service    service.Config       `yaml:"service"`
	DbConf     db.DataBaseConfig    `yaml:"db"`
	Consumer   amqp.ConsumerConfig  `yaml:"consumer"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/mid.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
			os.Exit(1)
		}
	}
	if appConfig.AppName == "" {
		log.Fatal("app name must be defiled as <host>-<name>")
	}
	if strings.Contains(appConfig.AppName, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}
	if appConfig.XMPAPIConf.InstanceId == "" {
		log.Fatal("instance id must be specified")
	}
	appConfig.Server.RPCPort = envString("PORT", appConfig.Server.RPCPort)
	appConfig.Server.HttpPort = envString("METRICS_PORT", appConfig.Server.HttpPort)

	fmt.Printf("env:" + os.Getenv("AWS_SDK_LOAD_CONFIG"))
	log.WithField("config", fmt.Sprintf("%#v", appConfig)).Info("Config loaded")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
