package rpcclient

// rpc client for "github.com/vostrok/inmem/server"
// supports reconnects when disconnected
import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/felixge/tcpkeepalive"

	"github.com/vostrok/inmem/server/src/handlers"
	"github.com/vostrok/inmem/service"
	m "github.com/vostrok/utils/metrics"
)

var errNotFound = errors.New("Not found")
var cli *Client

type Client struct {
	connection *rpc.Client
	conf       RPCClientConfig
	m          *Metrics
}
type RPCClientConfig struct {
	DSN             string `default:"127.0.0.1:50307" yaml:"dsn"`
	Timeout         int    `default:"10" yaml:"timeout"`
	RetryCallsPause int    `default:"10" yaml:"retry_calls_sleep"`
	RetryCallsLimit int    `default:"10" yaml:"retry_calls_limit"`
}
type Metrics struct {
	RPCConnectError m.Gauge
	RPCSuccess      m.Gauge
}

func initMetrics() *Metrics {
	m := &Metrics{
		RPCConnectError: m.NewGauge("rpc", "inmem", "errors", "RPC call errors"),
		RPCSuccess:      m.NewGauge("rpc", "inmem", "success", "RPC call success"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.RPCConnectError.Update()
			m.RPCSuccess.Update()
		}
	}()
	return m
}
func Init(clientConf RPCClientConfig) error {
	var err error
	cli = &Client{
		conf: clientConf,
		m:    initMetrics(),
	}
	if err = cli.dial(); err != nil {
		err = fmt.Errorf("cli.dial: %s", err.Error())
		log.WithField("error", err.Error()).Error("inmem rpc client unavialable")
		return err
	}
	log.WithField("conf", fmt.Sprintf("%#v", clientConf)).Info("inmem rpc client init done")

	return nil
}

func (c *Client) dial() error {
	if c.connection != nil {
		log.WithFields(log.Fields{}).Debug("closing connection...")
		if err := c.connection.Close(); err != nil {
			log.WithFields(log.Fields{
				"dsn":   c.conf.DSN,
				"error": err.Error(),
			}).Error("closing conn to inmem")
		}
	}

	conn, err := net.DialTimeout(
		"tcp",
		c.conf.DSN,
		time.Duration(c.conf.Timeout)*time.Second,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"dsn":   c.conf.DSN,
			"error": err.Error(),
		}).Error("dialing inmem")
		return err
	}
	kaConn, _ := tcpkeepalive.EnableKeepAlive(conn)
	kaConn.SetKeepAliveIdle(30 * time.Second)
	kaConn.SetKeepAliveCount(4)
	kaConn.SetKeepAliveInterval(5 * time.Second)
	c.connection = jsonrpc.NewClient(kaConn)
	log.WithFields(log.Fields{
		"dsn": c.conf.DSN,
	}).Debug("dialing inmem")
	return nil
}

func call(funcName string, req interface{}, res interface{}) error {
	if cli.connection == nil {
		cli.dial()
	}
	resendCount := 0
call:
	if err := cli.connection.Call(funcName, req, &res); err != nil {
		cli.m.RPCConnectError.Inc()
		if resendCount < cli.conf.RetryCallsLimit {

			log.WithFields(log.Fields{
				"sleep":       cli.conf.RetryCallsPause,
				"resendCount": resendCount,
				"func":        funcName,
				"error":       err.Error(),
			}).Error("call retry")
			resendCount = resendCount + 1
			time.Sleep(time.Duration(cli.conf.RetryCallsPause) * time.Second)

			cli.dial()
			goto call
		}
		err = fmt.Errorf(funcName+": %s", err.Error())
		log.WithFields(log.Fields{
			"call":        funcName,
			"resendCount": resendCount,
			"error":       err.Error(),
		}).Error("failed redial")
		return err
	}
	cli.m.RPCSuccess.Inc()
	return nil
}
func GetOperatorByCode(code int64) (service.Operator, error) {
	var operator service.Operator
	err := call(
		"Operator.ByCode",
		handlers.GetByCodeParams{Code: code},
		&operator,
	)
	if operator == (service.Operator{}) {
		return operator, errNotFound
	}

	return operator, err
}

func GetOperatorByName(name string) (service.Operator, error) {
	var operator service.Operator
	err := call(
		"Operator.ByName",
		handlers.GetByNameParams{Name: name},
		&operator,
	)
	if operator == (service.Operator{}) {
		return operator, errNotFound
	}
	return operator, err
}
func GetIPInfoByMsisdn(msisdn string) (service.IPInfo, error) {
	var ipInfo service.IPInfo
	err := call(
		"IPInfo.ByMsisdn",
		handlers.GetByMsisdnParams{Msisdn: msisdn},
		&ipInfo,
	)
	return ipInfo, err
}
func GetOperatorByPrefix(prefix string) (service.Operator, error) {
	var operator service.Operator
	err := call(
		"Prefix.GetOperator",
		handlers.GetByPrefixParams{Prefix: prefix},
		&operator,
	)
	if operator == (service.Operator{}) {
		return operator, errNotFound
	}
	return operator, err
}
func GetIPInfoByIps(ips []net.IP) ([]service.IPInfo, error) {
	var res handlers.GetByIPsResponse
	err := call(
		"IPInfo.ByIP",
		handlers.GetByIPsParams{IPs: ips},
		&res,
	)
	if len(res.IPInfos) == 0 {
		return res.IPInfos, errNotFound
	}
	return res.IPInfos, err
}
func GetCampaignByHash(hash string) (service.Campaign, error) {
	var campaign service.Campaign
	err := call(
		"Campaign.ByHash",
		handlers.GetByHashParams{Hash: hash},
		&campaign,
	)
	if campaign.Id == 0 {
		return campaign, errNotFound
	}
	return campaign, err
}
func GetCampaignByLink(link string) (service.Campaign, error) {
	var campaign service.Campaign
	err := call(
		"Campaign.ByLink",
		handlers.GetByLinkParams{Link: link},
		&campaign,
	)
	if campaign.Id == 0 {
		return campaign, errNotFound
	}
	return campaign, err
}
func GetAllCampaigns() (map[string]service.Campaign, error) {
	var res handlers.GetAllCampaignsResponse
	err := call(
		"Campaign.All",
		handlers.GetAllParams{},
		&res,
	)

	if len(res.Campaigns) == 0 {
		return res.Campaigns, errNotFound
	}
	return res.Campaigns, err
}

func GetServiceById(serviceId int64) (service.Service, error) {
	var svc service.Service
	err := call(
		"Service.ById",
		handlers.GetByIdParams{Id: serviceId},
		&svc,
	)
	if svc.Id == 0 {
		return svc, errNotFound
	}
	return svc, err
}

func GetContentById(contentId int64) (service.Content, error) {
	var content service.Content
	err := call(
		"Content.ById",
		handlers.GetByIdParams{Id: contentId},
		&content,
	)
	if content.Id == 0 {
		return content, errNotFound
	}
	return content, err
}

func GetPixelSettingByKey(key string) (service.PixelSetting, error) {
	var pixelSetting service.PixelSetting
	err := call(
		"PixelSetting.ByKey",
		handlers.GetByKeyParams{Key: key},
		&pixelSetting,
	)
	if pixelSetting == (service.PixelSetting{}) {
		return pixelSetting, errNotFound
	}
	return pixelSetting, err
}
func GetPixelSettingByKeyWithRatio(key string) (service.PixelSetting, error) {
	var pixelSetting service.PixelSetting
	err := call(
		"PixelSetting.ByKeyWithRatio",
		handlers.GetByKeyParams{Key: key},
		&pixelSetting,
	)
	if pixelSetting == (service.PixelSetting{}) {
		return pixelSetting, errNotFound
	}
	return pixelSetting, err
}

func SentContentClear(msisdn string, serviceId int64) error {
	var res handlers.Response
	err := call(
		"SentContent.Clear",
		handlers.GetByParams{Msisdn: msisdn, ServiceId: serviceId},
		&res,
	)
	return err
}

func SentContentPush(msisdn string, serviceId int64, contentId int64) error {
	var res handlers.Response
	err := call(
		"SentContent.Push",
		handlers.GetByParams{Msisdn: msisdn, ServiceId: serviceId, ContentId: contentId},
		&res,
	)
	return err
}

func SentContentGet(msisdn string, serviceId int64) (map[int64]struct{}, error) {
	var res handlers.GetContentSentResponse
	err := call(
		"SentContent.Get",
		handlers.GetByParams{Msisdn: msisdn, ServiceId: serviceId},
		&res,
	)
	return res.ContentdIds, err
}

func IsBlackListed(msisdn string) (bool, error) {
	var res handlers.BoolResponse
	err := call(
		"BlackList.ByMsisdn",
		handlers.GetByMsisdnParams{Msisdn: msisdn},
		&res,
	)
	return res.Result, err
}
func IsPostPaid(msisdn string) (bool, error) {
	var res handlers.BoolResponse
	err := call(
		"PostPaid.ByMsisdn",
		handlers.GetByMsisdnParams{Msisdn: msisdn},
		&res,
	)
	return res.Result, err
}

func PostPaidPush(msisdn string) error {
	var res handlers.Response
	err := call(
		"PostPaid.Push",
		handlers.GetByMsisdnParams{Msisdn: msisdn},
		&res,
	)
	return err
}

// for tests only!
// do not removes from database!
func PostPaidRemove(msisdn string) error {
	var res handlers.Response
	err := call(
		"PostPaid.Remove",
		handlers.GetByMsisdnParams{Msisdn: msisdn},
		&res,
	)
	return err
}
