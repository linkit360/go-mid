package rpcclient

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-mid/server/src/handlers"
	"github.com/linkit360/go-mid/service"
	m "github.com/linkit360/go-utils/metrics"
	"github.com/linkit360/go-utils/structs"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

var errNotFound = func(v interface{}) error {
	cli.m.NotFound.Inc()
	return fmt.Errorf("%v: not found", v)
}
var cli *Client

type Client struct {
	connection *rpc.Client
	conf       ClientConfig
	m          *Metrics
}
type ClientConfig struct {
	DSN     string `default:":50307" yaml:"dsn"`
	Timeout int    `default:"10" yaml:"timeout"`
}

type Metrics struct {
	RPCConnectError m.Gauge
	RPCSuccess      m.Gauge
	RPCDuration     prometheus.Summary
	NotFound        m.Gauge
}

func initMetrics() *Metrics {
	m := &Metrics{
		RPCConnectError: m.NewGauge("rpc", "mid", "errors", "RPC call errors"),
		RPCSuccess:      m.NewGauge("rpc", "mid", "success", "RPC call success"),
		RPCDuration:     m.NewSummary("rpc_mid_duration_seconds", "RPC call duration seconds"),
		NotFound:        m.NewGauge("rpc", "mid", "404_errors", "RPC 404 errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.RPCConnectError.Update()
			m.RPCSuccess.Update()
			m.NotFound.Update()
		}
	}()
	return m
}
func Init(clientConf ClientConfig) error {
	var err error
	cli = &Client{
		conf: clientConf,
		m:    initMetrics(),
	}
	if err = cli.dial(); err != nil {
		err = fmt.Errorf("cli.dial: %s", err.Error())
		log.WithField("error", err.Error()).Error("mid rpc client unavialable")
		return err
	}
	log.WithField("conf", fmt.Sprintf("%#v", clientConf)).Info("mid rpc client init done")

	return nil
}

func (c *Client) dial() error {
	conn, err := net.DialTimeout(
		"tcp",
		c.conf.DSN,
		time.Duration(c.conf.Timeout)*time.Second,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"dsn":   c.conf.DSN,
			"error": err.Error(),
		}).Error("dialing mid")
		return err
	}
	c.connection = jsonrpc.NewClient(conn)
	return nil
}

func call(funcName string, req interface{}, res interface{}) error {
	begin := time.Now()
	retryCount := 0
retry:
	if err := cli.connection.Call(funcName, req, &res); err != nil {
		cli.m.RPCConnectError.Inc()

		if err == rpc.ErrShutdown {

			if retryCount < 10 {
				retryCount = retryCount + 1
				cli.connection.Close()
				cli.dial()
				log.WithFields(log.Fields{
					"retry": retryCount,
					"error": err.Error(),
				}).Debug("retrying..")
				goto retry
			}

			log.WithFields(log.Fields{
				"func":  funcName,
				"error": err.Error(),
			}).Fatal("call")
		}

		log.WithFields(log.Fields{
			"func":  funcName,
			"error": err.Error(),
			"type":  fmt.Sprintf("%T", err),
		}).Error("call")
		return err
	}
	log.WithFields(log.Fields{
		"req":  req,
		"func": funcName,
		"took": time.Since(begin),
	}).Debug("rpccall")

	cli.m.RPCSuccess.Inc()
	cli.m.RPCDuration.Observe(time.Since(begin).Seconds())
	return nil
}
func GetCampaignByHash(hash string) (service.Campaign, error) {
	var campaign service.Campaign
	err := call(
		"Campaign.ByHash",
		handlers.GetByHashParams{Hash: hash},
		&campaign,
	)
	if campaign.Id == "" {
		return campaign, errNotFound(hash)
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
	if campaign.Id == "" {
		return campaign, errNotFound(link)
	}
	return campaign, err
}
func GetCampaignByKeyWord(keyWord string) (service.Campaign, error) {
	var campaign service.Campaign
	err := call(
		"Campaign.ByKeyWord",
		handlers.GetByKeyWordParams{Key: keyWord},
		&campaign,
	)
	if campaign.Id == "" {
		return campaign, errNotFound(keyWord)
	}
	return campaign, err
}
func GetCampaignByUUID(uuid string) (service.Campaign, error) {
	var campaign service.Campaign
	err := call(
		"Campaign.ByUUID",
		handlers.GetByUUIDParams{UUID: uuid},
		&campaign,
	)
	if campaign.Id == "" {
		return campaign, errNotFound(uuid)
	}
	return campaign, err
}
func GetCampaignByServiceCode(serviceCode string) (service.Campaign, error) {
	var campaign service.Campaign
	err := call(
		"Campaign.ByServiceCode",
		handlers.GetByCodeParams{Code: serviceCode},
		&campaign,
	)
	if campaign.Id == "" {
		return campaign, errNotFound(serviceCode)
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
		return res.Campaigns, errNotFound("")
	}
	return res.Campaigns, err
}

func GetAllServices() (map[string]xmp_api_structs.Service, error) {
	var res handlers.GetAllServicesResponse
	err := call(
		"Service.All",
		handlers.GetAllParams{},
		&res,
	)

	if len(res.Services) == 0 {
		return res.Services, errNotFound("")
	}
	return res.Services, err
}

func GetOperatorByCode(code int64) (xmp_api_structs.Operator, error) {
	var operator xmp_api_structs.Operator
	err := call(
		"Operator.ByCode",
		handlers.GetByIdParams{Id: code},
		&operator,
	)
	if operator.Code == 0 {
		return operator, errNotFound(code)
	}

	return operator, err
}
func GetCountryName() string {
	var country string
	call(
		"Operator.GetCountry",
		handlers.GetAllParams{},
		&country,
	)
	return country
}
func GetServiceByCode(serviceCode string) (xmp_api_structs.Service, error) {
	var svc xmp_api_structs.Service
	err := call(
		"Service.ByCode",
		handlers.GetByCodeParams{Code: serviceCode},
		&svc,
	)
	if svc.Id == "" {
		return svc, errNotFound(serviceCode)
	}
	return svc, err
}

func GetContentById(uuid string) (xmp_api_structs.Content, error) {
	var content xmp_api_structs.Content
	err := call(
		"Content.ById",
		handlers.GetByUUIDParams{UUID: uuid},
		&content,
	)

	if content.Id == "" {
		return content, errNotFound(uuid)
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
		return pixelSetting, errNotFound(key)
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
		return pixelSetting, errNotFound(key)
	}
	return pixelSetting, err
}

func SentContentClear(msisdn, serviceCode string) error {
	var res handlers.Response
	err := call(
		"SentContent.Clear",
		handlers.GetByParams{Msisdn: msisdn, ServiceCode: serviceCode},
		&res,
	)
	return err
}

func SentContentPush(msisdn, serviceCode, contentCode string) error {
	var res handlers.Response
	err := call(
		"SentContent.Push",
		handlers.GetByParams{Msisdn: msisdn, ServiceCode: serviceCode, ContentCode: contentCode},
		&res,
	)
	return err
}

func SentContentGet(msisdn, serviceCode string) (map[string]struct{}, error) {
	var res handlers.GetContentSentResponse
	err := call(
		"SentContent.Get",
		handlers.GetByParams{Msisdn: msisdn, ServiceCode: serviceCode},
		&res,
	)
	return res.ContentdCodes, err
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

// Rejected, return campaign code
func GetMsisdnCampaignCache(campaignCode, msisdn string) (string, error) {
	var res string
	err := call(
		"RejectedByCampaign.Get",
		handlers.RejectedParams{Msisdn: msisdn, CampaignCode: campaignCode},
		&res,
	)
	return res, err
}
func SetMsisdnCampaignCache(campaignCode, msisdn string) error {
	var res handlers.BoolResponse
	err := call(
		"RejectedByCampaign.Set",
		handlers.RejectedParams{Msisdn: msisdn, CampaignCode: campaignCode},
		&res,
	)
	return err
}

func SetMsisdnServiceCache(serviceCode, msisdn string) error {
	var res handlers.BoolResponse
	err := call(
		"RejectedByService.Set",
		handlers.RejectedParams{Msisdn: msisdn, ServiceCode: serviceCode},
		&res,
	)
	return err
}

func IsMsisdnRejectedByService(serviceCode, msisdn string) (bool, error) {
	var res bool
	err := call(
		"RejectedByService.Is",
		handlers.RejectedParams{Msisdn: msisdn, ServiceCode: serviceCode},
		&res,
	)
	return res, err
}

func SetUniqueUrlCache(req structs.ContentSentProperties) error {
	var res handlers.Response
	err := call(
		"UniqueUrls.Set",
		req,
		&res,
	)
	return err
}
func GetUniqueUrlCache(uniqueUrl string) (structs.ContentSentProperties, error) {
	var res structs.ContentSentProperties
	err := call(
		"UniqueUrls.Get",
		handlers.GetByKeyParams{Key: uniqueUrl},
		&res,
	)
	return res, err
}
func DeleteUniqueUrlCache(req structs.ContentSentProperties) error {
	var res handlers.Response
	err := call(
		"UniqueUrls.Delete",
		req,
		&res,
	)
	return err
}

func GetAllPublishers() (map[string]service.Publisher, error) {
	var res handlers.GetAllPublishersResponse
	err := call(
		"Publisher.All",
		handlers.GetAllParams{},
		&res,
	)

	if len(res.Publishers) == 0 {
		return res.Publishers, errNotFound("")
	}
	return res.Publishers, err
}

func GetAllDestinations() ([]service.Destination, error) {
	var res handlers.GetAllDestinationsResponse
	err := call(
		"Destinations.All",
		handlers.GetAllParams{},
		&res,
	)

	if len(res.Destinations) == 0 {
		return res.Destinations, errNotFound("")
	}
	return res.Destinations, err
}

func GetAllRedirectStatCounts() (map[int64]*service.StatCount, error) {
	var res handlers.GetAllRedirectStatCountsResponse
	err := call(
		"RedirectStatCounts.All",
		handlers.GetAllParams{},
		&res,
	)
	return res.StatCounts, err
}
func IncRedirectStatCount(destinationId int64) error {
	var res handlers.Response
	err := call(
		"RedirectStatCounts.Inc",
		handlers.GetByIdParams{Id: destinationId},
		&res,
	)
	return err
}
