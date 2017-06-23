package handlers

import (
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-mid/service"
	"github.com/linkit360/go-utils/structs"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

type GetAllParams struct{}

type GetAllCampaignsResponse struct {
	Campaigns map[string]service.Campaign `json:"campaigns,omitempty"`
}
type GetAllServicesResponse struct {
	Services map[string]xmp_api_structs.Service `json:"services,omitempty"`
}
type GetAllPublishersResponse struct {
	Publishers map[string]service.Publisher `json:"publishers,omitempty"`
}
type GetAllDestinationsResponse struct {
	Destinations []service.Destination `json:"destinations,omitempty"`
}
type GetAllRedirectStatCountsResponse struct {
	StatCounts map[int64]*service.StatCount `json:"stats,omitempty"`
}
type GetByNameParams struct {
	Name string `json:"name,omitempty"`
}
type GetByHashParams struct {
	Hash string `json:"hash,omitempty"`
}
type GetByIdParams struct {
	Id int64 `json:"id,omitempty"`
}
type GetByUUIDParams struct {
	UUID string `json:"uuid,omitempty"`
}
type GetByCodeParams struct {
	Code string `json:"code,omitempty"`
}
type GetByPrefixParams struct {
	Prefix string `json:"prefix,omitempty"`
}
type GetByLinkParams struct {
	Link string `json:"link,omitempty"`
}
type GetByMsisdnParams struct {
	Msisdn string `json:"msisdn,omitempty"`
}
type BlackListedParams struct {
	Msisdns []string `json:"msisdns,omitempty"`
}
type GetByKeyParams struct {
	Key string `json:"key,omitempty"`
}
type GetByKeyWordParams struct {
	Key string `json:"keyword,omitempty"`
}
type GetByParams struct {
	Msisdn      string `json:"msisdn,omitempty"`
	ServiceCode string `json:"code_service,omitempty"`
	ContentCode string `json:"content_code,omitempty"`
}
type RejectedParams struct {
	Msisdn       string `json:"msisdn,omitempty"`
	CampaignCode string `json:"campaign_code,omitempty"`
	ServiceCode  string `json:"service_code,omitempty"`
}
type Response struct{}

type GetContentSentResponse struct {
	ContentdCodes map[string]struct{} `json:"content_codes,omitempty"`
}
type BoolResponse struct {
	Result bool `json:"result,omitempty"`
}

// Campaign
type Campaign struct{}

func (rpc *Campaign) ByHash(
	req GetByHashParams, res *service.Campaign) error {

	campaign, err := service.Svc.Campaigns.GetByHash(req.Hash)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = campaign
	success.Inc()
	return nil
}
func (rpc *Campaign) ByLink(
	req GetByLinkParams, res *service.Campaign) error {

	campaign, err := service.Svc.Campaigns.GetByLink(req.Link)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = campaign
	success.Inc()
	return nil
}

func (rpc *Campaign) ByUUID(
	req GetByUUIDParams, res *service.Campaign) error {

	campaign, err := service.Svc.Campaigns.GetByUUID(req.UUID)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = campaign
	success.Inc()
	return nil
}

func (rpc *Campaign) ByServiceCode(
	req GetByCodeParams, res *service.Campaign) error {

	campaigns, err := service.Svc.Campaigns.GetByServiceCode(req.Code)
	if err != nil || len(campaigns) == 0 {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = campaigns[0]
	success.Inc()
	return nil
}

func (rpc *Campaign) ByKeyWord(
	req GetByKeyWordParams, res *service.Campaign) error {

	campaignId, ok := service.Svc.KeyWords.ByKeyWord[strings.ToLower(req.Key)]
	if !ok {
		log.Errorf("campaign id not found, key: %s", req.Key)
		notFound.Inc()
		keyWordNotFound.Inc()
		errors.Inc()
		return nil
	}
	campaign, err := service.Svc.Campaigns.GetByUUID(campaignId)
	if err != nil {
		log.Errorf("campaign not found %#v", req.Key)
		errors.Inc()
		return nil
	}
	*res = campaign
	success.Inc()
	return nil
}

func (rpc *Campaign) All(
	req GetAllParams, res *GetAllCampaignsResponse) error {
	*res = GetAllCampaignsResponse{
		Campaigns: service.Svc.Campaigns.GetAll(),
	}

	success.Inc()
	return nil
}

// BlackList
type BlackList struct{}

func (rpc *BlackList) ByMsisdn(
	req GetByMsisdnParams, res *BoolResponse) error {

	blackListed := service.Svc.BlackList.IsBlacklisted(req.Msisdn)
	*res = BoolResponse{Result: blackListed}

	success.Inc()
	return nil
}

// PostPaid
type PostPaid struct{}

func (rpc *PostPaid) ByMsisdn(
	req GetByMsisdnParams, res *BoolResponse) error {

	_, ok := service.Svc.PostPaid.ByMsisdn[req.Msisdn]
	if ok {
		*res = BoolResponse{Result: true}
	} else {
		*res = BoolResponse{Result: false}
	}
	success.Inc()
	return nil
}
func (rpc *PostPaid) Push(
	req GetByMsisdnParams, res *BoolResponse) error {

	service.Svc.PostPaid.Push(req.Msisdn)
	*res = BoolResponse{Result: true}
	success.Inc()
	return nil
}
func (rpc *PostPaid) Remove(
	req GetByMsisdnParams, res *BoolResponse) error {

	service.Svc.PostPaid.Remove(req.Msisdn)
	*res = BoolResponse{Result: true}
	success.Inc()
	return nil
}

// Rejected
type RejectedByCampaign struct{}

func (rpc *RejectedByCampaign) Set(
	req RejectedParams, res *BoolResponse) error {

	service.SetMsisdnCampaignCache(req.CampaignCode, req.Msisdn)
	success.Inc()
	return nil
}

func (rpc *RejectedByCampaign) Get(
	req RejectedParams, res *string) error {

	campaignCode := service.GetMsisdnCampaignCache(req.CampaignCode, req.Msisdn)
	*res = campaignCode
	success.Inc()
	return nil
}

type RejectedByService struct{}

func (rpc *RejectedByService) Set(
	req RejectedParams, res *BoolResponse) error {

	service.SetMsisdnServiceCache(req.ServiceCode, req.Msisdn)
	success.Inc()
	return nil
}

func (rpc *RejectedByService) Is(
	req RejectedParams, res *bool) error {

	is := service.IsMsisdnRejectedByService(req.ServiceCode, req.Msisdn)
	*res = is
	success.Inc()
	return nil
}

// Service
type Service struct{}

func (rpc *Service) All(
	req GetAllParams, res *GetAllServicesResponse) error {
	*res = GetAllServicesResponse{
		Services: service.Svc.Services.GetAll(),
	}
	success.Inc()
	return nil
}

func (rpc *Service) ByCode(
	req GetByCodeParams, res *xmp_api_structs.Service) error {

	svc, err := service.Svc.Services.GetByCode(req.Code)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = svc
	success.Inc()
	return nil
}

// Pixel Setting
type PixelSetting struct{}

func (rpc *PixelSetting) ByCampaignCode(
	req GetByCodeParams, res *service.PixelSetting) error {

	svc, err := service.Svc.PixelSettings.GetByCampaignCode(req.Code)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = svc
	success.Inc()
	return nil
}
func (rpc *PixelSetting) ByKey(
	req GetByKeyParams, res *service.PixelSetting) error {

	ps, err := service.Svc.PixelSettings.GetByKey(req.Key)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = ps
	success.Inc()
	return nil
}
func (rpc *PixelSetting) ByKeyWithRatio(
	req GetByKeyParams, res *service.PixelSetting) error {

	ps, err := service.Svc.PixelSettings.ByKeyWithRatio(req.Key)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = ps
	success.Inc()
	return nil
}

// Content
type Content struct{}

func (rpc *Content) ById(
	req GetByUUIDParams, res *xmp_api_structs.Content) error {

	content, err := service.Svc.Contents.GetById(req.UUID)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}

	*res = content
	success.Inc()
	return nil
}

type Operator struct{}

func (rpc *Operator) ByCode(
	req GetByIdParams, res *xmp_api_structs.Operator) error {

	operator, err := service.Svc.Operators.GetByCode(req.Id)
	if err != nil {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = operator
	success.Inc()
	return nil
}
func (rpc *Operator) GetCountry(req GetAllParams, res *string) error {
	*res = service.GetCountry()
	success.Inc()
	return nil
}

// Content Sent
type ContentSent struct{}

func (rpc *ContentSent) Clear(
	req GetByParams, res *Response) error {
	service.Svc.SentContents.Clear(req.Msisdn, req.ServiceCode)
	success.Inc()
	return nil
}
func (rpc *ContentSent) Push(
	req GetByParams, res *Response) error {
	service.Svc.SentContents.Push(req.Msisdn, req.ServiceCode, req.ContentCode)
	return nil
}
func (rpc *ContentSent) Get(
	req GetByParams, res *GetContentSentResponse) error {

	contentIds := service.Svc.SentContents.Get(req.Msisdn, req.ServiceCode)
	*res = GetContentSentResponse{ContentdCodes: contentIds}
	success.Inc()
	return nil
}

type UniqueUrls struct{}

func (rpc *UniqueUrls) Get(req GetByKeyParams, res *structs.ContentSentProperties) error {
	properties, err := service.Svc.UniqueUrls.Get(req.Key)
	if err != nil {
		log.Errorf("unique url not found, key: %s", req.Key)
		notFound.Inc()
		urlCacheNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = properties
	success.Inc()
	return nil
}

func (rpc *UniqueUrls) Set(req structs.ContentSentProperties, res *Response) error {
	service.Svc.UniqueUrls.Set(req)
	success.Inc()
	return nil
}
func (rpc *UniqueUrls) Delete(req structs.ContentSentProperties, res *Response) error {
	service.Svc.UniqueUrls.Delete(req)
	success.Inc()
	return nil
}

type Publisher struct{}

func (rpc *Publisher) All(
	req GetAllParams, res *GetAllPublishersResponse) error {
	*res = GetAllPublishersResponse{
		Publishers: service.Svc.Publishers.All,
	}
	success.Inc()
	return nil
}

type Destinations struct{}

func (rpc *Destinations) All(
	req GetAllParams, res *GetAllDestinationsResponse) error {
	*res = GetAllDestinationsResponse{
		Destinations: service.Svc.Destinations.ByPrice,
	}
	success.Inc()
	return nil
}

type RedirectStatCounts struct{}

func (rpc *RedirectStatCounts) All(
	req GetAllParams, res *GetAllRedirectStatCountsResponse) error {
	*res = GetAllRedirectStatCountsResponse{
		StatCounts: service.Svc.RedirectStatCounts.ById,
	}
	success.Inc()
	return nil
}

func (rpc *RedirectStatCounts) Inc(req GetByIdParams, res *Response) error {
	err := service.Svc.RedirectStatCounts.IncHit(req.Id)
	success.Inc()
	return err
}
