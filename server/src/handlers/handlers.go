package handlers

import (
	"net"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/vostrok/inmem/service"
)

type GetAllParams struct {
}
type GetAllCampaignsResponse struct {
	Campaigns map[string]service.Campaign `json:"campaigns,omitempty"`
}
type GetAllServicesResponse struct {
	Services map[int64]service.Service `json:"services,omitempty"`
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
type GetByCodeParams struct {
	Code int64 `json:"code,omitempty"`
}
type GetByPrefixParams struct {
	Prefix string `json:"prefix,omitempty"`
}
type GetByLinkParams struct {
	Link string `json:"link,omitempty"`
}
type GetByIdParams struct {
	Id int64 `json:"id,omitempty"`
}
type GetByMsisdnParams struct {
	Msisdn string `json:"msisdn,omitempty"`
}
type BlackListedParams struct {
	Msisdns []string `json:"msisdns,omitempty"`
}
type GetByIPsParams struct {
	IPs []net.IP `json:"ips,omitempty"`
}
type GetByIPsResponse struct {
	IPInfos []service.IPInfo `json:"ip_infos,omitempty"`
}
type GetByKeyParams struct {
	Key string `json:"key,omitempty"`
}
type GetByKeyWordParams struct {
	Key string `json:"keyword,omitempty"`
}
type GetByParams struct {
	Msisdn    string `json:"msisdn,omitempty"`
	ServiceId int64  `json:"service_id,omitempty"`
	ContentId int64  `json:"content_id,omitempty"`
}
type RejectedParams struct {
	Msisdn     string `json:"msisdn,omitempty"`
	CampaignId int64  `json:"campaign_id,omitempty"`
	ServiceId  int64  `json:"service_id,omitempty"`
}
type Response struct{}

type GetContentSentResponse struct {
	ContentdIds map[int64]struct{} `json:"content_ids,omitempty"`
}
type BoolResponse struct {
	Result bool `json:"result,omitempty"`
}

// Campaign
type Campaign struct{}

func (rpc *Campaign) ByHash(
	req GetByHashParams, res *service.Campaign) error {

	campaign, ok := service.Svc.Campaigns.ByHash[req.Hash]
	if !ok {
		notFound.Inc()
		campaignNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = campaign
	success.Inc()
	return nil
}
func (rpc *Campaign) ByLink(
	req GetByLinkParams, res *service.Campaign) error {

	campaign, ok := service.Svc.Campaigns.ByLink[req.Link]
	if !ok {
		notFound.Inc()
		campaignNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = campaign
	success.Inc()
	return nil
}
func (rpc *Campaign) ById(
	req GetByIdParams, res *service.Campaign) error {

	campaign, ok := service.Svc.Campaigns.ById[req.Id]
	if !ok {
		notFound.Inc()
		campaignNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = campaign
	success.Inc()
	return nil
}
func (rpc *Campaign) ByServiceId(
	req GetByIdParams, res *service.Campaign) error {

	campaign, ok := service.Svc.Campaigns.ByServiceId[req.Id]
	if !ok {
		notFound.Inc()
		campaignNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = campaign
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
	campaign, ok := service.Svc.Campaigns.ById[campaignId]
	if !ok {
		log.Errorf("campaign not found %#v", req.Key)
		notFound.Inc()
		campaignNotFound.Inc()
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
		Campaigns: service.Svc.Campaigns.ByLink,
	}
	success.Inc()
	return nil
}

// BlackList
type BlackList struct{}

func (rpc *BlackList) ByMsisdn(
	req GetByMsisdnParams, res *BoolResponse) error {

	_, ok := service.Svc.BlackList.ByMsisdn[req.Msisdn]
	if ok {
		*res = BoolResponse{Result: true}
	} else {
		*res = BoolResponse{Result: false}
	}
	success.Inc()
	return nil
}
func (rpc *BlackList) Add(
	req BlackListedParams, res *BoolResponse) error {

	for _, msisdn := range req.Msisdns {
		service.Svc.BlackList.Add(msisdn)
	}
	*res = BoolResponse{Result: true}
	success.Inc()
	return nil
}
func (rpc *BlackList) Delete(
	req BlackListedParams, res *BoolResponse) error {

	for _, msisdn := range req.Msisdns {
		service.Svc.BlackList.Delete(msisdn)
	}
	*res = BoolResponse{Result: true}
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

	service.SetMsisdnCampaignCache(req.CampaignId, req.Msisdn)
	success.Inc()
	return nil
}

func (rpc *RejectedByCampaign) Get(
	req RejectedParams, res *int64) error {

	campaignId := service.GetMsisdnCampaignCache(req.CampaignId, req.Msisdn)
	*res = campaignId
	success.Inc()
	return nil
}

type RejectedByService struct{}

func (rpc *RejectedByService) Set(
	req RejectedParams, res *BoolResponse) error {

	service.SetMsisdnServiceCache(req.ServiceId, req.Msisdn)
	success.Inc()
	return nil
}

func (rpc *RejectedByService) Is(
	req RejectedParams, res *bool) error {

	is := service.IsMsisdnRejectedByService(req.ServiceId, req.Msisdn)
	*res = is
	success.Inc()
	return nil
}

// Service
type Service struct{}

func (rpc *Service) All(
	req GetAllParams, res *GetAllServicesResponse) error {
	*res = GetAllServicesResponse{
		Services: service.Svc.Services.ById,
	}
	success.Inc()
	return nil
}

func (rpc *Service) ById(
	req GetByIdParams, res *service.Service) error {

	svc, ok := service.Svc.Services.ById[req.Id]
	if !ok {
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

func (rpc *PixelSetting) ByCampaignId(
	req GetByIdParams, res *service.PixelSetting) error {

	svc, ok := service.Svc.PixelSettings.ByCampaignId[req.Id]
	if !ok {
		notFound.Inc()
		pixelSettingNotFound.Inc()
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
		pixelSettingNotFound.Inc()
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
		pixelSettingNotFound.Inc()
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
	req GetByIdParams, res *service.Content) error {

	content, ok := service.Svc.Contents.ById[req.Id]
	if !ok {
		notFound.Inc()
		errors.Inc()
		return nil
	}
	*res = content
	success.Inc()
	return nil
}

// Content Sent
type ContentSent struct{}

func (rpc *ContentSent) Clear(
	req GetByParams, res *Response) error {
	service.Svc.SentContents.Clear(req.Msisdn, req.ServiceId)
	success.Inc()
	return nil
}
func (rpc *ContentSent) Push(
	req GetByParams, res *Response) error {
	service.Svc.SentContents.Push(req.Msisdn, req.ServiceId, req.ContentId)
	return nil
}
func (rpc *ContentSent) Get(
	req GetByParams, res *GetContentSentResponse) error {

	contentIds := service.Svc.SentContents.Get(req.Msisdn, req.ServiceId)
	*res = GetContentSentResponse{ContentdIds: contentIds}
	success.Inc()
	return nil
}

// Operator
type Operator struct{}

func (rpc *Operator) ByCode(
	req GetByCodeParams, res *service.Operator) error {

	operator, ok := service.Svc.Operators.ByCode[req.Code]
	if !ok {
		notFound.Inc()
		operatorNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = operator
	success.Inc()
	return nil
}

func (rpc *Operator) ByName(
	req GetByNameParams, res *service.Operator) error {

	operator, ok := service.Svc.Operators.ByName[strings.ToLower(req.Name)]
	if !ok {
		notFound.Inc()
		operatorNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = operator
	success.Inc()
	return nil
}

type Prefix struct{}

func (rpc *Prefix) GetOperator(
	req GetByPrefixParams, res *service.Operator) error {
	operator_code, ok := service.Svc.Prefixes.OperatorCodeByPrefix[req.Prefix]
	if !ok {
		notFound.Inc()
		unknownPrefix.Inc()
		errors.Inc()
		return nil
	}
	operator, ok := service.Svc.Operators.ByCode[operator_code]
	if !ok {
		notFound.Inc()
		operatorNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = operator
	success.Inc()
	return nil
}

// IpInfo
type IPInfo struct{}

func (rpc *IPInfo) ByMsisdn(
	req GetByMsisdnParams, res *service.IPInfo) error {

	for prefix, operatorCode := range service.Svc.Prefixes.OperatorCodeByPrefix {
		if strings.HasPrefix(req.Msisdn, prefix) {
			info := service.IPInfo{
				Supported:    true,
				OperatorCode: operatorCode,
			}
			if ipRange, ok := service.Svc.IpRanges.ByOperatorCode[operatorCode]; ok {
				info.OperatorCode = ipRange.OperatorCode
				info.CountryCode = ipRange.CountryCode
				info.MsisdnHeaders = ipRange.MsisdnHeaders
				if operatorCode != 0 {
					info.Supported = true
				}
			}
			*res = info
			success.Inc()
			return nil
		}
	}
	notFound.Inc()
	errors.Inc()
	return nil
}

func (rpc *IPInfo) ByIP(
	req GetByIPsParams, res *GetByIPsResponse) error {

	var infos []service.IPInfo
	for _, ip := range req.IPs {
		info := service.IPInfo{IP: ip.String(), Supported: false}

		if service.IsPrivateSubnet(ip) {
			info.Local = true
			infos = append(infos, info)
			continue
		}

		for _, ipRange := range service.Svc.IpRanges.Data {
			if ipRange.In(ip) {
				info.Range = ipRange
				info.OperatorCode = ipRange.OperatorCode
				info.CountryCode = ipRange.CountryCode
				info.MsisdnHeaders = ipRange.MsisdnHeaders
				if info.OperatorCode != 0 {
					info.Supported = true
				}
			}
		}
		infos = append(infos, info)
	}
	*res = GetByIPsResponse{IPInfos: infos}
	success.Inc()
	return nil
}

type UniqueUrls struct{}

func (rpc *UniqueUrls) Get(req GetByKeyParams, res *service.ContentSentProperties) error {
	properties, err := service.Svc.UniqueUrls.Get(req.Key)
	if err != nil {
		notFound.Inc()
		urlCacheNotFound.Inc()
		errors.Inc()
		return nil
	}
	*res = properties
	success.Inc()
	return nil
}

func (rpc *UniqueUrls) Set(req service.ContentSentProperties, res *Response) error {
	service.Svc.UniqueUrls.Set(req)
	success.Inc()
	return nil
}
func (rpc *UniqueUrls) Delete(req service.ContentSentProperties, res *Response) error {
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
