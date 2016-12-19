package handlers

import (
	"net"
	"strings"

	"github.com/vostrok/inmem/service"
)

type GetAllParams struct {
}
type GetAllCampaignsResponse struct {
	Campaigns map[string]service.Campaign `json:"campaigns,omitempty"`
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
type Response struct {
}
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
		return nil
	}
	*res = campaign
	return nil
}
func (rpc *Campaign) ByLink(
	req GetByLinkParams, res *service.Campaign) error {

	campaign, ok := service.Svc.Campaigns.ByLink[req.Link]
	if !ok {
		notFound.Inc()
		campaignNotFound.Inc()
		return nil
	}
	*res = campaign
	return nil
}

func (rpc *Campaign) ByKeyWord(
	req GetByKeyWordParams, res *service.Campaign) error {

	campaignId, ok := service.Svc.KeyWords.ByKeyWord[req.Key]
	if !ok {
		notFound.Inc()
		keyWordNotFound.Inc()
		return nil
	}
	campaign, ok := service.Svc.Campaigns.ById[campaignId]
	if !ok {
		notFound.Inc()
		campaignNotFound.Inc()
		return nil
	}
	*res = campaign
	return nil
}

func (rpc *Campaign) All(
	req GetAllParams, res *GetAllCampaignsResponse) error {
	*res = GetAllCampaignsResponse{
		Campaigns: service.Svc.Campaigns.ByLink,
	}
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
	return nil
}
func (rpc *PostPaid) Push(
	req GetByMsisdnParams, res *BoolResponse) error {

	service.Svc.PostPaid.Push(req.Msisdn)
	*res = BoolResponse{Result: true}

	return nil
}
func (rpc *PostPaid) Remove(
	req GetByMsisdnParams, res *BoolResponse) error {

	service.Svc.PostPaid.Remove(req.Msisdn)
	*res = BoolResponse{Result: true}

	return nil
}

// Service
type Service struct{}

func (rpc *Service) ById(
	req GetByIdParams, res *service.Service) error {

	svc, ok := service.Svc.Services.ById[req.Id]
	if !ok {
		notFound.Inc()
		return nil
	}
	*res = svc
	return nil
}

// Pixel Setting
type PixelSetting struct{}

func (rpc *PixelSetting) ByKey(
	req GetByKeyParams, res *service.PixelSetting) error {

	svc, ok := service.Svc.PixelSettings.ByKey[req.Key]
	if !ok {
		notFound.Inc()
		pixelSettingNotFound.Inc()
		return nil
	}
	*res = *svc
	return nil
}
func (rpc *PixelSetting) ByCampaignId(
	req GetByIdParams, res *service.PixelSetting) error {

	svc, ok := service.Svc.PixelSettings.ByCampaignId[req.Id]
	if !ok {
		notFound.Inc()
		pixelSettingNotFound.Inc()
		return nil
	}
	*res = *svc
	return nil
}
func (rpc *PixelSetting) ByKeyWithRatio(
	req GetByKeyParams, res *service.PixelSetting) error {

	ps, err := service.Svc.PixelSettings.ByKeyWithRatio(req.Key)
	if err != nil {
		notFound.Inc()
		pixelSettingNotFound.Inc()
		return nil
	}
	*res = ps
	return nil
}

// Content
type Content struct{}

func (rpc *Content) ById(
	req GetByIdParams, res *service.Content) error {

	content, ok := service.Svc.Contents.ById[req.Id]
	if !ok {
		notFound.Inc()
		return nil
	}
	*res = content
	return nil
}

// Content Sent
type ContentSent struct{}

func (rpc *ContentSent) Clear(
	req GetByParams, res *Response) error {
	service.Svc.SentContents.Clear(req.Msisdn, req.ServiceId)
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
		return nil
	}
	*res = operator
	return nil
}

func (rpc *Operator) ByName(
	req GetByNameParams, res *service.Operator) error {

	operator, ok := service.Svc.Operators.ByName[strings.ToLower(req.Name)]
	if !ok {
		notFound.Inc()
		operatorNotFound.Inc()
		return nil
	}
	*res = operator
	return nil
}

type Prefix struct{}

func (rpc *Prefix) GetOperator(
	req GetByPrefixParams, res *service.Operator) error {
	operator_code, ok := service.Svc.Prefixes.OperatorCodeByPrefix[req.Prefix]
	if !ok {
		notFound.Inc()
		unknownPrefix.Inc()
		return nil
	}
	operator, ok := service.Svc.Operators.ByCode[operator_code]
	if !ok {
		notFound.Inc()
		operatorNotFound.Inc()
		return nil
	}
	*res = operator
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
			return nil
		}
	}
	notFound.Inc()
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
	return nil
}
