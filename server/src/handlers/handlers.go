package handlers

import (
	"errors"
	"net"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/inmem/service"
)

var errNotFound = errors.New("Not found")

type GetAllParams struct {
}
type GetAllCampaignsResponse struct {
	Campaigns map[string]service.Campaign
}
type GetByHashParams struct {
	Hash string `json:"hash,omitempty"`
}
type GetByCodeParams struct {
	Code int64 `json:"code"`
}
type GetByLinkParams struct {
	Link string `json:"link,omitempty"`
}
type GetByIdParams struct {
	Id int64 `json:"id,omitempty"`
}
type GetByMsisdnParams struct {
	Msisdn string `json:"msisdn"`
}
type GetByIPsParams struct {
	IPs []net.IP `json:"ips"`
}
type GetByIPsResponse struct {
	IPInfos []service.IPInfo `json:"ip_infos"`
}

type GetByKeyParams struct {
	Key string `json:"key,omitempty"`
}
type GetByParams struct {
	Msisdn    string `json:"msisdn,omitempty"`
	ServiceId int64  `json:"service_id,omitempty"`
	ContentId int64  `json:"content_id,omitempty"`
}
type Response struct {
}

// Campaign
type Campaign struct{}

func (rpc *Campaign) ByHash(
	req GetByHashParams, res *service.Campaign) error {

	campaign, ok := service.Svc.Campaigns.ByHash[req.Hash]
	if !ok {
		return errNotFound
	}
	*res = campaign
	return nil
}
func (rpc *Campaign) ByLink(
	req GetByLinkParams, res *service.Campaign) error {

	campaign, ok := service.Svc.Campaigns.ByLink[req.Link]
	if !ok {
		return errNotFound
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

// Service
type Service struct{}

func (rpc *Service) ById(
	req GetByIdParams, res *service.Service) error {

	svc, ok := service.Svc.Services.ById[req.Id]
	if !ok {
		return errNotFound
	}
	*res = svc
	return nil
}

// Content
type Content struct{}

func (rpc *Content) ById(
	req GetByIdParams, res *service.Content) error {

	content, ok := service.Svc.Contents.ById[req.Id]
	if !ok {
		return errNotFound
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
	req GetByParams, res *Response) error {
	service.Svc.SentContents.Get(req.Msisdn, req.ServiceId)
	return nil
}

// Operator
type Operator struct{}

func (rpc *Operator) ByCode(
	req GetByCodeParams, res *service.Operator) error {

	operator, ok := service.Svc.Operators.ByCode[req.Code]
	if !ok {
		return errNotFound
	}
	*res = operator
	return nil
}

// IpInfo
type IPInfo struct{}

func (rpc *IPInfo) ByMsisdn(
	req GetByMsisdnParams, res *service.IPInfo) error {

	for prefix, operatorCode := range service.Svc.Prefixes.Map {
		if strings.HasPrefix(req.Msisdn, prefix) {
			info := service.IPInfo{
				Supported:    true,
				OperatorCode: operatorCode,
			}
			if ipRange, ok := service.Svc.IpRanges.ByOperatorCode[operatorCode]; ok {
				info.OperatorCode = ipRange.OperatorCode
				info.CountryCode = ipRange.CountryCode
				if operatorCode != 0 {
					info.Supported = true
				}
			}
			*res = info
			return nil
		}
	}
	return errNotFound
}
func (rpc *IPInfo) ByIP(
	req GetByIPsParams, res *GetByIPsResponse) error {

	var infos []service.IPInfo
	for _, ip := range req.IPs {
		info := service.IPInfo{IP: ip.String(), Supported: false}

		if service.IsPrivateSubnet(ip) {
			info.Local = true
			log.WithFields(log.Fields{
				"info":  info.IP,
				"from ": info.Range.IpFrom,
				"to":    info.Range.IpTo,
			}).Debug("found local ip info")

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
		log.WithFields(log.Fields{
			"info":         info.IP,
			"from":         info.Range.IpFrom,
			"to":           info.Range.IpTo,
			"supported":    info.Supported,
			"operatorCode": info.OperatorCode,
		}).Debug("found ip info")

		infos = append(infos, info)
	}
	*res = GetByIPsResponse{IPInfos: infos}
	return nil
}
