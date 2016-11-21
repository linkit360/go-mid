package handlers

import (
	"errors"

	"github.com/vostrok/inmem/service"
)

var errNotFound = errors.New("Not found")

type Campaign struct{}
type GetByHashParams struct {
	Hash string `json:"hash,omitempty"`
}
type GetByIdParams struct {
	Id int64 `json:"id,omitempty"`
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

func (rpc *Campaign) ByHash(
	req GetByHashParams, res *service.Campaign) error {

	campaign, ok := service.Svc.Campaigns.ByHash[req.Hash]
	if !ok {
		return errNotFound
	}
	*res = campaign
	return nil
}

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
