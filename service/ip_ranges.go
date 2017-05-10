package service

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

type IpRanges struct {
	sync.RWMutex
	Data           []IpRange
	ByOperatorCode map[int64]IpRange
}

type IPInfo struct {
	IP            string
	CountryCode   int64
	OperatorCode  int64
	MsisdnHeaders []string
	Supported     bool
	Local         bool
	Range         IpRange
}

type IpRange struct {
	Id            int64    `json:"id,omitempty" yaml:"-"`
	OperatorCode  int64    `json:"operator_code,omitempty" yaml:"-"`
	CountryCode   int64    `json:"country_code,omitempty" yaml:"-"`
	IpFrom        string   `json:"ip_from,omitempty" yaml:"start"`
	Start         net.IP   `json:"-" yaml:"-"`
	IpTo          string   `json:"ip_to,omitempty" yaml:"end"`
	End           net.IP   `json:"-" yaml:"-"`
	MsisdnHeaders []string `yaml:"-"`
}

func (r IpRange) In(ip net.IP) bool {
	if ip.To4() == nil {
		return false
	}
	if bytes.Compare(ip, r.Start) >= 0 && bytes.Compare(ip, r.End) <= 0 {
		return true
	}
	return false
}

// msisdn could be in many headers
func (ipRanges *IpRanges) Reload() (err error) {
	ipRanges.Lock()
	defer ipRanges.Unlock()

	query := fmt.Sprintf(""+
		"SELECT "+
		"id, "+
		"operator_code, "+
		"country_code, "+
		"ip_from, "+
		"ip_to, "+
		" ( SELECT %soperators.msisdn_headers as header "+
		"FROM %soperators "+
		"WHERE operator_code = code ) "+
		" from %soperator_ip",
		Svc.dbConf.TablePrefix,
		Svc.dbConf.TablePrefix,
		Svc.dbConf.TablePrefix,
	)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	operatorLoadHeaderError := false
	var records []IpRange
	for rows.Next() {
		record := IpRange{}

		var headers string
		if err = rows.Scan(
			&record.Id,
			&record.OperatorCode,
			&record.CountryCode,
			&record.IpFrom,
			&record.IpTo,
			&headers,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		decodedHeaders := make([]string, 0)
		if err := json.Unmarshal([]byte(headers), &decodedHeaders); err != nil {
			operatorLoadHeaderError = true
		}
		record.MsisdnHeaders = decodedHeaders
		record.Start = net.ParseIP(record.IpFrom)
		record.End = net.ParseIP(record.IpTo)
		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	if operatorLoadHeaderError == false {
		Svc.m.LoadOperatorHeaderError.Set(0.)
	} else {
		Svc.m.LoadOperatorHeaderError.Set(1.)
	}

	ipRanges.Data = records
	ipRanges.ByOperatorCode = make(map[int64]IpRange)
	for _, v := range records {
		ipRanges.ByOperatorCode[v.OperatorCode] = v
	}

	return nil
}

func IsPrivateSubnet(ipAddress net.IP) bool {
	if ipCheck := ipAddress.To4(); ipCheck != nil {
		for _, r := range Svc.privateIPRanges {
			if r.In(ipAddress) {
				return true
			}
		}
	}
	return false
}

func GetSupportedIPInfo(infos []IPInfo) IPInfo {
	for _, v := range infos {
		if !v.Supported {
			continue
		}
		return v
	}
	return IPInfo{Supported: false}
}
