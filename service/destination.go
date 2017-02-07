package service

// targets - links to redirect rejected traffic

import (
	"database/sql"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/vostrok/utils/rec"
	"strconv"
	"strings"
)

type Destinations struct {
	sync.RWMutex
	ById    map[int64]Destination
	ByPrice []Destination
}

type Destination struct {
	DestinationId int64   `json:"destination_id,omitempty"`
	PartnerId     int64   `json:"partner_id,omitempty"`
	AmountLimit   int64   `json:"amount_limit,omitempty"`
	Destination   string  `json:"destination,omitempty"`
	RateLimit     int     `json:"rate_limit,omitempty"`
	PricePerHit   float64 `json:"price_per_hit,omitempty"`
	Score         int64   `json:"score,omitempty"`
	CountryCode   int64   `json:"country_code"`
	OperatorCode  int64   `json:"operator_code"`
}

func (ds *Destinations) Reload() error {
	ds.Lock()
	defer ds.Unlock()

	query := "SELECT " +
		"id, " +
		"id_partner, " +
		"amount_limit, " +
		"destination, " +
		"rate_limit, " +
		"price_per_hit, " +
		"score, " +
		"country_code, " +
		"operator_code " +
		"FROM tr.partners_destinations" +
		"WHERE active IS true" +
		"ORDER BY price_per_hit, score "

	var err error
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var d Destination
		if err = rows.Scan(
			&d.DestinationId,
			&d.PartnerId,
			&d.AmountLimit,
			&d.Destination,
			&d.RateLimit,
			&d.PricePerHit,
			&d.Score,
			&d.CountryCode,
			&d.OperatorCode,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		log.Debugf("%#v", d)
		ds.ByPrice = append(ds.ByPrice, d)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	ds.ById = make(map[int64]Destination, len(ds.ByPrice))
	for _, d := range ds.ByPrice {
		ds.ById[d.DestinationId] = d
	}
	return nil
}

type RedirectStatCounts struct {
	sync.RWMutex
	ById map[int64]*StatCount
}

type StatCount struct {
	Id    int64
	Count uint64
	Limit uint64
}

func (dh *RedirectStatCounts) Reload() {
	dh.Lock()
	defer dh.Unlock()

	ids := []int64{}
	placeHolders := []string{}
	i := 1
	for _, v := range Svc.Destinations.ById {
		ids = append(ids, v.DestinationId)
		placeHolders = append(placeHolders, fmt.Sprintf("$%d", i))
	}
	query := "SELECT id_destination, count(*) FROM destinations_hits " +
		" WHERE id_destination IN (" + strings.Join(placeHolders, ", ") +
		") GROUP BY id_destination"

	var err error
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var sc []StatCount
	for rows.Next() {
		var s StatCount
		if err = rows.Scan(
			&s.Id,
			&s.Count,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		log.Debugf("id %#v: %#v", s.Id, s.Count)
		sc = append(sc, s)
	}

	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	dh.ById = make(map[int64]Destination, len(sc))
	for _, s := range sc {
		dh.ById[s.Id] = s
	}
}

func (dh *RedirectStatCounts) IncHit(id int64) bool {
	if _, ok := dh.ById[id]; !ok {
		if _, ok := Svc.Destinations.ById[id]; !ok {
			log.Errorf("id %d: is unknown", id)
			return
		}
		dh.ById[id] = &StatCount{
			Id:    id,
			Limit: Svc.Destinations.ById[id].AmountLimit,
			Count: 0,
		}
	}
	dh.ById[id]++
	return
}
