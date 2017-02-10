package service

// destinations - links to redirect rejected traffic

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type Destinations struct {
	sync.RWMutex
	ById    map[int64]Destination
	ByPrice []Destination
}

type Destination struct {
	DestinationId int64   `json:"destination_id,omitempty"`
	PartnerId     int64   `json:"partner_id,omitempty"`
	AmountLimit   uint64  `json:"amount_limit,omitempty"`
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
		"FROM tr.partners_destinations " +
		"WHERE active IS true " +
		"ORDER BY price_per_hit, score "

	var err error
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var dd []Destination
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
		dd = append(dd, d)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	ds.ById = make(map[int64]Destination, len(ds.ByPrice))
	for _, d := range dd {
		ds.ById[d.DestinationId] = d
	}
	ds.ByPrice = dd
	return nil
}

type RedirectStatCounts struct {
	sync.RWMutex
	ById map[int64]*StatCount
}

type StatCount struct {
	DestinationId int64
	Count         uint64
}

func (dh *RedirectStatCounts) Reload() (err error) {
	dh.Lock()
	defer dh.Unlock()

	if len(Svc.Destinations.ById) == 0 {
		return nil
	}
	ids := []interface{}{}
	placeHolders := []string{}
	i := 1
	for _, v := range Svc.Destinations.ById {
		ids = append(ids, v.DestinationId)
		placeHolders = append(placeHolders, fmt.Sprintf("$%d", i))
		i++
	}
	query := "SELECT id_destination, count(*) FROM tr.destinations_hits " +
		" WHERE id_destination IN (" + strings.Join(placeHolders, ", ") +
		") GROUP BY id_destination"

	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ids...)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s, args: %#v", err.Error(), query, ids)
		return
	}
	defer rows.Close()

	var sc []StatCount
	for rows.Next() {
		var s StatCount
		if err = rows.Scan(
			&s.DestinationId,
			&s.Count,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		log.Debugf("id %#v: %#v", s.DestinationId, s.Count)
		sc = append(sc, s)
	}

	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	dh.ById = make(map[int64]*StatCount, len(sc))
	for _, s := range sc {
		statCount := s
		dh.ById[s.DestinationId] = &statCount
	}
	return nil
}

func (dh *RedirectStatCounts) IncHit(id int64) (err error) {
	if _, ok := dh.ById[id]; !ok {
		_, ok := Svc.Destinations.ById[id]
		if !ok {

			err = fmt.Errorf("id %d: is unknown", id)
			log.Error(err.Error())
			return
		}
		dh.ById[id] = &StatCount{
			DestinationId: id,
			Count:         0,
		}
	}
	dh.ById[id].Count++
	return nil
}
