package service

// targets - links to redirect rejected traffic

import (
	"database/sql"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type Targets struct {
	sync.RWMutex
	ById map[int64]Target
}
type Target struct {
	TargetId    int64   `json:"target_id,omitempty"`
	PartnerId   int64   `json:"partner_id,omitempty"`
	Amount      int64   `json:"amount,omitempty"`
	Target      string  `json:"target,omitempty"`
	RateLimit   int     `json:"rate_limit,omitempty"`
	PricePerHit float64 `json:"price_per_hit,omitempty"`
	Score       int64   `json:"score,omitempty"`
}

func (t *Targets) Reload() error {
	t.Lock()
	defer t.Unlock()

	query := "SELECT " +
		"id, " +
		"id_partner, " +
		"amount, " +
		"target, " +
		"rate_limit, " +
		"price_per_hit, " +
		"score " +
		"FROM tr.partners_targets"

	var err error
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var targets []Target
	for rows.Next() {
		var target Target
		if err = rows.Scan(
			&target.TargetId,
			&target.PartnerId,
			&target.Amount,
			&target.Target,
			&target.RateLimit,
			&target.PricePerHit,
			&target.Score,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		log.Debugf("%#v", target)
		targets = append(targets, target)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	t.ById = make(map[int64]Target, len(targets))
	for _, target := range targets {
		t.ById[target.TargetId] = target
	}
	return nil
}
