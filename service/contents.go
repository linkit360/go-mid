package service

import (
	"database/sql"
	"fmt"
	"sync"
)

// Tasks:
// Keep in memory all active content_ids mapping to their object string (url path to content)
// Allow to get object for given content id
// Reload when changes to content
type Contents struct {
	sync.RWMutex
	ById map[int64]Content
}
type Content struct {
	Id   int64
	Path string
	Name string
}

func (s *Contents) Reload() (err error) {
	s.Lock()
	defer s.Unlock()

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"object, "+
		"content_name "+
		"FROM %scontent "+
		"WHERE status = $1",
		Svc.dbConf.TablePrefix)

	var rows *sql.Rows
	rows, err = Svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var contents []Content
	for rows.Next() {
		var c Content
		if err = rows.Scan(
			&c.Id,
			&c.Path,
			&c.Name,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		contents = append(contents, c)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.ById = make(map[int64]Content)
	for _, content := range contents {
		s.ById[content.Id] = content
	}
	return nil
}
