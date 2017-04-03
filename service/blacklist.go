package service

// very large table.
// probably, need another type of searching blacklisted numbers
import (
	"database/sql"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"

	acceptor_client "github.com/linkit360/go-acceptor-client"
)

type BlackList struct {
	sync.RWMutex
	ByMsisdn map[string]struct{}
}

func (bl *BlackList) Add(msisdn string) {
	bl.ByMsisdn[msisdn] = struct{}{}
}
func (bl *BlackList) Delete(msisdn string) {
	delete(bl.ByMsisdn, msisdn)
}
func (bl *BlackList) getBlackListedDBCache() (msisdns []string, err error) {
	query := fmt.Sprintf("SELECT "+
		"msisdn FROM %smsisdn_blacklist",
		Svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = Svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var msisdn string
		if err = rows.Scan(&msisdn); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		msisdns = append(msisdns, msisdn)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	return
}

func (bl *BlackList) getBlackListed() ([]string, error) {
	msisdns, err := acceptor_client.BlackListGet(Svc.conf.ProviderName)
	if err != nil {
		err = fmt.Errorf("acceptor_client.GetBlackList: %s", err.Error())
		log.WithFields(log.Fields{"error": err.Error()}).Error("cannot get blacklist from client")
		return []string{}, err
	}
	return msisdns, nil
}

func (bl *BlackList) Reload() error {
	bl.Lock()
	defer bl.Unlock()

	blackList, err := bl.getBlackListed()
	if err != nil {
		blackList, err = bl.getBlackListedDBCache()
		if err != nil {
			err = fmt.Errorf("bl.getBlackListedDBCache: %s", err.Error())
			log.WithFields(log.Fields{"error": err.Error()}).Error("cannot get blacklist from db cache")
			return err
		}
	}

	bl.ByMsisdn = make(map[string]struct{}, len(blackList))
	for _, msisdn := range blackList {
		bl.ByMsisdn[msisdn] = struct{}{}
	}
	return nil
}
