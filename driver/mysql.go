package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pongsatt/go-dbevent"
)

var (
	defaultLockTimeoutSec = 20
)

// MySQLStoreConfig represents sql store configuration
type MySQLStoreConfig struct {
	NodeID         string
	LockTimeoutSec int
}

// MySQLDriver represents event database
type MySQLDriver struct {
	db     *sql.DB
	change *MySQLChange
	config *MySQLStoreConfig
}

// NewMySQLEventDriver creates new instance
func NewMySQLEventDriver(dbConfig *dbevent.DBConfig, config *MySQLStoreConfig) *MySQLDriver {
	db, err := sql.Open("mysql", fmt.Sprintf("%s?parseTime=true", dbConfig.ToDSN()))

	if err != nil {
		panic(err)
	}

	err = db.Ping()

	if err != nil {
		panic(err)
	}

	if config.LockTimeoutSec == 0 {
		config.LockTimeoutSec = defaultLockTimeoutSec
	}

	change := NewMySQLChange(dbConfig, "events")

	return &MySQLDriver{
		db:     db,
		change: change,
		config: config,
	}
}

// WaitChange waits for event change
func (db *MySQLDriver) WaitChange(timeout time.Duration) {
	db.change.WaitChange(timeout)
}

// Close all mysql resources
func (db *MySQLDriver) Close() error {
	db.change.Close()

	if err := db.db.Close(); err != nil {
		return err
	}
	return nil
}

// Provision prepares event tables
func (db *MySQLDriver) Provision() error {
	if err := db.createEventTable(); err != nil {
		return err
	}

	if err := db.createEventLockTable(); err != nil {
		return err
	}

	if err := db.createEventOffsetTable(); err != nil {
		return err
	}

	return nil
}

func (db *MySQLDriver) createEventTable() error {
	query := `
    CREATE TABLE IF NOT EXISTS events (
        id INT AUTO_INCREMENT,
        type TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
		data JSON DEFAULT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (id)
    );`

	_, err := db.db.Exec(query)

	if err != nil {
		return err
	}

	return nil
}

func (db *MySQLDriver) createEventOffsetTable() error {
	query := `
    CREATE TABLE IF NOT EXISTS event_offsets (
        name varchar(128),
        offset INT NOT NULL,
        PRIMARY KEY (name)
    );`

	_, err := db.db.Exec(query)

	if err != nil {
		return err
	}

	return nil
}

func (db *MySQLDriver) currentOffset(readGroup string) (uint, error) {
	query := `SELECT offset FROM event_offsets WHERE name = ?`

	var offset uint
	err := db.db.QueryRow(query, readGroup).Scan(&offset)

	if err != nil {
		if err != sql.ErrNoRows {
			return 0, err
		}
	}

	return offset, nil
}

func (db *MySQLDriver) createEventLockTable() error {
	query := `CREATE TABLE IF NOT EXISTS event_locks ( 
		name varchar(128) NOT NULL, 
		lock_by varchar(128) NOT NULL, 
		last_seen timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, 
		PRIMARY KEY (name) 
	  ) ENGINE=InnoDB`

	_, err := db.db.Exec(query)

	if err != nil {
		return err
	}

	return nil
}

func (db *MySQLDriver) lock(name string, nodeID string) (bool, error) {
	query := `insert ignore into event_locks ( name, lock_by, last_seen ) 
	values ( ?, ?, now() ) 
	on duplicate key 
	update lock_by = if(last_seen < now() - interval ? second, values(lock_by), lock_by), 
	last_seen = if(lock_by = values(lock_by), values(last_seen), last_seen);`

	_, err := db.db.Exec(query, name, nodeID, db.config.LockTimeoutSec)

	if err != nil {
		return false, err
	}

	var count int
	err = db.db.QueryRow("select count(*) from event_locks where name=? and lock_by=?", name, nodeID).Scan(&count)

	if err != nil {
		return false, err
	}

	if count > 0 {
		return true, nil
	}

	return false, err
}

// Create event into database
func (db *MySQLDriver) Create(events ...*dbevent.Event) error {
	query := `INSERT INTO events 
	(type, aggregate_type, aggregate_id, data, created_at) VALUES `

	var inserts []string
	var params []interface{}
	for _, event := range events {
		inserts = append(inserts, "(?, ?, ?, ?, ?)")
		params = append(params, event.Type, event.AggregateType, event.AggregateID, event.Data, event.CreatedAt)
	}

	queryVals := strings.Join(inserts, ",")
	query = query + queryVals

	_, err := db.db.Exec(query, params...)

	if err != nil {
		return err
	}

	return nil
}

// Fetch events from database
func (db *MySQLDriver) Fetch(readGroup string, limit int) ([]*dbevent.Event, error) {
	// lock
	success, err := db.lock(readGroup, db.config.NodeID)

	if err != nil {
		return nil, err
	}

	if !success {
		return nil, nil
	}

	// current offset
	offset, err := db.currentOffset(readGroup)

	if err != nil {
		return nil, err
	}

	// fetch
	events, err := db.getEvents(offset, limit)

	if err != nil {
		return nil, err
	}

	return events, nil
}

// CommitEvent as processed
func (db *MySQLDriver) CommitInTrans(readGroup string, event *dbevent.Event, handler func() error) error {
	tx, err := db.db.Begin()

	if err != nil {
		return err
	}

	query := `INSERT INTO event_offsets (name, offset) VALUES (?, ?)
	ON DUPLICATE KEY UPDATE offset = ?`

	_, err = tx.Exec(query, readGroup, event.ID, event.ID)

	if err != nil {
		return err
	}

	// event handler
	if err = handler(); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (db *MySQLDriver) getEvents(offset uint, limit int) ([]*dbevent.Event, error) {
	query := `SELECT id, type, aggregate_type, aggregate_id, data, created_at FROM events WHERE id > ? LIMIT ?`

	rows, err := db.db.Query(query, offset, limit)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	events := make([]*dbevent.Event, 0)

	for rows.Next() {
		event := new(dbevent.Event)
		err = rows.Scan(&event.ID, &event.Type, &event.AggregateType, &event.AggregateID, &event.Data, &event.CreatedAt)

		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	return events, nil
}
