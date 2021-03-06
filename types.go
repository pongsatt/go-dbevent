package dbevent

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// JSON custom data type
type JSON json.RawMessage

// Scan scan value into Jsonb, implements sql.Scanner interface
func (j *JSON) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	result := json.RawMessage{}
	err := json.Unmarshal(bytes, &result)
	*j = JSON(result)
	return err
}

// Value return json value, implement driver.Valuer interface
func (j JSON) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return json.RawMessage(j).MarshalJSON()
}

// Event represents event data
type Event struct {
	ID            uint   `xorm:"pk 'id'"`
	Type          string `xorm:"type" gorm:"not null"`
	AggregateType string `xorm:"aggregate_type"`
	AggregateID   string `xorm:"aggregate_id"`
	Data          JSON
	CreatedAt     *time.Time `xorm:"created_at"`
}

// DBConfig represents database configuration
type DBConfig struct {
	DSN      string
	Host     string
	Port     int
	DBName   string
	User     string
	Password string
}

// ToDSN return datasource name
func (config *DBConfig) ToDSN() string {
	if config.DSN != "" {
		return config.DSN
	}

	return fmt.Sprintf("%s:%s@(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.DBName)
}
