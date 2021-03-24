package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	// we need it
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pongsatt/go-dbevent"
	"github.com/pongsatt/go-dbevent/driver"
)

// Data represent example json data
type Data struct {
	ID string
}

func main() {
	eventNum := 1

	if len(os.Args) > 1 {
		eventNum, _ = strconv.Atoi(os.Args[1])
	}

	dbConfig := &dbevent.DBConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		DBName:   "testdb",
		User:     "root",
		Password: "my-secret-pw",
	}

	mysqlDriver := driver.NewMySQLEventDriver(dbConfig, &driver.MySQLStoreConfig{})
	if err := mysqlDriver.Provision(); err != nil {
		panic(err)
	}

	defer mysqlDriver.Close()

	for i := 0; i < eventNum; i++ {
		now := time.Now()

		data := &Data{
			ID: uuid.NewString(),
		}

		b, _ := json.Marshal(data)
		event := &dbevent.Event{
			Type:          "testtype",
			AggregateType: "aggType",
			AggregateID:   "agg1",
			Data:          b,
			CreatedAt:     &now,
		}

		if err := mysqlDriver.Create(event); err != nil {
			panic(err)
		}
		fmt.Printf("event '%s' produced\n", data.ID)
	}
}
