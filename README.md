Go Database Eventing (Publish event using database)
================================

[![Test](https://github.com/pongsatt/go-dbevent/actions/workflows/test.yml/badge.svg)](https://github.com/pongsatt/go-dbevent/actions/workflows/test.yml)

Go library to help publishing events along with transation data atomically

Features include:

  * [Consume and process events (support multiple consumer by read group)](#consumer-example)
  * [Create event](#producer-example)

Running example
-------------------------------------------------------------------------------------------

### Consumer Example

Start mysql server
```sh
./start_server.sh
```

```go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pongsatt/go-dbevent"
	"github.com/pongsatt/go-dbevent/driver"
)

// Data represent example json data
type Data struct {
	ID string
}

func main() {
	var nodeID string

	if len(os.Args) > 1 {
		nodeID = os.Args[1]
	} else {
		panic("nodeID required")
	}

	dbConfig := &dbevent.DBConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		DBName:   "testdb",
		User:     "root",
		Password: "my-secret-pw",
	}

	mysqlDriver := driver.NewMySQLEventDriver(dbConfig, &driver.MySQLStoreConfig{NodeID: nodeID})
	defer mysqlDriver.Close()

	eventStore := dbevent.NewStore(mysqlDriver)
	consumer := eventStore.NewConsumer("group1", &dbevent.ConsumerConfig{})
	defer consumer.Close()

	consumer.Consume(func(event *dbevent.Event) error {
		fmt.Printf("got event %d\n", event.ID)
		return nil
	})

	fmt.Println("Consumer ready")

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	<-termChan
	fmt.Println("Done")
}
```

Start consumer
```sh
go run example/consumer_highlevel/* node1
```

### Producer Example

```go
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
```

Run producer
```sh
go run example/producer/* 1
```

Consumer output
```console
got event 1
```
