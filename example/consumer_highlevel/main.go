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

	eventStore := dbevent.NewStore(mysqlDriver)
	defer eventStore.Close()

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
