package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	if err := mysqlDriver.Provision(); err != nil {
		panic(err)
	}

	defer mysqlDriver.Close()

	go func() {
		groupID := "group1"
		for {
			events, err := mysqlDriver.Fetch(groupID, 5)

			if err != nil {
				panic(err)
			}

			fmt.Printf("%s -> found %d events\n", nodeID, len(events))

			for _, event := range events {
				// process event

				if err = mysqlDriver.CommitEvent(groupID, event); err != nil {
					fmt.Println("error committing an event")
				}

				fmt.Printf("%s -> event id %d committed\n", nodeID, event.ID)
			}

			// time.Sleep(time.Duration(rand.Intn(10)) * time.Second)

			if len(events) == 0 {
				fmt.Println("no events. let's wait")
				mysqlDriver.WaitChange(20 * time.Second)
				fmt.Println("new event arrived or timed out. let's continue")
			}
		}
	}()

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	<-termChan
	fmt.Println("Done")
}
