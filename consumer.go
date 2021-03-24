package dbevent

import (
	"fmt"
	"log"
	"time"
)

var (
	defaultWaitChangeTimeoutSec = 20
)

// ConsumerDriver represents event consumer driver
type ConsumerDriver interface {
	Fetch(readGroup string, limit int) ([]*Event, error)
	CommitEvent(readGroup string, event *Event) error
	WaitChange(timeout time.Duration)
}

// ConsumerConfig represents consumer configuration
type ConsumerConfig struct {
	WaitChangeTimeoutSec int
	BatchSize            int
}

// Backoffer represents backoff algorithm interface
type Backoffer interface {
	SleepBackoff()
	ResetSleepBackoff()
}

// Consumer represents consumer
type Consumer struct {
	driver    ConsumerDriver
	backoff   Backoffer
	config    *ConsumerConfig
	readGroup string
	running   bool
	closeChan chan bool
}

// NewConsumer creates new consumer
func NewConsumer(readGroup string, driver ConsumerDriver, config *ConsumerConfig) *Consumer {
	if config.WaitChangeTimeoutSec == 0 {
		config.WaitChangeTimeoutSec = defaultWaitChangeTimeoutSec
	}

	if config.BatchSize == 0 {
		config.BatchSize = 10
	}

	return &Consumer{
		readGroup: readGroup,
		driver:    driver,
		backoff:   NewBackoff(&BackOffConfig{}),
		config:    config,
	}
}

// Close consumer
func (consumer *Consumer) Close() {
	consumer.running = false
}

// CloseAndWait closes consumer and wait it to be done
func (consumer *Consumer) CloseAndWait() {
	consumer.closeChan = make(chan bool)
	consumer.Close()
	<-consumer.closeChan
}

// Consume subscribes to new event
func (consumer *Consumer) Consume(onMessage func(event *Event) error) {
	consumer.running = true
	driver := consumer.driver

	go func() {
		for consumer.running {
			events, err := driver.Fetch(consumer.readGroup, consumer.config.BatchSize)

			if err != nil {
				log.Printf("error while fetching events. error: %s", err)
				consumer.backoff.SleepBackoff()
				continue
			}

			consumer.backoff.ResetSleepBackoff()

			for _, event := range events {
				// process event
				if err = onMessage(event); err != nil {
					consumer.backoff.SleepBackoff()
					break
				}

				if err = driver.CommitEvent(consumer.readGroup, event); err != nil {
					fmt.Printf("cannot commit event %d. error: %s\n", event.ID, err)
					consumer.backoff.SleepBackoff()
					break
				}

				consumer.backoff.ResetSleepBackoff()
			}

			if len(events) == 0 {
				consumer.driver.WaitChange(time.Duration(consumer.config.WaitChangeTimeoutSec) * time.Second)
			}
		}
		consumer.closeChan <- true
	}()

}
