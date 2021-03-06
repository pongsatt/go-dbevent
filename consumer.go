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
	CommitInTrans(readGroup string, event *Event, handler func() error) error
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
	driver         ConsumerDriver
	fetchBackoff   Backoffer
	handlerBackoff Backoffer
	config         *ConsumerConfig
	readGroup      string
	running        bool
	closeChan      chan bool
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
		readGroup:      readGroup,
		driver:         driver,
		fetchBackoff:   NewBackoff(&BackOffConfig{}),
		handlerBackoff: NewBackoff(&BackOffConfig{}),
		config:         config,
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
				consumer.fetchBackoff.SleepBackoff()
				continue
			}

			consumer.fetchBackoff.ResetSleepBackoff()

			for _, event := range events {
				err = driver.CommitInTrans(consumer.readGroup, event, func() error {
					return onMessage(event)
				})

				if err != nil {
					consumer.handlerBackoff.SleepBackoff()
					break
				}

				if err != nil {
					fmt.Printf("cannot commit event %d. error: %s\n", event.ID, err)
					consumer.handlerBackoff.SleepBackoff()
					break
				}

				consumer.handlerBackoff.ResetSleepBackoff()
			}

			if len(events) == 0 {
				consumer.driver.WaitChange(time.Duration(consumer.config.WaitChangeTimeoutSec) * time.Second)
			}
		}
		consumer.closeChan <- true
	}()

}
