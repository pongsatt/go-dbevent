package dbevent

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConsumer_ConsumeNormal(t *testing.T) {
	mockBackoffer := &MockBackoffer{}
	mockDriver := &MockConsumerDriver{}

	readGroup := "testGroup"

	events := []*Event{{ID: 1}}

	mockBackoffer.On("ResetSleepBackoff")
	mockDriver.On("Fetch", readGroup, mock.Anything).Return(events, nil)

	mockDriver.On("CommitInTrans", readGroup, mock.Anything, mock.Anything).Return(nil).
		Run(func(args mock.Arguments) {
			evenHandler := args.Get(2).(func() error)
			evenHandler()
		})

	consumer := &Consumer{
		driver:    mockDriver,
		backoff:   mockBackoffer,
		readGroup: readGroup,
		config:    &ConsumerConfig{},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	var gotEvent *Event
	onMessage := func(event *Event) error {
		gotEvent = event
		consumer.Close() // run only once
		wg.Done()
		return nil
	}

	consumer.Consume(onMessage)

	wg.Wait()

	if assert.NotNil(t, gotEvent) {
		assert.Equal(t, gotEvent.ID, uint(1))
	}
}

func TestConsumer_ConsumeNoEvents(t *testing.T) {
	mockBackoffer := &MockBackoffer{}
	mockDriver := &MockConsumerDriver{}

	readGroup := "testGroup"

	events := []*Event{}

	mockBackoffer.On("ResetSleepBackoff")
	mockDriver.On("Fetch", readGroup, mock.Anything).Return(events, nil)

	consumer := &Consumer{
		driver:    mockDriver,
		backoff:   mockBackoffer,
		readGroup: readGroup,
		config:    &ConsumerConfig{},
	}

	onMessage := func(event *Event) error {
		return nil
	}

	mockDriver.On("WaitChange", mock.Anything).Run(func(args mock.Arguments) {
		consumer.Close()
	})

	consumer.Consume(onMessage)
}

func TestConsumer_ConsumeError(t *testing.T) {
	mockBackoffer := &MockBackoffer{}
	mockDriver := &MockConsumerDriver{}

	readGroup := "testGroup"

	events := []*Event{{ID: 1}}

	mockBackoffer.On("ResetSleepBackoff")
	mockBackoffer.On("SleepBackoff")
	mockDriver.On("Fetch", readGroup, mock.Anything).Return(events, nil)
	mockDriver.On("CommitInTrans", readGroup, mock.Anything, mock.Anything).
		Return(func(readGroup string, event *Event, handler func() error) error {
			return handler()
		})

	consumer := &Consumer{
		driver:    mockDriver,
		backoff:   mockBackoffer,
		readGroup: readGroup,
		config:    &ConsumerConfig{},
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var called int
	onMessage := func(event *Event) error {
		called++
		wg.Done()

		if called == 2 {
			consumer.Close() // run twice
		}
		return errors.New("mock error")
	}

	consumer.Consume(onMessage)
	wg.Wait()

	mockDriver.AssertNumberOfCalls(t, "Fetch", 2)
	mockBackoffer.AssertNumberOfCalls(t, "SleepBackoff", 2)
}

func TestConsumer_FetchError(t *testing.T) {
	mockBackoffer := &MockBackoffer{}
	mockDriver := &MockConsumerDriver{}

	readGroup := "testGroup"

	mockErr := errors.New("mock error")

	mockDriver.On("Fetch", readGroup, mock.Anything).Return(nil, mockErr)

	consumer := &Consumer{
		driver:    mockDriver,
		backoff:   mockBackoffer,
		readGroup: readGroup,
		config:    &ConsumerConfig{},
	}

	var called bool
	onMessage := func(event *Event) error {
		called = true
		return nil
	}

	mockBackoffer.On("SleepBackoff").Once().Run(func(args mock.Arguments) {
		consumer.Close()
	})

	consumer.Consume(onMessage)

	assert.Equal(t, false, called)
}
