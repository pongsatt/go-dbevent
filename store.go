package dbevent

// StoreDriver represents event store driver
type StoreDriver interface {
	Provision() error
	Create(events ...*Event) error
	Close() error
	ConsumerDriver
}

// Store represents event store
type Store struct {
	driver StoreDriver
}

// NewStore creates new store
func NewStore(driver StoreDriver) *Store {
	if err := driver.Provision(); err != nil {
		panic(err)
	}

	return &Store{
		driver: driver,
	}
}

// Produce creates new event
func (store *Store) Produce(events ...*Event) error {
	return store.driver.Create(events...)
}

// NewConsumer creates new consumer for store
func (store *Store) NewConsumer(readGroup string, config *ConsumerConfig) *Consumer {
	return NewConsumer(readGroup, store.driver, config)
}

// Close driver
func (store *Store) Close() error {
	return store.driver.Close()
}
