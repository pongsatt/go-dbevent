package dbevent

import (
	"encoding/json"
	"time"
)

// builder represents event build data
type builder struct {
	event *Event
}

// NewBuilder returns new builder instance
func NewBuilder(eventType string) *builder {
	return &builder{
		event: &Event{
			Type: eventType,
		},
	}
}

// Data marshal data and put into the event
func (builder *builder) Data(data interface{}) *builder {
	b, _ := json.Marshal(data)

	builder.event.Data = b
	return builder
}

// Build returns built event
func (builder *builder) Build() *Event {
	now := time.Now()

	builder.event.CreatedAt = &now
	return builder.event
}
