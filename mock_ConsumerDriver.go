// Code generated by mockery v2.6.0. DO NOT EDIT.

package dbevent

import (
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// MockConsumerDriver is an autogenerated mock type for the ConsumerDriver type
type MockConsumerDriver struct {
	mock.Mock
}

// CommitInTrans provides a mock function with given fields: readGroup, event, handler
func (_m *MockConsumerDriver) CommitInTrans(readGroup string, event *Event, handler func() error) error {
	ret := _m.Called(readGroup, event, handler)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, *Event, func() error) error); ok {
		r0 = rf(readGroup, event, handler)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Fetch provides a mock function with given fields: readGroup, limit
func (_m *MockConsumerDriver) Fetch(readGroup string, limit int) ([]*Event, error) {
	ret := _m.Called(readGroup, limit)

	var r0 []*Event
	if rf, ok := ret.Get(0).(func(string, int) []*Event); ok {
		r0 = rf(readGroup, limit)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*Event)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int) error); ok {
		r1 = rf(readGroup, limit)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// WaitChange provides a mock function with given fields: timeout
func (_m *MockConsumerDriver) WaitChange(timeout time.Duration) {
	_m.Called(timeout)
}
