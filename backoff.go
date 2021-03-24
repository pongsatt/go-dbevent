package dbevent

import (
	"math/rand"
	"time"
)

// BackOffConfig represents backoff configuration
type BackOffConfig struct {
	InitialBackoffMs    int
	BackoffMultiplier   int
	BackoffRandomFactor float32
}

// Backoff represents exponential backoff
type Backoff struct {
	previousBackoffMs int
	config            *BackOffConfig
}

// NewBackoff creates new backoff object
func NewBackoff(config *BackOffConfig) *Backoff {
	if config.BackoffMultiplier == 0 {
		config.BackoffMultiplier = 2
	}

	if config.BackoffRandomFactor == 0 {
		config.BackoffRandomFactor = 0.2
	} else if config.BackoffRandomFactor == -1 {
		config.BackoffRandomFactor = 0
	}

	if config.InitialBackoffMs == 0 {
		config.InitialBackoffMs = 1000
	}

	return &Backoff{
		config: config,
	}
}

// NextBackoffMs returns next backoff time in milisecond
func (backoff *Backoff) NextBackoffMs() int {
	var sleepMs int

	if backoff.previousBackoffMs == 0 {
		sleepMs = backoff.config.InitialBackoffMs
	} else {
		min := int(float32(backoff.previousBackoffMs) * (1 - backoff.config.BackoffRandomFactor))
		max := int(float32(backoff.previousBackoffMs) * (1 + backoff.config.BackoffRandomFactor))

		baseMs := min

		if max > min {
			baseMs = min + rand.Intn(max-min)
		}

		sleepMs = baseMs * backoff.config.BackoffMultiplier
	}

	backoff.previousBackoffMs = sleepMs
	return sleepMs
}

// SleepBackoff sleeps using backoff delay
func (backoff *Backoff) SleepBackoff() {
	sleepMs := backoff.NextBackoffMs()

	time.Sleep(time.Duration(sleepMs) * time.Millisecond)
}

// ResetSleepBackoff resets backoff
func (backoff *Backoff) ResetSleepBackoff() {
	backoff.previousBackoffMs = 0
}
