package dbevent

import (
	"testing"
)

func TestBackoff_NoRandom(t *testing.T) {
	b := NewBackoff(&BackOffConfig{
		BackoffRandomFactor: -1,
		BackoffMultiplier:   2,
		InitialBackoffMs:    1000,
	})

	if v := b.NextBackoffMs(); v != 1000 {
		t.Errorf("expect initial backoff to be 1000 but %d", v)
	}

	if v := b.NextBackoffMs(); v != 2000 {
		t.Errorf("expect backoff to be 2000 but %d", v)
	}

	if v := b.NextBackoffMs(); v != 4000 {
		t.Errorf("expect backoff to be 4000 but %d", v)
	}

	if v := b.NextBackoffMs(); v != 8000 {
		t.Errorf("expect backoff to be 8000 but %d", v)
	}

	b.ResetSleepBackoff()
	if v := b.NextBackoffMs(); v != 1000 {
		t.Errorf("expect backoff to be 1000 but %d", v)
	}
}

func TestBackoff_Random(t *testing.T) {
	b := NewBackoff(&BackOffConfig{
		BackoffRandomFactor: 0.2,
		BackoffMultiplier:   2,
		InitialBackoffMs:    1000,
	})

	if v := b.NextBackoffMs(); v != 1000 {
		t.Errorf("expect initial backoff to be 1000 but %d", v)
	}

	if v := b.NextBackoffMs(); v < 1000 || v > 2000 {
		t.Errorf("expect backoff to be between 1000 and 2000 but %d", v)
	}

	if v := b.NextBackoffMs(); v < 2000 || v > 4000 {
		t.Errorf("expect backoff to be between 2000 and 4000 but %d", v)
	}

	if v := b.NextBackoffMs(); v < 4000 || v > 8000 {
		t.Errorf("expect backoff to be between 4000 and 8000 but %d", v)
	}

	b.ResetSleepBackoff()
	if v := b.NextBackoffMs(); v != 1000 {
		t.Errorf("expect backoff to be 1000 but %d", v)
	}
}
