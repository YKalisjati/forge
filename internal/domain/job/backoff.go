package job

import (
	"math/rand"
	"time"
)

// BackoffPolicy calculates retry backoff delays using full jitter exponential backoff.
//
// Formula: Backoff = min(MaxInterval, BaseInterval * 2^attempt + random_jitter)
// where random_jitter is uniform in [0, BaseInterval].
type BackoffPolicy struct {
	BaseInterval time.Duration
	MaxInterval  time.Duration
}

// DefaultBackoffPolicy returns the default production backoff parameters (2s base, 300s max).
func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{
		BaseInterval: 2 * time.Second,
		MaxInterval:  300 * time.Second,
	}
}

// NextDelay computes the duration to wait before the next attempt.
// attempt is 1-indexed (the attempt number that just failed).
func (b BackoffPolicy) NextDelay(attempt int) time.Duration {
	base := b.BaseInterval
	if base <= 0 {
		base = 2 * time.Second
	}
	maxInterval := b.MaxInterval
	if maxInterval <= 0 {
		maxInterval = 300 * time.Second
	}

	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 30 {
		shift = 30
	}

	multiplier := 1 << shift
	baseDelay := time.Duration(multiplier) * base

	// Full jitter in [0, BaseInterval]
	var jitter time.Duration
	if base > 0 {
		jitter = time.Duration(rand.Int63n(int64(base)))
	}

	delay := baseDelay + jitter
	if delay > maxInterval {
		delay = maxInterval
	}
	return delay
}
