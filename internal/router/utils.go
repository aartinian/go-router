package router

import (
	"math"
	"math/rand"
	"time"
)

const (
	// Small value to avoid division by zero in weight calculations
	epsilon = 0.1
)

type exponentialBackoff struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	attempt   int
	rng       *rand.Rand
}

// newExponentialBackoff creates a new exponential backoff instance for
// connection retries.
func newExponentialBackoff(
	baseDelay time.Duration, maxDelay time.Duration,
) *exponentialBackoff {
	return &exponentialBackoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Next calculates the next delay using exponential backoff with jitter.
func (b *exponentialBackoff) Next() time.Duration {
	delay := min(
		time.Duration(float64(b.baseDelay)*math.Pow(2, float64(b.attempt))),
		b.maxDelay,
	)

	jitter := time.Duration(b.rng.Float64() * float64(delay) * 0.1)
	b.attempt++

	return delay + jitter
}

// Reset resets the backoff attempt counter to start over.
func (b *exponentialBackoff) Reset() {
	b.attempt = 0
}
