package mysql

import (
	"math"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// intervaler is an interface that defines any method to be used when implementing backoff.
type intervaler interface {
	// NextInterval defines the next interval for backoff.
	NextInterval(order int) time.Duration
}

const (
	// defaultBackoffInterval is the default value for any backoff interval.
	defaultBackoffInterval = 500 * time.Millisecond
	// defaultJitterInterval is the default value for any jitter interval.
	defaultJitterInterval = 200 * time.Millisecond
	// defaultMultiplier is the default value factor for exponential backoff.
	defaultMultiplier = 2
	// defaultMaxInterval is the maximum interval for backoff.
	defaultMaxInterval = 3 * time.Second
)

// constantBackoff implements intervaler using constant interval.
type constantBackoff struct {
	// backoffInterval defines how long the next backoff will be compared to the previous one.
	backoffInterval time.Duration
	// jitterInterval defines the randomness additional value for interval.
	jitterInterval time.Duration
	// maxInterval defines the maximum interval allowed.
	maxInterval time.Duration
}

// newConstantBackoff creates an instance of constantBackoff with default values.
// See Constants for the default values.
func newConstantBackoff() constantBackoff {
	return constantBackoff{
		backoffInterval: defaultBackoffInterval,
		jitterInterval:  defaultJitterInterval,
		maxInterval:     defaultMaxInterval,
	}
}

// NextInterval returns next interval for backoff.
func (c constantBackoff) NextInterval(order int) time.Duration {
	if order <= 0 {
		return 0 * time.Millisecond
	}

	// just in case backoff interval exceeds max interval
	backoffInterval := math.Min(float64(c.backoffInterval), float64(c.maxInterval))
	jitterInterval := rand.Int63n(int64(c.jitterInterval))

	return time.Duration(backoffInterval + float64(jitterInterval))
}

// exponentialBackoff implements intervaler using exponential interval.
type exponentialBackoff struct {
	// backoffInterval defines how long the next backoff will be compared to the previous one.
	backoffInterval time.Duration
	// jitterInterval defines the randomness additional value for interval.
	jitterInterval time.Duration
	// maxInterval defines the maximum interval allowed.
	maxInterval time.Duration
	// multiplier defines exponential factor.
	multiplier int64
}

// newExponentialBackoff creates an instance of exponentialBackoff with default values.
// See Constants for the default values.
func newExponentialBackoff() exponentialBackoff {
	return exponentialBackoff{
		backoffInterval: defaultBackoffInterval,
		jitterInterval:  defaultJitterInterval,
		maxInterval:     defaultMaxInterval,
		multiplier:      defaultMultiplier,
	}
}

// NextInterval returns the next order-th interval for backoff.
func (e exponentialBackoff) NextInterval(order int) time.Duration {
	if order <= 0 {
		return 0 * time.Millisecond
	}

	exponent := math.Pow(float64(e.multiplier), float64(order-1))
	backoffInterval := float64(e.backoffInterval) * exponent
	// prevent backoff to exceed max interval
	backoffInterval = math.Min(backoffInterval, float64(e.maxInterval))
	jitterInterval := rand.Int63n(int64(e.jitterInterval))

	return time.Duration(backoffInterval + float64(jitterInterval))
}

// noBackoff implements intervaler without any interval.
type noBackoff struct {
}

// newNoBackoff creates an instance of noBackoff
func newNoBackoff() noBackoff {
	return noBackoff{}
}

// NextInterval returns the next order-th interval for backoff.
func (n noBackoff) NextInterval(_ int) time.Duration {
	return 0
}
