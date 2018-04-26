package mysql

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewConstantBackoff_Success(t *testing.T) {
	backoff := newConstantBackoff()
	assert.NotNil(t, backoff)
	assert.Equal(t, 500*time.Millisecond, backoff.backoffInterval)
	assert.Equal(t, 200*time.Millisecond, backoff.jitterInterval)
	assert.Equal(t, 3*time.Second, backoff.maxInterval)
}

func TestConstantBackoff_NextInterval(t *testing.T) {
	backoff := newConstantBackoff()

	// order less than 0
	nextInterval := backoff.NextInterval(-1)
	assert.True(t, nextInterval == 0*time.Millisecond)

	// order 0
	nextInterval = backoff.NextInterval(0)
	assert.True(t, nextInterval == 0*time.Millisecond)

	// order more than 0
	nextInterval = backoff.NextInterval(1)
	assert.True(t, nextInterval >= backoff.backoffInterval)
	assert.True(t, nextInterval < backoff.backoffInterval+backoff.jitterInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)

	nextInterval = backoff.NextInterval(2)
	assert.True(t, nextInterval >= backoff.backoffInterval)
	assert.True(t, nextInterval < backoff.backoffInterval+backoff.jitterInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)

	nextInterval = backoff.NextInterval(3)
	assert.True(t, nextInterval >= backoff.backoffInterval)
	assert.True(t, nextInterval < backoff.backoffInterval+backoff.jitterInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)

	nextInterval = backoff.NextInterval(4)
	assert.True(t, nextInterval >= backoff.backoffInterval)
	assert.True(t, nextInterval < backoff.backoffInterval+backoff.jitterInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)

	nextInterval = backoff.NextInterval(5)
	assert.True(t, nextInterval >= backoff.backoffInterval)
	assert.True(t, nextInterval < backoff.backoffInterval+backoff.jitterInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)
}

func TestNewExponentialBackoff_Success(t *testing.T) {
	backoff := newExponentialBackoff()
	assert.NotNil(t, backoff)
	assert.Equal(t, 500*time.Millisecond, backoff.backoffInterval)
	assert.Equal(t, 200*time.Millisecond, backoff.jitterInterval)
	assert.Equal(t, 3*time.Second, backoff.maxInterval)
	assert.Equal(t, int64(2), backoff.multiplier)
}

func TestExponentialBackoff_NextInterval(t *testing.T) {
	backoff := newExponentialBackoff()

	// order less than 0
	nextInterval := backoff.NextInterval(-1)
	assert.True(t, nextInterval == 0*time.Millisecond)

	// order 0
	nextInterval = backoff.NextInterval(0)
	assert.True(t, nextInterval == 0*time.Millisecond)

	// order 1
	nextInterval = backoff.NextInterval(1)
	assert.True(t, nextInterval >= backoff.backoffInterval)
	assert.True(t, nextInterval < backoff.backoffInterval+backoff.jitterInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)

	// order 2
	nextInterval = backoff.NextInterval(2)
	backoffInterval := time.Duration(math.Pow(float64(backoff.multiplier), 1)) * backoff.backoffInterval
	assert.True(t, nextInterval >= backoffInterval)
	assert.True(t, nextInterval < backoffInterval+backoff.jitterInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)

	// order 3
	nextInterval = backoff.NextInterval(3)
	backoffInterval = time.Duration(math.Pow(float64(backoff.multiplier), 2)) * backoff.backoffInterval
	assert.True(t, nextInterval >= backoffInterval)
	assert.True(t, nextInterval < backoffInterval+backoff.jitterInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)

	// order 4
	// it exceeds max interval
	nextInterval = backoff.NextInterval(4)
	backoffInterval = time.Duration(math.Pow(float64(backoff.multiplier), 3)) * backoff.backoffInterval
	assert.True(t, nextInterval <= backoffInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)

	// order 5
	// it exceeds max interval
	nextInterval = backoff.NextInterval(5)
	backoffInterval = time.Duration(math.Pow(float64(backoff.multiplier), 4)) * backoff.backoffInterval
	assert.True(t, nextInterval <= backoffInterval)
	assert.True(t, nextInterval <= backoff.maxInterval+backoff.jitterInterval)
}

func TestNewNoBackoff_Success(t *testing.T) {
	backoff := newNoBackoff()
	assert.NotNil(t, backoff)
}

func TestNoBackoff_NextInterval(t *testing.T) {
	backoff := newNoBackoff()

	// order less than 0
	nextInterval := backoff.NextInterval(-1)
	assert.True(t, nextInterval == 0*time.Millisecond)

	// order 0
	nextInterval = backoff.NextInterval(0)
	assert.True(t, nextInterval == 0*time.Millisecond)

	// order more than 0
	nextInterval = backoff.NextInterval(1)
	assert.True(t, nextInterval == 0*time.Millisecond)

	nextInterval = backoff.NextInterval(2)
	assert.True(t, nextInterval == 0*time.Millisecond)

	nextInterval = backoff.NextInterval(3)
	assert.True(t, nextInterval == 0*time.Millisecond)

	nextInterval = backoff.NextInterval(4)
	assert.True(t, nextInterval == 0*time.Millisecond)

	nextInterval = backoff.NextInterval(5)
	assert.True(t, nextInterval == 0*time.Millisecond)
}
