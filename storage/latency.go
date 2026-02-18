// Package storage - latency.go wraps a Backend with simulated network latency.
// This is useful for benchmarking and demonstrating the value of
// chunk-aware fetching and page caching against realistic S3 latencies.
package storage

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"
)

// LatencyBackend wraps another Backend and adds simulated network latency
// to every operation. This simulates real S3 conditions:
//   - GET: ~50-100ms first byte latency
//   - PUT: ~80-200ms
//   - Throughput: ~100MB/s per connection
type LatencyBackend struct {
	inner      Backend
	minLatency time.Duration
	maxLatency time.Duration

	// Stats
	TotalLatency atomic.Int64 // total simulated latency in microseconds
	OpCount      atomic.Int64
}

// NewLatencyBackend wraps a backend with S3-like latency (50-100ms per op).
func NewLatencyBackend(inner Backend) *LatencyBackend {
	return &LatencyBackend{
		inner:      inner,
		minLatency: 50 * time.Millisecond,
		maxLatency: 100 * time.Millisecond,
	}
}

// NewLatencyBackendCustom wraps with custom latency range.
func NewLatencyBackendCustom(inner Backend, minLatency, maxLatency time.Duration) *LatencyBackend {
	return &LatencyBackend{
		inner:      inner,
		minLatency: minLatency,
		maxLatency: maxLatency,
	}
}

func (b *LatencyBackend) simulateLatency() {
	jitter := time.Duration(rand.Int63n(int64(b.maxLatency - b.minLatency)))
	delay := b.minLatency + jitter
	time.Sleep(delay)
	b.TotalLatency.Add(int64(delay / time.Microsecond))
	b.OpCount.Add(1)
}

func (b *LatencyBackend) Get(ctx context.Context, key string) ([]byte, error) {
	b.simulateLatency()
	return b.inner.Get(ctx, key)
}

func (b *LatencyBackend) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	b.simulateLatency()
	return b.inner.GetRange(ctx, key, offset, length)
}

func (b *LatencyBackend) Put(ctx context.Context, key string, data []byte) error {
	b.simulateLatency()
	return b.inner.Put(ctx, key, data)
}

func (b *LatencyBackend) Delete(ctx context.Context, key string) error {
	b.simulateLatency()
	return b.inner.Delete(ctx, key)
}

func (b *LatencyBackend) List(ctx context.Context, prefix string) ([]string, error) {
	b.simulateLatency()
	return b.inner.List(ctx, prefix)
}

func (b *LatencyBackend) Exists(ctx context.Context, key string) (bool, error) {
	b.simulateLatency()
	return b.inner.Exists(ctx, key)
}

// LatencyStats returns latency statistics.
func (b *LatencyBackend) LatencyStats() (ops int64, totalLatencyMs float64, avgLatencyMs float64) {
	ops = b.OpCount.Load()
	totalUs := b.TotalLatency.Load()
	totalLatencyMs = float64(totalUs) / 1000.0
	if ops > 0 {
		avgLatencyMs = totalLatencyMs / float64(ops)
	}
	return
}
