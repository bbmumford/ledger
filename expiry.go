/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"context"
	"time"
)

// ExpirySource is implemented by any cache that can prune TTL-expired records.
type ExpirySource interface {
	PruneExpired() (removed int, err error)
}

// ExpiryWorker runs a background goroutine that prunes expired records.
type ExpiryWorker struct {
	cache    ExpirySource
	interval time.Duration
}

// DefaultExpiryInterval is the default period between expiry sweeps.
const DefaultExpiryInterval = 60 * time.Second

// NewExpiryWorker creates an ExpiryWorker that calls cache.PruneExpired()
// on the given interval. If interval is zero, DefaultExpiryInterval is used.
func NewExpiryWorker(cache ExpirySource, interval time.Duration) *ExpiryWorker {
	if interval <= 0 {
		interval = DefaultExpiryInterval
	}
	return &ExpiryWorker{
		cache:    cache,
		interval: interval,
	}
}

// PruneExpired delegates to the underlying cache's PruneExpired method.
// Useful for on-demand pruning outside the background ticker loop.
func (w *ExpiryWorker) PruneExpired() (int, error) {
	return w.cache.PruneExpired()
}

// Run starts the expiry loop and blocks until ctx is cancelled.
func (w *ExpiryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.cache.PruneExpired()
		}
	}
}
