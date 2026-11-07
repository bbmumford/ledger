/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrStreamOverflow ends a stream whose consumer fell behind: a record
	// arrived while the stream's live queue was full. Every record queued
	// before it was handed over; it and every later record were not, so the
	// consumer streams again from a watermark no later than the first record
	// it has not applied to receive them.
	ErrStreamOverflow = errors.New("ledger: stream ended: its live queue overflowed")

	// ErrLedgerClosed ends a stream because its ledger closed, and refuses a
	// stream asked of a closed ledger. Streaming again cannot succeed.
	ErrLedgerClosed = errors.New("ledger: closed")
)

// StreamEnd carries why a stream's record channel closed. The ledger sets it
// before it closes the channel, so a consumer that has received the close
// reads the reason through Err. The reason is ErrStreamOverflow,
// ErrLedgerClosed, the caller's context error, or an error from reading the
// stream's history.
type StreamEnd struct {
	mu  sync.Mutex
	err error
}

// Set records err as the reason the stream ended. The first non-nil reason
// stands; a later call does nothing.
func (e *StreamEnd) Set(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err == nil {
		e.err = err
	}
}

// Err reports the reason the stream ended, or nil while it has not ended.
func (e *StreamEnd) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

// EndReportingLedger is a Ledger whose streams report why they ended.
// StreamWithEnd delivers what Stream delivers, and its StreamEnd holds the
// reason once the channel has closed. A consumer that must not lose a
// record (a directory cache kept from the stream) reads the reason and
// streams again after ErrStreamOverflow or a history error, and stops after
// ErrLedgerClosed or its own context's end.
//
// A record appended while a stream starts can arrive twice, once from the
// history read and once from the live queue, so the consumer applies a
// repeated record harmlessly.
type EndReportingLedger interface {
	Ledger
	StreamWithEnd(ctx context.Context, from CausalWatermark, topics []Topic) (<-chan Record, *StreamEnd, error)
}
