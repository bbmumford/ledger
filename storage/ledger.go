/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
// Package ledger provides implementations of the LAD Ledger interface for
// storing and synchronizing peer discovery records.
//
// Two implementations are available:
//   - LocalDBLedger: Persistent storage using embedded libSQL (production default)
//   - MemoryLedger: In-memory storage for testing or ephemeral deployments
//
// The application chooses which one it constructs; this package has no
// selection setting and no replicated implementation.
package storage

import (
	"context"
	"sync"

	lad "github.com/bbmumford/ledger"
)

// subscriberQueueDepth is how many live records a subscriber's queue holds
// while its Stream goroutine waits on the caller.
const subscriberQueueDepth = 64

// subscriber is one Stream call's live-update queue, used by LocalDBLedger and
// MemoryLedger. broadcast is its producer and the Stream goroutine that
// registered it is its one consumer, through forward.
//
// ch is never closed. broadcast sends to subscribers it copied out from under
// the ledger's lock, so a close by the ledger could meet a send already past
// that lock. The end is done instead: end closes it once, offer checks it under
// mu before it sends, and forward selects on it, so no send meets a closed
// channel and nothing is queued after the consumer has seen the end.
type subscriber struct {
	id     int
	topics map[lad.Topic]struct{}
	ch     chan lad.Record

	mu     sync.Mutex // held by end and offer, so a send and the end never interleave
	ended  bool
	reason error // why the subscription ended, when the ledger or a full queue ended it
	done   chan struct{}
}

func newSubscriber(id int, topics map[lad.Topic]struct{}) *subscriber {
	return &subscriber{
		id:     id,
		topics: topics,
		ch:     make(chan lad.Record, subscriberQueueDepth),
		done:   make(chan struct{}),
	}
}

// end marks the subscription over and closes done; a second call does
// nothing. The Stream goroutine's unsubscribe calls it when it leaves, with
// no reason: its own caller ended it. After end returns, offer queues
// nothing, so forward's drain after done sees every record that was queued.
func (s *subscriber) end() {
	s.endFor(nil)
}

// endFor is end with the reason the ledger ended the subscription:
// lad.ErrLedgerClosed from Close, lad.ErrStreamOverflow from offer. The first
// end's reason stands.
func (s *subscriber) endFor(reason error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endLocked(reason)
}

func (s *subscriber) endLocked(reason error) {
	if !s.ended {
		s.ended = true
		s.reason = reason
		close(s.done)
	}
}

// endReason reports why the ledger ended the subscription, or nil when it
// has not, or when the Stream goroutine's own unsubscribe ended it.
func (s *subscriber) endReason() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reason
}

// offer queues rec unless the subscription has ended. A full queue ends the
// subscription with lad.ErrStreamOverflow instead of queuing rec: the
// consumer receives every record queued before rec, then the channel's close,
// and reads the overflow from its StreamEnd, so it streams again from its
// watermark and loses nothing. Neither a silent drop (the consumer would
// never learn of rec) nor a blocking send (every Append would wait on the
// slowest consumer, and an appender is a publish path its own callers wait
// on) keeps that.
func (s *subscriber) offer(rec lad.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	select {
	case s.ch <- rec:
	default:
		s.endLocked(lad.ErrStreamOverflow)
	}
}

// broadcast offers a record to every subscriber whose topic filter matches.
// subs is a copy taken under the ledger's lock, so a subscriber in it may have
// ended since; offer makes that a no-op.
func broadcast(subs []*subscriber, rec lad.Record) {
	for _, sub := range subs {
		// Filter by topic if subscriber has topic filter
		if len(sub.topics) > 0 {
			if _, ok := sub.topics[rec.Topic]; !ok {
				continue
			}
		}
		sub.offer(rec)
	}
}

// forward is a Stream goroutine's live phase: it hands sub's records to out,
// the channel the Stream call returned, until ctx ends or the subscription
// ends. At the end it hands over every record the queue still holds and then
// returns the reason the stream ended, which the Stream goroutine sets on its
// StreamEnd before its deferred close(out) reports the end to the consumer.
// Each hand-off waits for the consumer or for ctx, which is how a consumer
// that stops reading releases the goroutine.
func forward(ctx context.Context, sub *subscriber, out chan<- lad.Record) error {
	forwardRecords(ctx, sub, out)
	if reason := sub.endReason(); reason != nil {
		return reason
	}
	return ctx.Err()
}

func forwardRecords(ctx context.Context, sub *subscriber, out chan<- lad.Record) {
	for {
		select {
		case rec := <-sub.ch:
			if !handOff(ctx, out, rec) {
				return
			}
		case <-sub.done:
			for {
				select {
				case rec := <-sub.ch:
					if !handOff(ctx, out, rec) {
						return
					}
				default:
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// handOff sends rec on out, or reports false once ctx ends first.
func handOff(ctx context.Context, out chan<- lad.Record, rec lad.Record) bool {
	select {
	case out <- rec:
		return true
	case <-ctx.Done():
		return false
	}
}
