/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
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

	mu    sync.Mutex // held by end and offer, so a send and the end never interleave
	ended bool
	done  chan struct{}
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
// nothing. The ledger calls it when the Stream goroutine leaves (unsubscribe)
// and when the ledger closes. After end returns, offer queues nothing, so
// forward's drain after done sees every record that was queued.
func (s *subscriber) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.ended = true
		close(s.done)
	}
}

// offer queues rec unless the subscription has ended or its queue is full.
//
// TODO: a full queue drops rec for this subscriber with no signal to its
// consumer, which misses the record unless it streams again from a watermark;
// a full queue should be typed backpressure or a refusal the consumer sees,
// never a silent loss. The consumer outside this repository that applies the
// stream to a directory cache streams once and does not restart when the
// channel closes, so ending the subscription here would stop it, and blocking
// Append on a slow consumer would hold the caller's publish path. The choice
// needs that consumer changed with it.
func (s *subscriber) offer(rec lad.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	select {
	case s.ch <- rec:
	default:
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
// returns, and the Stream goroutine's deferred close(out) reports the end to
// the consumer. Each hand-off waits for the consumer or for ctx, which is how
// a consumer that stops reading releases the goroutine.
func forward(ctx context.Context, sub *subscriber, out chan<- lad.Record) {
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
