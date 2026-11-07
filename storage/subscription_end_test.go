/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// nullDriver answers every statement LocalDBLedger issues with success and
// every query with no rows. NewLocalDBLedger's column check finds no table and
// its schema statements succeed, Append's INSERT succeeds, and Stream's history
// query returns nothing, so these tests drive the live-update path alone: the
// subscriber queue between broadcast and the Stream goroutine.
type nullDriver struct{}

func (nullDriver) Open(string) (driver.Conn, error) { return nullConn{}, nil }

type nullConn struct{}

func (nullConn) Prepare(string) (driver.Stmt, error) { return nullStmt{}, nil }
func (nullConn) Close() error                        { return nil }
func (nullConn) Begin() (driver.Tx, error)           { return nullTx{}, nil }

type nullTx struct{}

func (nullTx) Commit() error   { return nil }
func (nullTx) Rollback() error { return nil }

type nullStmt struct{}

func (nullStmt) Close() error  { return nil }
func (nullStmt) NumInput() int { return -1 }
func (nullStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (nullStmt) Query([]driver.Value) (driver.Rows, error) { return nullRows{}, nil }

type nullRows struct{}

func (nullRows) Columns() []string         { return nil }
func (nullRows) Close() error              { return nil }
func (nullRows) Next([]driver.Value) error { return io.EOF }

var registerNullDriver sync.Once

func openNullLedger(t *testing.T) *LocalDBLedger {
	t.Helper()
	registerNullDriver.Do(func() { sql.Register("ledger-storage-null", nullDriver{}) })
	db, err := sql.Open("ledger-storage-null", "")
	if err != nil {
		t.Fatalf("open null database: %v", err)
	}
	l, err := NewLocalDBLedger(db, LedgerConfig{})
	if err != nil {
		t.Fatalf("NewLocalDBLedger: %v", err)
	}
	t.Cleanup(func() {
		_ = l.Close()
		_ = db.Close()
	})
	return l
}

// waitFor polls cond every millisecond for up to five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func localSubscribers(l *LocalDBLedger) []*subscriber {
	l.mu.RLock()
	defer l.mu.RUnlock()
	subs := make([]*subscriber, 0, len(l.subscribers))
	for _, sub := range l.subscribers {
		subs = append(subs, sub)
	}
	return subs
}

func memorySubscribers(l *MemoryLedger) []*subscriber {
	l.mu.RLock()
	defer l.mu.RUnlock()
	subs := make([]*subscriber, 0, len(l.subscribers))
	for _, sub := range l.subscribers {
		subs = append(subs, sub)
	}
	return subs
}

// readAll reads out until it closes, failing the test if it stays open for
// five seconds.
func readAll(t *testing.T, out <-chan lad.Record) []lad.Record {
	t.Helper()
	var got []lad.Record
	timeout := time.After(5 * time.Second)
	for {
		select {
		case rec, ok := <-out:
			if !ok {
				return got
			}
			got = append(got, rec)
		case <-timeout:
			t.Fatalf("stream still open after 5 s, %d records read", len(got))
		}
	}
}

// appendUntil appends records on one topic until stop closes, so broadcast
// is sending to the subscriber queues throughout the caller's run.
func appendUntil(l lad.Ledger, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for seq := uint64(1); ; seq++ {
		select {
		case <-stop:
			return
		default:
		}
		_ = l.Append(context.Background(), lad.Record{Topic: lad.TopicMember, Seq: seq, NodeID: "n"})
	}
}

// A Stream whose caller cancels ends its subscription while Append is
// broadcasting to it. broadcast sends to a subscriber it copied out from
// under the ledger's lock, so the end must leave the queue open and make the
// send a no-op; under -race a close of the queue shows as a race between the
// close and the send, and without -race as a send on a closed channel.
//
// The appender outpaces the reader, so a stream can also end by overflowing
// its queue before the cancel reaches it, an end under the same broadcast;
// either way the stream reports which end it met.
func TestLocalDBLedger_AStreamEndingWhileAppendsRunNeverMeetsAClosedQueue(t *testing.T) {
	l := openNullLedger(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go appendUntil(l, stop, &wg)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		out, end, err := l.StreamWithEnd(ctx, nil, nil)
		if err != nil {
			cancel()
			t.Fatalf("StreamWithEnd: %v", err)
		}
		first := make(chan struct{})
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			n := 0
			for range out {
				if n++; n == 1 {
					close(first)
				}
			}
			if n == 0 {
				close(first)
			}
		}()
		<-first
		cancel()
		<-drained
		if n := len(localSubscribers(l)); n != 0 {
			t.Fatalf("iteration %d: %d subscribers left after the stream closed, want 0", i, n)
		}
		if why := end.Err(); !errors.Is(why, context.Canceled) && !errors.Is(why, lad.ErrStreamOverflow) {
			t.Fatalf("iteration %d: the stream ended with %v, want the cancel or the overflow", i, why)
		}
	}
}

// Close ends every subscription while Append is broadcasting to them: the
// same send against an end as above, from the ledger's side.
func TestLocalDBLedger_ClosingTheLedgerWhileAppendsRunNeverMeetsAClosedQueue(t *testing.T) {
	for i := 0; i < 50; i++ {
		l := openNullLedger(t)
		outs := make([]<-chan lad.Record, 4)
		for j := range outs {
			out, err := l.Stream(context.Background(), nil, nil)
			if err != nil {
				t.Fatalf("Stream: %v", err)
			}
			outs[j] = out
		}
		waitFor(t, "four subscriptions", func() bool { return len(localSubscribers(l)) == 4 })

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go appendUntil(l, stop, &wg)
		var readers sync.WaitGroup
		for _, out := range outs {
			readers.Add(1)
			go func(out <-chan lad.Record) {
				defer readers.Done()
				for range out {
				}
			}(out)
		}
		time.Sleep(time.Millisecond)
		_ = l.Close()
		readers.Wait()
		close(stop)
		wg.Wait()
	}
}

// A Stream whose history read finishes after Close registers no subscription
// and closes its channel. Close drops the subscriber map, so registering into
// it would write a nil map.
func TestLocalDBLedger_AStreamStartedAfterCloseEndsWithoutASubscription(t *testing.T) {
	l := openNullLedger(t)
	_ = l.Close()

	out, err := l.Stream(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := readAll(t, out); len(got) != 0 {
		t.Fatalf("a stream started after Close delivered %d records, want 0", len(got))
	}
	if n := len(localSubscribers(l)); n != 0 {
		t.Fatalf("%d subscribers registered after Close, want 0", n)
	}
}

// fillStream queues records seq 1..129 through appendRec, which must return
// only once its record is in sub's queue, while the consumer of out reads
// nothing. It paces them so that none meets a full queue: 64 records fill out,
// the 65th is held by the Stream goroutine waiting on out, and the last 64
// fill the subscriber's queue.
func fillStream(t *testing.T, appendRec func(seq uint64), out <-chan lad.Record, sub *subscriber) {
	t.Helper()
	seq := uint64(1)
	for ; seq <= 64; seq++ {
		appendRec(seq)
	}
	waitFor(t, "64 records in the stream's channel", func() bool { return len(out) == 64 && len(sub.ch) == 0 })
	appendRec(seq)
	seq++
	waitFor(t, "the Stream goroutine to take record 65", func() bool { return len(sub.ch) == 0 })
	for ; seq <= 129; seq++ {
		appendRec(seq)
	}
	waitFor(t, "64 records in the subscriber's queue", func() bool { return len(sub.ch) == 64 })
}

func checkAllSeqs(t *testing.T, got []lad.Record, n int) {
	t.Helper()
	seen := make(map[uint64]bool, len(got))
	for _, rec := range got {
		seen[rec.Seq] = true
	}
	if len(got) != n || len(seen) != n {
		t.Fatalf("the stream delivered %d records (%d distinct) before it ended, want all %d queued at Close", len(got), len(seen), n)
	}
	for seq := uint64(1); seq <= uint64(n); seq++ {
		if !seen[seq] {
			t.Fatalf("record %d queued at Close was not delivered", seq)
		}
	}
}

// Close with records still queued for a Stream: the stream hands its
// consumer every one of them before its channel closes.
func TestLocalDBLedger_ClosingTheLedgerHandsOverWhatTheQueueHolds(t *testing.T) {
	l := openNullLedger(t)
	out, err := l.Stream(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	waitFor(t, "the stream's subscription", func() bool { return len(localSubscribers(l)) == 1 })
	sub := localSubscribers(l)[0]

	fillStream(t, func(seq uint64) {
		if err := l.Append(context.Background(), lad.Record{Topic: lad.TopicMember, Seq: seq, NodeID: "n"}); err != nil {
			t.Fatalf("Append %d: %v", seq, err)
		}
	}, out, sub)

	_ = l.Close()
	checkAllSeqs(t, readAll(t, out), 129)
}

// The same for MemoryLedger. Its Append hands each record to broadcast on a
// goroutine of its own, so the test calls broadcast itself, in order, to pace
// the queue as fillStream needs.
func TestMemoryLedger_ClosingTheLedgerHandsOverWhatTheQueueHolds(t *testing.T) {
	l := NewMemoryLedger(MemoryLedgerConfig{})
	out, err := l.Stream(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	waitFor(t, "the stream's subscription", func() bool { return len(memorySubscribers(l)) == 1 })
	sub := memorySubscribers(l)[0]

	fillStream(t, func(seq uint64) {
		broadcast(memorySubscribers(l), lad.Record{Topic: lad.TopicMember, Seq: seq, NodeID: "n"})
	}, out, sub)

	_ = l.Close()
	checkAllSeqs(t, readAll(t, out), 129)
}

// end is idempotent, closes done, and an offer after it queues nothing, so
// forward's drain after done has every record there will be.
func TestSubscriber_AnEndedSubscriptionQueuesNothing(t *testing.T) {
	sub := newSubscriber(0, nil)
	sub.offer(lad.Record{Topic: lad.TopicMember, Seq: 1})
	sub.end()
	sub.end()
	sub.offer(lad.Record{Topic: lad.TopicMember, Seq: 2})

	select {
	case <-sub.done:
	default:
		t.Fatal("done is still open after end")
	}
	if n := len(sub.ch); n != 1 {
		t.Fatalf("queue holds %d records, want only the one offered before end", n)
	}
	if rec := <-sub.ch; rec.Seq != 1 {
		t.Fatalf("queue holds record %d, want 1", rec.Seq)
	}
}

// Close ends only the subscribers it finds, so neither ledger registers one
// after it: LocalDBLedger has dropped its map, and a MemoryLedger subscriber
// registered then would never end.
func TestLedgers_RegisterNoSubscriberAfterClose(t *testing.T) {
	local := openNullLedger(t)
	_ = local.Close()
	if id := local.subscribe(nil); id != -1 {
		t.Fatalf("LocalDBLedger.subscribe after Close returned id %d, want -1", id)
	}

	mem := NewMemoryLedger(MemoryLedgerConfig{})
	_ = mem.Close()
	if sub := mem.subscribe(nil); sub != nil {
		t.Fatalf("MemoryLedger.subscribe after Close registered subscriber %d, want none", sub.id)
	}
	if n := len(memorySubscribers(mem)); n != 0 {
		t.Fatalf("%d MemoryLedger subscribers after Close, want 0", n)
	}
}

// A consumer that stops reading and cancels its context releases the Stream
// goroutine even while it waits to hand over a record, and the subscription
// is removed.
func TestLocalDBLedger_ACancelledConsumerThatStopsReadingReleasesTheStream(t *testing.T) {
	l := openNullLedger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, err := l.Stream(ctx, nil, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	waitFor(t, "the stream's subscription", func() bool { return len(localSubscribers(l)) == 1 })
	sub := localSubscribers(l)[0]

	fillStream(t, func(seq uint64) {
		if err := l.Append(context.Background(), lad.Record{Topic: lad.TopicMember, Seq: seq, NodeID: "n"}); err != nil {
			t.Fatalf("Append %d: %v", seq, err)
		}
	}, out, sub)

	cancel()
	waitFor(t, "the Stream goroutine to leave", func() bool { return len(localSubscribers(l)) == 0 })
}

// A Stream whose caller cancels with nothing queued leaves its live phase,
// closes its channel and ends its subscription, so a broadcast still holding
// the subscriber from an earlier copy queues nothing for it.
func TestLedgers_AStreamWhoseCallerCancelsWithNothingQueuedEnds(t *testing.T) {
	check := func(t *testing.T, stream func(context.Context) (<-chan lad.Record, error), subs func() []*subscriber) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out, err := stream(ctx)
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		waitFor(t, "the stream's subscription", func() bool { return len(subs()) == 1 })
		sub := subs()[0]

		cancel()
		if got := readAll(t, out); len(got) != 0 {
			t.Fatalf("a cancelled stream delivered %d records, want 0", len(got))
		}
		if n := len(subs()); n != 0 {
			t.Fatalf("%d subscribers left after the stream closed, want 0", n)
		}
		select {
		case <-sub.done:
		default:
			t.Fatal("the removed subscription was not ended")
		}
		broadcast([]*subscriber{sub}, lad.Record{Topic: lad.TopicMember, Seq: 1})
		if n := len(sub.ch); n != 0 {
			t.Fatalf("a broadcast after the stream ended queued %d records, want 0", n)
		}
	}
	t.Run("LocalDBLedger", func(t *testing.T) {
		l := openNullLedger(t)
		check(t, func(ctx context.Context) (<-chan lad.Record, error) { return l.Stream(ctx, nil, nil) },
			func() []*subscriber { return localSubscribers(l) })
	})
	t.Run("MemoryLedger", func(t *testing.T) {
		l := NewMemoryLedger(MemoryLedgerConfig{})
		defer l.Close()
		check(t, func(ctx context.Context) (<-chan lad.Record, error) { return l.Stream(ctx, nil, nil) },
			func() []*subscriber { return memorySubscribers(l) })
	})
}
