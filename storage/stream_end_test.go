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
	"strings"
	"sync"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// A record that meets a full subscriber queue ends the stream with
// ErrStreamOverflow: the consumer receives every record queued before it,
// then the close, and the reason; the overflowing record is not delivered,
// so the consumer knows to stream again.
func TestLocalDBLedger_AFullQueueEndsTheStreamWithOverflow(t *testing.T) {
	l := openNullLedger(t)
	out, end, err := l.StreamWithEnd(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("StreamWithEnd: %v", err)
	}
	waitFor(t, "the stream's subscription", func() bool { return len(localSubscribers(l)) == 1 })
	sub := localSubscribers(l)[0]
	appendSeq := func(seq uint64) {
		if err := l.Append(context.Background(), lad.Record{Topic: lad.TopicMember, Seq: seq, NodeID: "n"}); err != nil {
			t.Fatalf("Append %d: %v", seq, err)
		}
	}
	fillStream(t, appendSeq, out, sub)
	appendSeq(130)

	checkAllSeqs(t, readAll(t, out), 129)
	if why := end.Err(); !errors.Is(why, lad.ErrStreamOverflow) {
		t.Fatalf("the stream ended with %v, want ErrStreamOverflow", why)
	}
	waitFor(t, "the overflowed subscription's removal", func() bool { return len(localSubscribers(l)) == 0 })
}

// The same for MemoryLedger, pacing the queue through broadcast as
// TestMemoryLedger_ClosingTheLedgerHandsOverWhatTheQueueHolds does.
func TestMemoryLedger_AFullQueueEndsTheStreamWithOverflow(t *testing.T) {
	l := NewMemoryLedger(MemoryLedgerConfig{})
	defer l.Close()
	out, end, err := l.StreamWithEnd(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("StreamWithEnd: %v", err)
	}
	sub := memorySubscribers(l)[0]
	offer := func(seq uint64) {
		broadcast(memorySubscribers(l), lad.Record{Topic: lad.TopicMember, Seq: seq, NodeID: "n"})
	}
	fillStream(t, offer, out, sub)
	offer(130)

	checkAllSeqs(t, readAll(t, out), 129)
	if why := end.Err(); !errors.Is(why, lad.ErrStreamOverflow) {
		t.Fatalf("the stream ended with %v, want ErrStreamOverflow", why)
	}
}

// Close ends a stream with ErrLedgerClosed, and a closed ledger refuses or
// ends a new stream with it, so a consumer can tell the end it must not
// restart from the ones it must.
func TestLedgers_CloseEndsAStreamWithLedgerClosed(t *testing.T) {
	t.Run("LocalDBLedger", func(t *testing.T) {
		l := openNullLedger(t)
		out, end, err := l.StreamWithEnd(context.Background(), nil, nil)
		if err != nil {
			t.Fatalf("StreamWithEnd: %v", err)
		}
		waitFor(t, "the stream's subscription", func() bool { return len(localSubscribers(l)) == 1 })
		_ = l.Close()
		readAll(t, out)
		if why := end.Err(); !errors.Is(why, lad.ErrLedgerClosed) {
			t.Fatalf("the stream ended with %v, want ErrLedgerClosed", why)
		}

		out, end, err = l.StreamWithEnd(context.Background(), nil, nil)
		if err != nil {
			t.Fatalf("StreamWithEnd after Close: %v", err)
		}
		readAll(t, out)
		if why := end.Err(); !errors.Is(why, lad.ErrLedgerClosed) {
			t.Fatalf("a stream started after Close ended with %v, want ErrLedgerClosed", why)
		}
	})
	t.Run("MemoryLedger", func(t *testing.T) {
		l := NewMemoryLedger(MemoryLedgerConfig{})
		out, end, err := l.StreamWithEnd(context.Background(), nil, nil)
		if err != nil {
			t.Fatalf("StreamWithEnd: %v", err)
		}
		_ = l.Close()
		readAll(t, out)
		if why := end.Err(); !errors.Is(why, lad.ErrLedgerClosed) {
			t.Fatalf("the stream ended with %v, want ErrLedgerClosed", why)
		}
		if _, err := l.Stream(context.Background(), nil, nil); !errors.Is(err, lad.ErrLedgerClosed) {
			t.Fatalf("Stream after Close = %v, want ErrLedgerClosed", err)
		}
	})
}

// A caller's cancel ends the stream with the context's error.
func TestLedgers_ACancelEndsTheStreamWithTheContextError(t *testing.T) {
	check := func(t *testing.T, stream func(context.Context) (<-chan lad.Record, *lad.StreamEnd, error)) {
		ctx, cancel := context.WithCancel(context.Background())
		out, end, err := stream(ctx)
		if err != nil {
			t.Fatalf("StreamWithEnd: %v", err)
		}
		cancel()
		readAll(t, out)
		if why := end.Err(); !errors.Is(why, context.Canceled) {
			t.Fatalf("the stream ended with %v, want context.Canceled", why)
		}
	}
	t.Run("LocalDBLedger", func(t *testing.T) {
		l := openNullLedger(t)
		check(t, func(ctx context.Context) (<-chan lad.Record, *lad.StreamEnd, error) {
			return l.StreamWithEnd(ctx, nil, nil)
		})
	})
	t.Run("MemoryLedger", func(t *testing.T) {
		l := NewMemoryLedger(MemoryLedgerConfig{})
		defer l.Close()
		check(t, func(ctx context.Context) (<-chan lad.Record, *lad.StreamEnd, error) {
			return l.StreamWithEnd(ctx, nil, nil)
		})
	})
}

// gatedDriver is nullDriver whose Stream history query (the one SELECT with
// "WHERE 1=1") signals entered and then waits for release before it returns
// no rows, so a test can commit a record while the history read is under way
// and after the rows it returns were read.
type gatedDriver struct{}

var gates sync.Map // DSN -> *historyGate

type historyGate struct {
	entered chan struct{}
	release chan struct{}
	err     error // returned by the history query when set
}

func (gatedDriver) Open(dsn string) (driver.Conn, error) {
	g, _ := gates.Load(dsn)
	return gatedConn{gate: g.(*historyGate)}, nil
}

type gatedConn struct{ gate *historyGate }

func (c gatedConn) Prepare(query string) (driver.Stmt, error) {
	return gatedStmt{gate: c.gate, query: query}, nil
}
func (gatedConn) Close() error              { return nil }
func (gatedConn) Begin() (driver.Tx, error) { return nullTx{}, nil }

type gatedStmt struct {
	gate  *historyGate
	query string
}

func (gatedStmt) Close() error  { return nil }
func (gatedStmt) NumInput() int { return -1 }
func (gatedStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (s gatedStmt) Query([]driver.Value) (driver.Rows, error) {
	if strings.Contains(s.query, "WHERE 1=1") {
		close(s.gate.entered)
		<-s.gate.release
		if s.gate.err != nil {
			return nil, s.gate.err
		}
	}
	return nullRows{}, nil
}

var registerGatedDriver sync.Once

func openGatedLedger(t *testing.T, gate *historyGate) *LocalDBLedger {
	t.Helper()
	registerGatedDriver.Do(func() { sql.Register("ledger-storage-gated", gatedDriver{}) })
	dsn := t.Name()
	gates.Store(dsn, gate)
	db, err := sql.Open("ledger-storage-gated", dsn)
	if err != nil {
		t.Fatalf("open gated database: %v", err)
	}
	l, err := NewLocalDBLedger(db, LedgerConfig{})
	if err != nil {
		t.Fatalf("NewLocalDBLedger: %v", err)
	}
	t.Cleanup(func() {
		_ = l.Close()
		_ = db.Close()
		gates.Delete(dsn)
	})
	return l
}

// A record committed while the history query reads, after the rows it
// returns, reaches the stream through the live queue: the subscription is
// registered before the query runs.
func TestLocalDBLedger_ARecordCommittedDuringTheHistoryReadReachesTheStream(t *testing.T) {
	gate := &historyGate{entered: make(chan struct{}), release: make(chan struct{})}
	l := openGatedLedger(t, gate)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, _, err := l.StreamWithEnd(ctx, nil, nil)
	if err != nil {
		t.Fatalf("StreamWithEnd: %v", err)
	}
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the history query never ran")
	}
	if err := l.Append(context.Background(), lad.Record{Topic: lad.TopicMember, Seq: 7, NodeID: "n"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	close(gate.release)

	select {
	case rec, ok := <-out:
		if !ok || rec.Seq != 7 {
			t.Fatalf("the stream delivered %+v (open %v), want record 7", rec, ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the record committed during the history read never reached the stream")
	}
}

// A failed history query ends the stream with that error, not a silent close.
func TestLocalDBLedger_AFailedHistoryReadEndsTheStreamWithItsError(t *testing.T) {
	queryErr := errors.New("history query failed")
	gate := &historyGate{entered: make(chan struct{}), release: make(chan struct{}), err: queryErr}
	close(gate.release)
	l := openGatedLedger(t, gate)
	out, end, err := l.StreamWithEnd(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("StreamWithEnd: %v", err)
	}
	readAll(t, out)
	if why := end.Err(); !errors.Is(why, queryErr) {
		t.Fatalf("the stream ended with %v, want the query's error", why)
	}
	if n := len(localSubscribers(l)); n != 0 {
		t.Fatalf("%d subscribers left after the failed stream, want 0", n)
	}
}

// MemoryLedger registers the subscription and copies its history under one
// hold of its lock, so records appended while streams start each arrive
// exactly once. Fewer records than the queue holds are appended, so no
// stream can overflow however slowly it is read.
func TestMemoryLedger_RecordsAppendedWhileAStreamStartsArriveOnce(t *testing.T) {
	const n = 60
	for i := 0; i < 50; i++ {
		l := NewMemoryLedger(MemoryLedgerConfig{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for seq := uint64(1); seq <= n; seq++ {
				_ = l.Append(context.Background(), lad.Record{Topic: lad.TopicMember, Seq: seq, NodeID: "n"})
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		out, _, err := l.StreamWithEnd(ctx, nil, nil)
		if err != nil {
			cancel()
			t.Fatalf("StreamWithEnd: %v", err)
		}
		<-done
		counts := make(map[uint64]int, n)
		timeout := time.After(5 * time.Second)
		for len(counts) < n {
			select {
			case rec := <-out:
				counts[rec.Seq]++
			case <-timeout:
				cancel()
				t.Fatalf("iteration %d: %d of %d records arrived", i, len(counts), n)
			}
		}
		// Anything still to come is a duplicate.
		select {
		case rec := <-out:
			counts[rec.Seq]++
		case <-time.After(20 * time.Millisecond):
		}
		cancel()
		_ = l.Close()
		for seq, c := range counts {
			if c != 1 {
				t.Fatalf("iteration %d: record %d arrived %d times, want once", i, seq, c)
			}
		}
	}
}
