/*
 * Copyright (c) 2026 ORBTR Pty Ltd. All Rights Reserved.
 */

package storage

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// Appends on one topic from several goroutines at once each take their own
// Seq: the topic's head is read and the record inserted as one step, so no two
// appends read the same head and none is refused by the primary key.
func TestLocalDBLedger_ConcurrentAppendsEachTakeTheirOwnSeq(t *testing.T) {
	l, _ := openSQLiteLedger(t)
	const n = 16
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- l.Append(context.Background(), lad.Record{Topic: lad.TopicRole, NodeID: "node-1", Body: json.RawMessage(`{}`)})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Append: %v", err)
		}
	}
	head, err := l.Head(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if head[lad.TopicRole] != n {
		t.Fatalf("the topic's head is %d after %d appends", head[lad.TopicRole], n)
	}
	seen := map[uint64]bool{}
	for _, rec := range history(t, l) {
		if seen[rec.Seq] {
			t.Errorf("Seq %d was stored twice", rec.Seq)
		}
		seen[rec.Seq] = true
	}
	if len(seen) != n {
		t.Fatalf("%d distinct Seq values stored, want %d", len(seen), n)
	}
}

// Append and BatchAppend number their records through their own transaction:
// on a database that hands out one connection (an in-memory database), neither
// waits on the connection its transaction holds.
func TestLocalDBLedger_ABatchNumbersInsideItsTransaction(t *testing.T) {
	db := openSQLiteFile(t)
	db.SetMaxOpenConns(1)
	l, err := NewLocalDBLedger(db, LedgerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := l.Append(ctx, lad.Record{Topic: lad.TopicRole, NodeID: "node-1", Body: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Append on one connection: %v", err)
	}
	if err := l.BatchAppend(ctx, []lad.Record{
		{Topic: lad.TopicRole, NodeID: "node-2", Body: json.RawMessage(`{}`)},
		{Topic: lad.TopicMember, NodeID: "node-2", Body: json.RawMessage(`{}`)},
	}); err != nil {
		t.Fatalf("BatchAppend on one connection: %v", err)
	}
	head, err := l.Head(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if head[lad.TopicRole] != 2 || head[lad.TopicMember] != 1 {
		t.Fatalf("heads after the batch: %v, want role 2 and member 1", head)
	}
}

// eachLedger runs fn over both ledgers.
func eachLedger(t *testing.T, fn func(t *testing.T, l lad.Ledger)) {
	t.Helper()
	t.Run("local", func(t *testing.T) {
		l, _ := openSQLiteLedger(t)
		fn(t, l)
	})
	t.Run("memory", func(t *testing.T) {
		l := NewMemoryLedger(MemoryLedgerConfig{})
		t.Cleanup(func() { _ = l.Close() })
		fn(t, l)
	})
}

// A record signed before it has a Seq is refused by Append and BatchAppend:
// the ledger would give it a Seq the signature does not cover, and the stored
// record would never verify. Nothing of a refused batch is stored.
func TestLedgers_ARecordSignedWithoutItsSeqIsRefused(t *testing.T) {
	eachLedger(t, func(t *testing.T, l lad.Ledger) {
		priv := testKey(t)
		signed := lad.Record{Topic: lad.TopicMember, NodeID: "node-1", Body: json.RawMessage(`{}`), Timestamp: time.Now().UTC()}
		lad.SignRecord(&signed, priv)
		if err := l.Append(context.Background(), signed); err == nil {
			t.Error("Append stored a record signed before its Seq was given")
		}
		unsigned := lad.Record{Topic: lad.TopicMember, NodeID: "node-2", Body: json.RawMessage(`{}`)}
		if err := l.BatchAppend(context.Background(), []lad.Record{unsigned, signed}); err == nil {
			t.Error("BatchAppend stored a batch holding a record signed before its Seq was given")
		}
		head, err := l.Head(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if head[lad.TopicMember] != 0 {
			t.Fatalf("the refused records left the member topic at Seq %d", head[lad.TopicMember])
		}

		// A record signed with the Seq it will be stored under is kept and verifies.
		numbered := lad.Record{Topic: lad.TopicMember, Seq: 5, NodeID: "node-3", Body: json.RawMessage(`{}`), Timestamp: time.Now().UTC()}
		lad.SignRecord(&numbered, priv)
		if err := l.Append(context.Background(), numbered); err != nil {
			t.Fatalf("Append of a record signed with its Seq: %v", err)
		}
		head, err = l.Head(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if head[lad.TopicMember] != 5 {
			t.Fatalf("member head %d, want 5", head[lad.TopicMember])
		}
	})
}
