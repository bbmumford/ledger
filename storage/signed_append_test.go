/*
 * Copyright (c) 2026 ORBTR Pty Ltd. All Rights Reserved.
 */

package storage

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// Both ledgers are SigningLedgers.
var (
	_ lad.SigningLedger = (*LocalDBLedger)(nil)
	_ lad.SigningLedger = (*MemoryLedger)(nil)
)

func signWith(priv ed25519.PrivateKey) lad.SignFunc {
	return func(rec *lad.Record) error {
		lad.SignRecord(rec, priv)
		return nil
	}
}

// streamAll reads n records from l's history.
func streamAll(t *testing.T, l lad.Ledger, n int) []lad.Record {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := l.Stream(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]lad.Record, 0, n)
	timeout := time.After(5 * time.Second)
	for len(got) < n {
		select {
		case rec := <-ch:
			got = append(got, rec)
		case <-timeout:
			t.Fatalf("the ledger streamed %d of %d records", len(got), n)
		}
	}
	return got
}

// AppendSigned and BatchAppendSigned hand sign a record the ledger has already
// numbered, so every stored record verifies against the Seq it was stored
// under, and an Append after them continues the numbering.
func TestLedgers_ASignedAppendIsNumberedBeforeItIsSigned(t *testing.T) {
	eachLedger(t, func(t *testing.T, l lad.Ledger) {
		sl := l.(lad.SigningLedger)
		sign := signWith(testKey(t))
		var seenSeq []uint64
		record := func(rec *lad.Record) error {
			seenSeq = append(seenSeq, rec.Seq)
			if rec.Timestamp.IsZero() {
				t.Errorf("sign ran before the record's Timestamp was set")
			}
			return sign(rec)
		}
		if err := sl.AppendSigned(context.Background(), lad.Record{Topic: lad.TopicMember, NodeID: "node-1", Body: json.RawMessage(`{}`)}, record); err != nil {
			t.Fatalf("AppendSigned: %v", err)
		}
		if err := sl.BatchAppendSigned(context.Background(), []lad.Record{
			{Topic: lad.TopicMember, NodeID: "node-2", Body: json.RawMessage(`{}`)},
			{Topic: lad.TopicRole, NodeID: "node-2", Body: json.RawMessage(`{}`)},
			{Topic: lad.TopicMember, NodeID: "node-3", Body: json.RawMessage(`{}`)},
		}, record); err != nil {
			t.Fatalf("BatchAppendSigned: %v", err)
		}
		want := []uint64{1, 2, 1, 3}
		if len(seenSeq) != len(want) {
			t.Fatalf("sign saw Seq %v, want %v", seenSeq, want)
		}
		for i := range want {
			if seenSeq[i] != want[i] {
				t.Fatalf("sign saw Seq %v, want %v", seenSeq, want)
			}
		}
		for _, rec := range streamAll(t, l, 4) {
			if !lad.VerifyRecord(rec) {
				t.Errorf("%s Seq %d does not verify as stored", rec.Topic, rec.Seq)
			}
		}
		if err := l.Append(context.Background(), lad.Record{Topic: lad.TopicMember, NodeID: "node-4", Body: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		head, err := l.Head(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if head[lad.TopicMember] != 4 || head[lad.TopicRole] != 1 {
			t.Fatalf("heads %v, want member 4 and role 1", head)
		}
	})
}

// A sign function that fails stores nothing: not the record, and not any
// record of its batch, and no topic head moves.
func TestLedgers_AFailedSignatureStoresNothing(t *testing.T) {
	eachLedger(t, func(t *testing.T, l lad.Ledger) {
		sl := l.(lad.SigningLedger)
		refused := errors.New("no key")
		failOn := func(node string) lad.SignFunc {
			return func(rec *lad.Record) error {
				if rec.NodeID == node {
					return refused
				}
				return nil
			}
		}
		if err := sl.AppendSigned(context.Background(), lad.Record{Topic: lad.TopicMember, NodeID: "node-1", Body: json.RawMessage(`{}`)}, failOn("node-1")); !errors.Is(err, refused) {
			t.Fatalf("AppendSigned with a failing sign: %v", err)
		}
		if err := sl.BatchAppendSigned(context.Background(), []lad.Record{
			{Topic: lad.TopicMember, NodeID: "node-2", Body: json.RawMessage(`{}`)},
			{Topic: lad.TopicMember, NodeID: "node-3", Body: json.RawMessage(`{}`)},
		}, failOn("node-3")); !errors.Is(err, refused) {
			t.Fatalf("BatchAppendSigned with a failing sign: %v", err)
		}
		head, err := l.Head(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if head[lad.TopicMember] != 0 {
			t.Fatalf("a failed signature left the member head at %d", head[lad.TopicMember])
		}
		if err := sl.AppendSigned(context.Background(), lad.Record{Topic: lad.TopicMember, NodeID: "node-4"}, nil); err == nil {
			t.Fatal("AppendSigned with no sign function stored a record")
		}
		if err := sl.BatchAppendSigned(context.Background(), []lad.Record{{Topic: lad.TopicMember, NodeID: "node-4"}}, nil); err == nil {
			t.Fatal("BatchAppendSigned with no sign function stored a record")
		}
	})
}

// Signed appends from several goroutines at once each take their own Seq and
// each verifies.
func TestLedgers_ConcurrentSignedAppendsEachVerify(t *testing.T) {
	eachLedger(t, func(t *testing.T, l lad.Ledger) {
		sl := l.(lad.SigningLedger)
		sign := signWith(testKey(t))
		const n = 16
		errs := make(chan error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- sl.AppendSigned(context.Background(), lad.Record{Topic: lad.TopicReach, NodeID: "node-1", Body: json.RawMessage(`{}`)}, sign)
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Errorf("concurrent AppendSigned: %v", err)
			}
		}
		seen := map[uint64]bool{}
		for _, rec := range streamAll(t, l, n) {
			if seen[rec.Seq] {
				t.Errorf("Seq %d stored twice", rec.Seq)
			}
			seen[rec.Seq] = true
			if !lad.VerifyRecord(rec) {
				t.Errorf("Seq %d does not verify", rec.Seq)
			}
		}
	})
}
