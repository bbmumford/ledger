/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"testing"
	"time"
)

// newTestStore is a helper that creates an in-memory Store for replicator tests.
func newReplicatorTestStore(t *testing.T) *Store {
	t.Helper()
	key := make([]byte, 32)
	s, err := NewDistributedBlobStore(Config{
		StorageKey: key,
		InMemory:   true,
	})
	if err != nil {
		t.Fatalf("NewDistributedBlobStore: %v", err)
	}
	return s
}

func TestNewReplicator_RequiresStore(t *testing.T) {
	_, err := NewReplicator(ReplicatorConfig{
		PeerID:      "p",
		RarestFirst: NewRarestFirstState(0),
	}, nil, nil, nil, &Fetcher{})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestNewReplicator_RequiresFetcher(t *testing.T) {
	store := newReplicatorTestStore(t)
	_, err := NewReplicator(ReplicatorConfig{
		PeerID:      "p",
		RarestFirst: NewRarestFirstState(0),
	}, store, nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil fetcher")
	}
}

func TestNewReplicator_RequiresPeerID(t *testing.T) {
	store := newReplicatorTestStore(t)
	_, err := NewReplicator(ReplicatorConfig{
		RarestFirst: NewRarestFirstState(0),
	}, store, nil, nil, &Fetcher{})
	if err == nil {
		t.Fatal("expected error for empty peerID")
	}
}

func TestNewReplicator_RequiresRarestFirst(t *testing.T) {
	store := newReplicatorTestStore(t)
	_, err := NewReplicator(ReplicatorConfig{
		PeerID: "p",
	}, store, nil, nil, &Fetcher{})
	if err == nil {
		t.Fatal("expected error for nil RarestFirst")
	}
}

// TestReplicator_TickFetchesMissing runs one tick and verifies missing CIDs
// get fetched + cached. Uses the same mapTransport pattern as fetcher_test.go.
func TestReplicator_TickFetchesMissing(t *testing.T) {
	data := []byte("piece-bytes-for-replicator-test")
	cid := computePieceCID(data)

	tr := newMapTransport()
	tr.setPayload("peer-A", data)

	store := newReplicatorTestStore(t)
	rarest := NewRarestFirstState(time.Minute)
	rarest.Observe("peer-A", cid, time.Now())

	resolver := newStubResolver(t, []PeerAnswer{{PeerID: "peer-A"}})
	fetcher, err := NewFetcher(resolver, tr, FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher: %v", err)
	}

	rep, err := NewReplicator(ReplicatorConfig{
		PeerID:          "self",
		FetchInterval:   time.Hour, // large so the loop doesn't spin
		MaxParallelJobs: 2,
		RarestFirst:     rarest,
	}, store, nil, resolver, fetcher)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rep.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	has, err := store.Has(ctx, cid)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !has {
		t.Errorf("expected store to hold cid after tick")
	}
}

// TestReplicator_TickSkipsHeld verifies we don't re-fetch pieces we already
// have — filterMissing should strip them.
func TestReplicator_TickSkipsHeld(t *testing.T) {
	data := []byte("already-held")
	store := newReplicatorTestStore(t)
	cid, err := store.Put(context.Background(), data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	tr := newMapTransport()
	tr.setErr("peer-A", context.DeadlineExceeded) // poison — must not be called

	rarest := NewRarestFirstState(time.Minute)
	rarest.Observe("peer-A", cid, time.Now())

	resolver := newStubResolver(t, []PeerAnswer{{PeerID: "peer-A"}})
	fetcher, _ := NewFetcher(resolver, tr, FetcherConfig{})

	rep, _ := NewReplicator(ReplicatorConfig{
		PeerID:      "self",
		RarestFirst: rarest,
	}, store, nil, resolver, fetcher)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rep.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	// If it tried to fetch, tr.FetchPiece would have returned the poisoned
	// error. The fact that Tick returned nil means filterMissing excluded
	// the held cid.
}

// TestReplicator_StartStop verifies the goroutine lifecycle. Uses a
// CandidateSource that signals on every invocation so the test can wait for
// at least two ticks (the priming tick + one interval tick) without
// sleeping.
func TestReplicator_StartStop(t *testing.T) {
	tr := newMapTransport()
	resolver := newStubResolver(t, nil)
	fetcher, _ := NewFetcher(resolver, tr, FetcherConfig{})
	store := newReplicatorTestStore(t)

	ticks := make(chan struct{}, 16)
	candidates := CandidateSourceFunc(func(ctx context.Context) ([]string, error) {
		select {
		case ticks <- struct{}{}:
		default:
		}
		return nil, nil
	})

	rep, _ := NewReplicator(ReplicatorConfig{
		PeerID:        "self",
		FetchInterval: 10 * time.Millisecond,
		RarestFirst:   NewRarestFirstState(time.Minute),
		Candidates:    candidates,
	}, store, nil, resolver, fetcher)

	ctx := context.Background()
	if err := rep.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Double-start rejects.
	if err := rep.Start(ctx); err == nil {
		t.Error("expected error on double-start")
	}
	// Wait for at least 2 ticks to fire deterministically.
	deadline := time.After(2 * time.Second)
	for got := 0; got < 2; {
		select {
		case <-ticks:
			got++
		case <-deadline:
			t.Fatalf("only observed %d ticks in 2s", got)
		}
	}
	rep.Stop()
	// Stop is idempotent.
	rep.Stop()
}

// TestReplicator_AbuseGuardDeniesOpportunisticFetch wires an AbuseGuard
// with a zero-byte per-peer quota onto the replicator and confirms the
// fetched piece is dropped (not stored, not re-advertised).
func TestReplicator_AbuseGuardDeniesOpportunisticFetch(t *testing.T) {
	data := []byte("bytes-that-must-not-be-stored")
	cid := computePieceCID(data)

	tr := newMapTransport()
	tr.setPayload("peer-A", data)

	store := newReplicatorTestStore(t)
	rarest := NewRarestFirstState(time.Minute)
	rarest.Observe("peer-A", cid, time.Now())

	resolver := newStubResolver(t, []PeerAnswer{{PeerID: "peer-A"}})
	fetcher, err := NewFetcher(resolver, tr, FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher: %v", err)
	}

	// Guard with a per-peer cap smaller than the payload → Check must reject.
	guard := NewAbuseGuard(AbuseGuardConfig{
		Limits:                     DefaultPerTypeLimits(),
		MaxReplicationBytesPerPeer: 4, // way below len(data)
		MaxReplicationBytesTotal:   4,
	})

	rep, err := NewReplicator(ReplicatorConfig{
		PeerID:          "self",
		FetchInterval:   time.Hour,
		MaxParallelJobs: 1,
		RarestFirst:     rarest,
		AbuseGuard:      guard,
	}, store, nil, resolver, fetcher)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rep.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	has, err := store.Has(ctx, cid)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if has {
		t.Fatalf("store should NOT hold cid after guard denial")
	}
	// Per-peer ledger untouched — Check returned an error BEFORE charging.
	if got := guard.ReplicationUsage("peer-A"); got != 0 {
		t.Errorf("peer-A replication usage after denial: got %d want 0", got)
	}
}

// TestReplicator_AbuseGuardAllowsAndCharges confirms the happy path:
// Check accepts the piece, Put stores it, and the peer's replication
// ledger reflects the charge.
func TestReplicator_AbuseGuardAllowsAndCharges(t *testing.T) {
	data := []byte("bytes-that-should-be-stored")
	cid := computePieceCID(data)

	tr := newMapTransport()
	tr.setPayload("peer-A", data)

	store := newReplicatorTestStore(t)
	rarest := NewRarestFirstState(time.Minute)
	rarest.Observe("peer-A", cid, time.Now())

	resolver := newStubResolver(t, []PeerAnswer{{PeerID: "peer-A"}})
	fetcher, err := NewFetcher(resolver, tr, FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher: %v", err)
	}

	guard := NewAbuseGuard(AbuseGuardConfig{
		Limits:                     DefaultPerTypeLimits(),
		MaxReplicationBytesPerPeer: 1 << 20,
		MaxReplicationBytesTotal:   10 << 20,
	})

	rep, err := NewReplicator(ReplicatorConfig{
		PeerID:          "self",
		FetchInterval:   time.Hour,
		MaxParallelJobs: 1,
		RarestFirst:     rarest,
		AbuseGuard:      guard,
	}, store, nil, resolver, fetcher)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rep.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	has, err := store.Has(ctx, cid)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !has {
		t.Fatalf("store should hold cid after guard-allowed fetch")
	}
	if got := guard.ReplicationUsage("peer-A"); got != int64(len(data)) {
		t.Errorf("peer-A replication usage: got %d want %d", got, len(data))
	}
}

// TestReplicator_ReAdvertisesAfterFetch verifies that a successful cache-back
// triggers an immediate advertiser emission, rather than waiting for the
// next 60s advertise-interval tick.
func TestReplicator_ReAdvertisesAfterFetch(t *testing.T) {
	data := []byte("piece-for-readvertise-test")
	cid := computePieceCID(data)

	tr := newMapTransport()
	tr.setPayload("peer-A", data)

	store := newReplicatorTestStore(t)
	rarest := NewRarestFirstState(time.Minute)
	rarest.Observe("peer-A", cid, time.Now())

	resolver := newStubResolver(t, []PeerAnswer{{PeerID: "peer-A"}})
	fetcher, err := NewFetcher(resolver, tr, FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher: %v", err)
	}

	pub := newFakePublisher()
	advertiser, err := NewAdvertiser(store, pub, AdvertiserConfig{
		PeerID:   "self",
		Interval: time.Hour, // long — we only want to observe the post-Put emission
	})
	if err != nil {
		t.Fatalf("NewAdvertiser: %v", err)
	}

	rep, err := NewReplicator(ReplicatorConfig{
		PeerID:          "self",
		FetchInterval:   time.Hour,
		MaxParallelJobs: 2,
		RarestFirst:     rarest,
	}, store, advertiser, resolver, fetcher)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	before := pub.Count()
	if err := rep.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	// Wait for the post-Put EmitOnce to land on the publisher.
	select {
	case <-pub.notify:
	case <-time.After(2 * time.Second):
		t.Fatalf("advertiser never emitted after successful fetch")
	}
	after := pub.Count()
	if after <= before {
		t.Fatalf("expected advertiser emission count to rise (before=%d after=%d)", before, after)
	}
}
