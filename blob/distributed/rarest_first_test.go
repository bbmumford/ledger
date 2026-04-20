/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRarestFirst_ObserveAndCount(t *testing.T) {
	s := NewRarestFirstState(time.Minute)
	s.Observe("peer-a", "cid-1", time.Now())
	s.Observe("peer-b", "cid-1", time.Now())
	s.Observe("peer-a", "cid-2", time.Now())

	if got := s.Count("cid-1"); got != 2 {
		t.Errorf("cid-1 count = %d, want 2", got)
	}
	if got := s.Count("cid-2"); got != 1 {
		t.Errorf("cid-2 count = %d, want 1", got)
	}
	if got := s.Count("cid-unknown"); got != 0 {
		t.Errorf("cid-unknown count = %d, want 0", got)
	}
}

func TestRarestFirst_ObserveIdempotent(t *testing.T) {
	s := NewRarestFirstState(time.Minute)
	s.Observe("peer-a", "cid-1", time.Now())
	s.Observe("peer-a", "cid-1", time.Now()) // duplicate
	if got := s.Count("cid-1"); got != 1 {
		t.Errorf("duplicate observe: count = %d, want 1", got)
	}
}

func TestRarestFirst_Forget(t *testing.T) {
	s := NewRarestFirstState(time.Minute)
	s.Observe("peer-a", "cid-1", time.Now())
	s.Observe("peer-b", "cid-1", time.Now())
	s.Forget("peer-a", "cid-1")

	if got := s.Count("cid-1"); got != 1 {
		t.Errorf("after forget count = %d, want 1", got)
	}

	s.Forget("peer-b", "cid-1")
	if got := s.Count("cid-1"); got != 0 {
		t.Errorf("after forget-last count = %d, want 0", got)
	}
}

func TestRarestFirst_TTLExpiry(t *testing.T) {
	now := time.Now()
	fakeClock := now
	s := NewRarestFirstState(100 * time.Millisecond)
	s.setNow(func() time.Time { return fakeClock })

	s.Observe("peer-a", "cid-1", fakeClock)

	// Advance past TTL.
	fakeClock = now.Add(200 * time.Millisecond)
	if got := s.Count("cid-1"); got != 0 {
		t.Errorf("after TTL count = %d, want 0 (expired)", got)
	}

	// Prune actually removes them.
	s.Prune()
	if got := len(s.KnownCIDs()); got != 0 {
		t.Errorf("KnownCIDs after prune = %d, want 0", got)
	}
}

func TestRarestFirst_RankRarest(t *testing.T) {
	s := NewRarestFirstState(time.Minute)
	// cid-a: 3 peers (most popular)
	s.Observe("p1", "cid-a", time.Now())
	s.Observe("p2", "cid-a", time.Now())
	s.Observe("p3", "cid-a", time.Now())
	// cid-b: 1 peer (rarest known)
	s.Observe("p1", "cid-b", time.Now())
	// cid-c: unknown (rarest possible)

	ranked := s.RankRarest([]string{"cid-a", "cid-b", "cid-c"})
	if len(ranked) != 3 {
		t.Fatalf("ranked len = %d, want 3", len(ranked))
	}
	// Expect order: cid-c (0) < cid-b (1) < cid-a (3)
	if ranked[0] != "cid-c" || ranked[1] != "cid-b" || ranked[2] != "cid-a" {
		t.Errorf("ranked = %v, want [cid-c cid-b cid-a]", ranked)
	}
}

func TestRarestFirst_RankStableForTies(t *testing.T) {
	s := NewRarestFirstState(time.Minute)
	s.Observe("p1", "a", time.Now())
	s.Observe("p1", "b", time.Now())
	s.Observe("p1", "c", time.Now())

	// All three have count 1 — stable sort should preserve input order.
	ranked := s.RankRarest([]string{"b", "a", "c"})
	if ranked[0] != "b" || ranked[1] != "a" || ranked[2] != "c" {
		t.Errorf("ranked = %v, want stable [b a c]", ranked)
	}
}

func TestRarestFirst_ObserveHaveAdvertisement(t *testing.T) {
	s := NewRarestFirstState(time.Minute)
	ad := &HaveAdvertisement{
		PeerID:          "peer-x",
		EmittedAtUnixMs: time.Now().UnixMilli(),
		CIDs:            []string{"c1", "c2", "c3"},
	}
	s.ObserveHaveAdvertisement(ad)
	for _, cid := range []string{"c1", "c2", "c3"} {
		if got := s.Count(cid); got != 1 {
			t.Errorf("cid %s count = %d, want 1", cid, got)
		}
	}
}

func TestRarestFirst_ConcurrentSafe(t *testing.T) {
	s := NewRarestFirstState(time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Observe("peer", "cid", time.Now())
				_ = s.Count("cid")
				_ = s.RankRarest([]string{"cid"})
			}
		}(i)
	}
	wg.Wait()
	// Just testing no races — -race flag catches issues.
}

func TestRarestFirst_PrunerLifecycle(t *testing.T) {
	s := NewRarestFirstState(50 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartPruner(ctx, 20*time.Millisecond)

	s.Observe("peer", "cid", time.Now())
	if got := s.Count("cid"); got != 1 {
		t.Fatalf("pre-prune count = %d, want 1", got)
	}
	// Wait past TTL + one prune tick.
	time.Sleep(150 * time.Millisecond)
	if got := s.Count("cid"); got != 0 {
		t.Errorf("post-prune count = %d, want 0", got)
	}
	s.StopPruner()
}
