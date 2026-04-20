/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mapTransport implements PeerTransport using a static map of
// peerID → payload. Use addSlow to inject delays.
type mapTransport struct {
	mu       sync.Mutex
	payloads map[string][]byte
	errs     map[string]error
	delays   map[string]time.Duration
	reqCount map[string]int
	cancels  int32
	// cancelled is closed when the first in-flight FetchPiece observes ctx
	// cancellation. Tests that race a winner against a losing peer use this
	// to deterministically know the loser has exited without sleeping.
	cancelled     chan struct{}
	cancelledOnce sync.Once
}

func newMapTransport() *mapTransport {
	return &mapTransport{
		payloads:  make(map[string][]byte),
		errs:      make(map[string]error),
		delays:    make(map[string]time.Duration),
		reqCount:  make(map[string]int),
		cancelled: make(chan struct{}),
	}
}

func (m *mapTransport) setPayload(peerID string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.payloads[peerID] = data
}

func (m *mapTransport) setErr(peerID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errs[peerID] = err
}

func (m *mapTransport) setDelay(peerID string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delays[peerID] = d
}

func (m *mapTransport) FetchPiece(ctx context.Context, peerID string, req FetchRequest) (FetchResponse, error) {
	m.mu.Lock()
	m.reqCount[peerID]++
	err := m.errs[peerID]
	delay := m.delays[peerID]
	data := m.payloads[peerID]
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			atomic.AddInt32(&m.cancels, 1)
			m.cancelledOnce.Do(func() { close(m.cancelled) })
			return FetchResponse{}, ctx.Err()
		}
	}
	if err != nil {
		return FetchResponse{}, err
	}
	return FetchResponse{CID: req.CID, Payload: data}, nil
}

// resolverWithAnswers stubs the Resolver without hitting whisper.
type staticResolver struct {
	answers []PeerAnswer
	err     error
}

func (s staticResolver) Query(ctx context.Context, topic string, payload []byte) ([][]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	resp, _ := json.Marshal(&FindResponse{Answers: s.answers})
	return [][]byte{resp}, nil
}

func newStubResolver(t *testing.T, answers []PeerAnswer) *Resolver {
	t.Helper()
	r, err := NewResolver(
		stubHas{have: map[string]bool{}},
		staticResolver{answers: answers},
		ResolverConfig{PeerID: "asker"},
	)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	return r
}

func TestFetcherHappyPath(t *testing.T) {
	data := []byte("hello-distributed-piece")
	cid := computePieceCID(data)

	tr := newMapTransport()
	tr.setPayload("peer-A", data)

	r := newStubResolver(t, []PeerAnswer{{PeerID: "peer-A"}})
	f, err := NewFetcher(r, tr, FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher: %v", err)
	}
	got, err := f.Fetch(context.Background(), cid)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q want %q", got, data)
	}
}

func TestFetcherRejectsCIDMismatch(t *testing.T) {
	data := []byte("hello-distributed-piece")
	cid := computePieceCID(data)

	tr := newMapTransport()
	// Lying peer sends garbage under the requested CID.
	tr.setPayload("peer-Evil", []byte("totally-not-the-right-bytes"))

	r := newStubResolver(t, []PeerAnswer{{PeerID: "peer-Evil"}})
	f, _ := NewFetcher(r, tr, FetcherConfig{})

	_, err := f.Fetch(context.Background(), cid)
	if !errors.Is(err, ErrAllFetchesFailed) {
		t.Fatalf("expected ErrAllFetchesFailed, got %v", err)
	}
}

func TestFetcherNoPeers(t *testing.T) {
	tr := newMapTransport()
	r := newStubResolver(t, nil)
	f, _ := NewFetcher(r, tr, FetcherConfig{})
	_, err := f.Fetch(context.Background(), "some-cid")
	if !errors.Is(err, ErrNoPeers) {
		t.Fatalf("expected ErrNoPeers, got %v", err)
	}
}

func TestFetcherTimeout(t *testing.T) {
	data := []byte("eventually-arrives")
	cid := computePieceCID(data)

	tr := newMapTransport()
	tr.setPayload("peer-Slow", data)
	tr.setDelay("peer-Slow", 200*time.Millisecond)

	r := newStubResolver(t, []PeerAnswer{{PeerID: "peer-Slow"}})
	f, _ := NewFetcher(r, tr, FetcherConfig{
		PerPeerTimeout: 20 * time.Millisecond,
		OverallTimeout: 40 * time.Millisecond,
	})
	_, err := f.Fetch(context.Background(), cid)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestFetcherFirstValidWinsAndCancelsOthers(t *testing.T) {
	data := []byte("the-winning-bytes")
	cid := computePieceCID(data)

	tr := newMapTransport()
	// Fast peer wins immediately.
	tr.setPayload("peer-Fast", data)
	// Slow peer would succeed but should be cancelled.
	tr.setPayload("peer-Slow", data)
	tr.setDelay("peer-Slow", 500*time.Millisecond)
	// Erroring peer returns immediately.
	tr.setErr("peer-Err", errors.New("offline"))

	r := newStubResolver(t, []PeerAnswer{
		{PeerID: "peer-Fast"},
		{PeerID: "peer-Slow"},
		{PeerID: "peer-Err"},
	})
	f, _ := NewFetcher(r, tr, FetcherConfig{
		TopK:           3,
		PerPeerTimeout: 2 * time.Second,
		OverallTimeout: 2 * time.Second,
	})
	start := time.Now()
	got, err := f.Fetch(context.Background(), cid)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("payload mismatch")
	}
	if elapsed > 400*time.Millisecond {
		t.Errorf("Fetch took %v, expected <400ms (slow peer should have been cancelled)", elapsed)
	}
	// Deterministically await the slow peer observing its cancel. Bounded by
	// the test's overall budget to guard against a genuine hang.
	select {
	case <-tr.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatalf("slow peer never observed cancellation (cancels=%d)", atomic.LoadInt32(&tr.cancels))
	}
	if atomic.LoadInt32(&tr.cancels) < 1 {
		t.Errorf("expected at least 1 peer to observe cancellation, got %d", tr.cancels)
	}
}

func TestFetcherRanksByLatencyAndReputation(t *testing.T) {
	// Rank test: three peers, only TopK=1 is called. Peer with the best
	// latency × reputation score must be the one contacted.
	answers := []PeerAnswer{
		{PeerID: "fast-lowrep", LatencyHintMs: 10},  // rep 0.1
		{PeerID: "slow-highrep", LatencyHintMs: 200}, // rep 1.0
		{PeerID: "med-medrep", LatencyHintMs: 50},   // rep 0.5
	}
	rep := reputationFunc(func(peerID string) float64 {
		switch peerID {
		case "fast-lowrep":
			return 0.1
		case "slow-highrep":
			return 1.0
		case "med-medrep":
			return 0.5
		}
		return 0
	})
	// Scores:
	//   fast-lowrep : 0.1 / (1.0 + 0.01)  = 0.099
	//   slow-highrep: 1.0 / (1.0 + 0.2)   = 0.833
	//   med-medrep  : 0.5 / (1.0 + 0.05)  = 0.476
	// Top-1 must be slow-highrep.
	data := []byte("bytes")
	cid := computePieceCID(data)
	tr := newMapTransport()
	for _, a := range answers {
		tr.setPayload(a.PeerID, data)
	}

	r := newStubResolver(t, answers)
	f, _ := NewFetcher(r, tr, FetcherConfig{TopK: 1, Reputation: rep})
	if _, err := f.Fetch(context.Background(), cid); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	tr.mu.Lock()
	calls := make(map[string]int, len(tr.reqCount))
	for k, v := range tr.reqCount {
		calls[k] = v
	}
	tr.mu.Unlock()
	if calls["slow-highrep"] != 1 {
		t.Errorf("expected slow-highrep to be called 1 time, got %d", calls["slow-highrep"])
	}
	if calls["fast-lowrep"] != 0 || calls["med-medrep"] != 0 {
		t.Errorf("other peers should not be called: got %+v", calls)
	}
}

type reputationFunc func(string) float64

func (r reputationFunc) Score(peerID string) float64 { return r(peerID) }

func TestFetcherSkipsZeroReputationPeers(t *testing.T) {
	data := []byte("x")
	cid := computePieceCID(data)

	tr := newMapTransport()
	tr.setPayload("bad", data) // CID-valid but reputation=0

	rep := reputationFunc(func(peerID string) float64 {
		if peerID == "bad" {
			return 0
		}
		return 1
	})
	r := newStubResolver(t, []PeerAnswer{{PeerID: "bad"}})
	f, _ := NewFetcher(r, tr, FetcherConfig{Reputation: rep})
	_, err := f.Fetch(context.Background(), cid)
	if !errors.Is(err, ErrNoPeers) {
		t.Fatalf("expected ErrNoPeers after filtering, got %v", err)
	}
}

func TestStoreFallsBackToFetcher(t *testing.T) {
	// End-to-end wiring: Store.Get misses locally, falls back to Fetcher,
	// and caches the result.
	data := []byte("piece-data-via-fetcher")
	cid := computePieceCID(data)

	local := newTestStore(t, true) // in-memory local store
	tr := newMapTransport()
	tr.setPayload("peer-seed", data)

	resolver := newStubResolver(t, []PeerAnswer{{PeerID: "peer-seed"}})
	fetcher, err := NewFetcher(resolver, tr, FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher: %v", err)
	}
	// Swap in a new Store wired to the fetcher (reusing the seed's key).
	local.cfg.Fetcher = fetcher

	got, err := local.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("Get returned %q want %q", got, data)
	}
	// Cached back?
	has, _ := local.Has(context.Background(), cid)
	if !has {
		t.Error("expected piece to be cached back into store after fetch")
	}

	// Second Get must be local (no extra transport call).
	before := tr.reqCount["peer-seed"]
	if _, err := local.Get(context.Background(), cid); err != nil {
		t.Fatalf("Get #2: %v", err)
	}
	after := tr.reqCount["peer-seed"]
	if after != before {
		t.Errorf("second Get triggered another transport call (%d→%d); expected local hit", before, after)
	}
}

func TestStoreWithoutFetcherPreservesPhase1(t *testing.T) {
	s := newTestStore(t, true)
	_, err := s.Get(context.Background(), "nonexistent-cid-0000000000")
	if !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("expected ErrBlobNotFound with no fetcher, got %v", err)
	}
}

func TestStoreFetcherErrorPropagatesNotFoundOnNoPeers(t *testing.T) {
	s := newTestStore(t, true)
	tr := newMapTransport()
	resolver := newStubResolver(t, nil) // no peers
	fetcher, _ := NewFetcher(resolver, tr, FetcherConfig{})
	s.cfg.Fetcher = fetcher
	_, err := s.Get(context.Background(), "unknown")
	if !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("expected ErrBlobNotFound when fetcher finds no peers, got %v", err)
	}
}
