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

func TestNewEndgameFetcher_RejectsNilFetcher(t *testing.T) {
	r := newStubResolver(t, nil)
	_, err := NewEndgameFetcher(nil, r, EndgameConfig{})
	if err == nil {
		t.Fatal("expected error for nil fetcher")
	}
}

func TestNewEndgameFetcher_RejectsNilResolver(t *testing.T) {
	tr := newMapTransport()
	r := newStubResolver(t, nil)
	f, _ := NewFetcher(r, tr, FetcherConfig{})
	_, err := NewEndgameFetcher(f, nil, EndgameConfig{})
	if err == nil {
		t.Fatal("expected error for nil resolver")
	}
}

func TestNewEndgameFetcher_RejectsThresholdOutOfRange(t *testing.T) {
	tr := newMapTransport()
	r := newStubResolver(t, nil)
	f, _ := NewFetcher(r, tr, FetcherConfig{})
	_, err := NewEndgameFetcher(f, r, EndgameConfig{Threshold: 1.5})
	if err == nil {
		t.Fatal("expected error for threshold >= 1")
	}
}

// (The 2-piece no-endgame case is covered by shouldEnterEndgame unit tests
// below — a full end-to-end setup with mapTransport is awkward because it
// keys on peerID rather than CID; endgameTransport handles per-CID cases.)

// endgameTransport is a PeerTransport that maps CID→bytes so multi-piece
// tests can serve the right piece per CID.
type endgameTransport struct {
	byCID  map[string][]byte
	delay  map[string]time.Duration
	errFor map[string]error
}

func newEndgameTransport() *endgameTransport {
	return &endgameTransport{
		byCID:  make(map[string][]byte),
		delay:  make(map[string]time.Duration),
		errFor: make(map[string]error),
	}
}

func (e *endgameTransport) FetchPiece(ctx context.Context, peerID string, req FetchRequest) (FetchResponse, error) {
	if e.delay[peerID] > 0 {
		select {
		case <-time.After(e.delay[peerID]):
		case <-ctx.Done():
			return FetchResponse{}, ctx.Err()
		}
	}
	if err := e.errFor[peerID]; err != nil {
		return FetchResponse{}, err
	}
	return FetchResponse{CID: req.CID, Payload: e.byCID[req.CID]}, nil
}

// TestEndgame_FetchFileHappyPath fetches a 3-piece file end-to-end. No
// endgame engages (3 pieces × 99% = 2.97 held threshold; after piece 2 we
// have 66% done, still below). So it's purely a stage-1 test.
func TestEndgame_FetchFileHappyPath(t *testing.T) {
	piece := make([][]byte, 3)
	cids := make([]string, 3)
	for i := range piece {
		piece[i] = []byte("piece-" + string(rune('a'+i)))
		cids[i] = computePieceCID(piece[i])
	}

	tr := newEndgameTransport()
	for i, cid := range cids {
		tr.byCID[cid] = piece[i]
	}

	r := newStubResolver(t, []PeerAnswer{{PeerID: "peer-A"}})
	f, _ := NewFetcher(r, tr, FetcherConfig{})
	eg, err := NewEndgameFetcher(f, r, EndgameConfig{})
	if err != nil {
		t.Fatalf("NewEndgameFetcher: %v", err)
	}

	manifest := &Manifest{
		RootCID: "root",
		Pieces: []PieceRef{
			{Index: 0, CID: cids[0], SizeBytes: len(piece[0])},
			{Index: 1, CID: cids[1], SizeBytes: len(piece[1])},
			{Index: 2, CID: cids[2], SizeBytes: len(piece[2])},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := eg.FetchFile(ctx, manifest)
	if err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d pieces, want 3", len(got))
	}
	for i := range piece {
		if string(got[i]) != string(piece[i]) {
			t.Errorf("piece %d = %q, want %q", i, got[i], piece[i])
		}
	}
}

// TestEndgame_NilManifest rejects nil/empty input.
func TestEndgame_NilManifest(t *testing.T) {
	tr := newEndgameTransport()
	r := newStubResolver(t, nil)
	f, _ := NewFetcher(r, tr, FetcherConfig{})
	eg, _ := NewEndgameFetcher(f, r, EndgameConfig{})

	if _, err := eg.FetchFile(context.Background(), nil); err == nil {
		t.Error("expected error for nil manifest")
	}
	if _, err := eg.FetchFile(context.Background(), &Manifest{}); err == nil {
		t.Error("expected error for empty manifest")
	}
}

// TestEndgame_ShouldEnterEndgame unit-tests the threshold/min-remaining logic.
func TestEndgame_ShouldEnterEndgame(t *testing.T) {
	tr := newEndgameTransport()
	r := newStubResolver(t, nil)
	f, _ := NewFetcher(r, tr, FetcherConfig{})
	eg, _ := NewEndgameFetcher(f, r, EndgameConfig{
		Threshold:    0.5,
		MinRemaining: 2,
	})

	// 5 pieces, 2 held = 40%, below threshold.
	if eg.shouldEnterEndgame(2, 5) {
		t.Error("40% should not trigger endgame at threshold 50%")
	}
	// 5 pieces, 3 held = 60%, above threshold, 2 remaining >= min.
	if !eg.shouldEnterEndgame(3, 5) {
		t.Error("60% with 2 remaining should trigger endgame")
	}
	// 5 pieces, 4 held = 80%, 1 remaining < min.
	if eg.shouldEnterEndgame(4, 5) {
		t.Error("1 remaining below min should not trigger endgame")
	}
	// Zero total — edge case.
	if eg.shouldEnterEndgame(0, 0) {
		t.Error("zero total should not trigger endgame")
	}
}
