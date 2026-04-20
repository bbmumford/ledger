/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"sort"
	"sync"
	"time"
)

// DefaultRarestFirstTTL is the window in which an observed (peer, cid) pair
// is considered fresh. After this, the entry is treated as stale and pruned.
// Mirrors the HaveAdvertisement cadence × a small multiplier so a peer that
// went offline for one skipped tick still counts.
const DefaultRarestFirstTTL = 1 * time.Hour

// rarestFirstEntry tracks the set of peers known to hold a given CID plus
// the last time we saw a holder-advertisement. The map is keyed on peerID
// so duplicates within the TTL window are deduped, and Forget can remove
// a single peer when it is known to have evicted the piece.
type rarestFirstEntry struct {
	peers map[string]time.Time
}

// RarestFirstState tracks per-CID replication counts derived from inbound
// HaveAdvertisement broadcasts (plus any peer that explicitly responded to
// a plutus.blob.find request). The Replicator consults this state to
// prioritize fetching rare pieces before popular ones — implementing the
// BitTorrent-style rarest-first heuristic described in
// _OTHER/mercury/ROADMAP.md section 2 "Piece fetching protocol" item 3.
//
// The zero value is NOT valid; construct via NewRarestFirstState. The type
// is safe for concurrent use.
type RarestFirstState struct {
	mu      sync.Mutex
	entries map[string]*rarestFirstEntry // cid -> {peerID -> observedAt}
	updated map[string]time.Time         // cid -> last bookkeeping timestamp
	ttl     time.Duration
	now     func() time.Time

	// pruneCancel, if non-nil, stops the background pruner. Protected by mu.
	pruneCancel context.CancelFunc
	pruneDone   chan struct{}
}

// NewRarestFirstState constructs an empty tracker. Pass zero for ttl to use
// DefaultRarestFirstTTL.
func NewRarestFirstState(ttl time.Duration) *RarestFirstState {
	if ttl <= 0 {
		ttl = DefaultRarestFirstTTL
	}
	return &RarestFirstState{
		entries: make(map[string]*rarestFirstEntry),
		updated: make(map[string]time.Time),
		ttl:     ttl,
		now:     time.Now,
	}
}

// setNow overrides the clock for tests.
func (s *RarestFirstState) setNow(f func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = f
}

// Observe records that peerID holds cid as of emittedAt. A zero emittedAt
// falls back to the state's current time. Idempotent per (peer, cid): a
// newer observation overwrites the older timestamp.
func (s *RarestFirstState) Observe(peerID, cid string, emittedAt time.Time) {
	if peerID == "" || cid == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := emittedAt
	if ts.IsZero() {
		ts = s.now()
	}
	ent, ok := s.entries[cid]
	if !ok {
		ent = &rarestFirstEntry{peers: make(map[string]time.Time)}
		s.entries[cid] = ent
	}
	if existing, had := ent.peers[peerID]; !had || ts.After(existing) {
		ent.peers[peerID] = ts
	}
	s.updated[cid] = s.now()
}

// Forget removes a single (peer, cid) observation. Used when we learn a
// peer evicted a piece (e.g., its next HaveAdvertisement omits the CID) or
// when a targeted fetch surfaces that the peer no longer holds it.
func (s *RarestFirstState) Forget(peerID, cid string) {
	if peerID == "" || cid == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ent, ok := s.entries[cid]
	if !ok {
		return
	}
	delete(ent.peers, peerID)
	if len(ent.peers) == 0 {
		delete(s.entries, cid)
		delete(s.updated, cid)
		return
	}
	s.updated[cid] = s.now()
}

// ObserveHaveAdvertisement convenience: record every CID in a decoded
// HaveAdvertisement against the advertising peer. Callers subscribing to
// plutus.blob.have broadcasts can pipe decoded ads straight into this.
func (s *RarestFirstState) ObserveHaveAdvertisement(ad *HaveAdvertisement) {
	if ad == nil || ad.PeerID == "" {
		return
	}
	ts := time.UnixMilli(ad.EmittedAtUnixMs)
	if ad.EmittedAtUnixMs == 0 {
		ts = time.Time{} // let Observe default to now()
	}
	for _, cid := range ad.CIDs {
		s.Observe(ad.PeerID, cid, ts)
	}
}

// Count returns the current peer replication count for cid, excluding
// entries older than the TTL.
func (s *RarestFirstState) Count(cid string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.liveCountLocked(cid)
}

// liveCountLocked returns the live peer count for cid. Caller holds s.mu.
func (s *RarestFirstState) liveCountLocked(cid string) int {
	ent, ok := s.entries[cid]
	if !ok {
		return 0
	}
	cutoff := s.now().Add(-s.ttl)
	count := 0
	for _, ts := range ent.peers {
		if ts.After(cutoff) {
			count++
		}
	}
	return count
}

// RankRarest returns cids sorted ascending by live replication count
// (rarest first). Ties are broken by the input order (stable sort), which
// keeps behavior deterministic for tests. Unknown CIDs (count 0) come
// first — they are the rarest possible.
func (s *RarestFirstState) RankRarest(cids []string) []string {
	if len(cids) == 0 {
		return nil
	}
	s.mu.Lock()
	counts := make(map[string]int, len(cids))
	for _, cid := range cids {
		counts[cid] = s.liveCountLocked(cid)
	}
	s.mu.Unlock()

	out := make([]string, len(cids))
	copy(out, cids)
	sort.SliceStable(out, func(i, j int) bool {
		return counts[out[i]] < counts[out[j]]
	})
	return out
}

// Snapshot returns a copy of (cid → live count) for every tracked CID.
// Useful for metrics / debugging.
func (s *RarestFirstState) Snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.entries))
	for cid := range s.entries {
		out[cid] = s.liveCountLocked(cid)
	}
	return out
}

// KnownCIDs returns the set of CIDs the state has observed (regardless of
// TTL). Used by the Replicator to enumerate candidates that may still be
// recoverable after Prune has not yet run (e.g., to opportunistically
// discover a CID via the resolver when RarestFirst's freshness window
// has just closed). Callers that need TTL-enforced freshness — e.g. the
// proof-of-storage stochastic rate denominator, which must not count
// dead entries — should use ActiveCIDs instead.
func (s *RarestFirstState) KnownCIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.entries))
	for cid := range s.entries {
		out = append(out, cid)
	}
	return out
}

// ActiveCIDs returns CIDs with at least one peer observation inside the
// TTL window (i.e. a live holder known to the local tracker). Equivalent
// to KnownCIDs() but filtered the same way PeersForCID + Count filter
// their outputs, so counts + candidate enumeration stay consistent.
// Used by the proof-of-storage challenger loop to gate candidate CIDs
// to ones we actually expect to find a peer for on the current tick.
func (s *RarestFirstState) ActiveCIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().Add(-s.ttl)
	out := make([]string, 0, len(s.entries))
	for cid, ent := range s.entries {
		for _, ts := range ent.peers {
			if ts.After(cutoff) {
				out = append(out, cid)
				break
			}
		}
	}
	return out
}

// PeersForCID returns the peerIDs currently observed to hold cid
// (within the TTL window). Used by the proof-of-storage challenger to
// pick a target for each tick.
func (s *RarestFirstState) PeersForCID(cid string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ent, ok := s.entries[cid]
	if !ok {
		return nil
	}
	cutoff := s.now().Add(-s.ttl)
	out := make([]string, 0, len(ent.peers))
	for peerID, ts := range ent.peers {
		if ts.After(cutoff) {
			out = append(out, peerID)
		}
	}
	return out
}

// KnownPeerCount returns the approximate count of distinct peers the
// state has observed across all CIDs. Used by the proof-of-storage
// challenger to tune its stochastic per-tick probability (more peers →
// lower per-peer rate, keeping aggregate challenge rate near target).
func (s *RarestFirstState) KnownPeerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]struct{})
	for _, ent := range s.entries {
		for peerID := range ent.peers {
			seen[peerID] = struct{}{}
		}
	}
	return len(seen)
}

// StartPruner launches a background goroutine that expires stale entries
// every period. Safe to call once; subsequent calls are no-ops. Stop via
// StopPruner.
func (s *RarestFirstState) StartPruner(ctx context.Context, period time.Duration) {
	if period <= 0 {
		period = s.ttl
	}
	s.mu.Lock()
	if s.pruneCancel != nil {
		s.mu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	s.pruneCancel = cancel
	s.pruneDone = make(chan struct{})
	done := s.pruneDone
	s.mu.Unlock()

	go func() {
		defer close(done)
		t := time.NewTicker(period)
		defer t.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-t.C:
				s.Prune()
			}
		}
	}()
}

// StopPruner halts the background pruner if one is running and waits for
// it to exit. Safe to call multiple times.
func (s *RarestFirstState) StopPruner() {
	s.mu.Lock()
	cancel := s.pruneCancel
	done := s.pruneDone
	s.pruneCancel = nil
	s.pruneDone = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// Prune removes entries older than the TTL. Callers that don't want a
// background goroutine can invoke this manually on their own cadence.
func (s *RarestFirstState) Prune() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().Add(-s.ttl)
	for cid, ent := range s.entries {
		for peer, ts := range ent.peers {
			if ts.Before(cutoff) || ts.Equal(cutoff) {
				delete(ent.peers, peer)
			}
		}
		if len(ent.peers) == 0 {
			delete(s.entries, cid)
			delete(s.updated, cid)
		}
	}
}
