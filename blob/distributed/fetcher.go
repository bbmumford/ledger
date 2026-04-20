/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// BlobFetchStreamID is the Aether application-level stream ID assigned to
// the blob-fetch subprotocol. It sits inside the 300-399 "file transfer"
// range reserved for bulk data flows in the Aether protocol spec — see
// _PACKAGES/aether/docs/protocol-spec.md section 4.4.
//
// When Plutus wires a concrete PeerTransport onto an aether.Session it
// should use this constant when calling OpenStream so both sides agree.
// The full 300-399 range is reserved for the distributed blob subsystem;
// future subprotocols (e.g. multi-piece range fetch, proof-of-storage
// challenges) should pick IDs from here as well.
const BlobFetchStreamID uint64 = 300

// ErrNoPeers is returned when Resolver.Find returns no candidates.
var ErrNoPeers = errors.New("distributed: no peers know this cid")

// ErrAllFetchesFailed is returned when every candidate peer failed to
// deliver a valid piece.
var ErrAllFetchesFailed = errors.New("distributed: all peer fetches failed")

// FetchRequest is the message sent over the blob-fetch stream by the
// requester. Encoded as JSON for forward compatibility — the bottleneck is
// the 256 KB payload, not the envelope.
type FetchRequest struct {
	// CID identifies the piece to fetch.
	CID string `json:"cid"`
	// Offset, if >0, requests a byte-range from within the piece. Phase 2
	// ignores this (always returns the whole piece); reserved for Phase 3
	// resumable fetches.
	Offset int64 `json:"offset,omitempty"`
	// Length, if >0, limits the returned bytes. Zero = whole piece.
	Length int64 `json:"length,omitempty"`
}

// FetchResponse is the server-side reply on the blob-fetch stream.
type FetchResponse struct {
	// CID echoes the request CID for sanity.
	CID string `json:"cid"`
	// Payload is the plaintext piece bytes. Empty payload + non-empty Error
	// indicates failure (CID not held, quota blocked, etc.).
	Payload []byte `json:"payload,omitempty"`
	// Error is a short human-readable reason for failure.
	Error string `json:"error,omitempty"`
}

// PeerTransport is the minimum surface the fetcher needs from the
// underlying wire transport. A production impl wraps an *aether.Session
// pool (opening streams on StreamID = BlobFetchStreamID); tests use a
// fake that reads from a map.
//
// FetchPiece sends FetchRequest to peerID and returns the plaintext on
// success. Implementations MUST respect ctx cancellation so a losing
// parallel fetch can abort cleanly.
type PeerTransport interface {
	FetchPiece(ctx context.Context, peerID string, req FetchRequest) (FetchResponse, error)
}

// Reputation scores a peer on a [0.0, 1.0) band where higher is better.
// Pluggable so Plutus can back it with its own storage-provider ratings.
// Default (DefaultReputation) returns 1.0 for everyone.
type Reputation interface {
	Score(peerID string) float64
}

// DefaultReputation scores every peer equally (1.0). Use until the Plutus
// reputation subsystem lands (ROADMAP.md section 2 "Reputation gating").
type DefaultReputation struct{}

// Score returns 1.0 for any peer.
func (DefaultReputation) Score(peerID string) float64 { return 1.0 }

// FetcherConfig tunes parallelism, peer selection and timeouts.
type FetcherConfig struct {
	// TopK is the number of peers to race in parallel. Default 3.
	TopK int
	// PerPeerTimeout bounds each individual FetchPiece call. Default 30s.
	PerPeerTimeout time.Duration
	// OverallTimeout, if >0, caps the whole Fetch call (across all peers).
	// Defaults to 2x PerPeerTimeout.
	OverallTimeout time.Duration
	// Reputation plugs in per-peer reputation scoring. Defaults to
	// DefaultReputation (all peers equal).
	Reputation Reputation
}

// Fetcher resolves + races parallel piece fetches over a transport and
// verifies CIDs before handing plaintext back to the caller.
type Fetcher struct {
	resolver  *Resolver
	transport PeerTransport
	cfg       FetcherConfig
}

// NewFetcher wires a Resolver to a PeerTransport. All arguments are required.
func NewFetcher(resolver *Resolver, transport PeerTransport, cfg FetcherConfig) (*Fetcher, error) {
	if resolver == nil {
		return nil, errors.New("distributed: fetcher needs a non-nil resolver")
	}
	if transport == nil {
		return nil, errors.New("distributed: fetcher needs a non-nil transport")
	}
	if cfg.TopK <= 0 {
		cfg.TopK = 3
	}
	if cfg.PerPeerTimeout <= 0 {
		cfg.PerPeerTimeout = 30 * time.Second
	}
	if cfg.OverallTimeout <= 0 {
		cfg.OverallTimeout = 2 * cfg.PerPeerTimeout
	}
	if cfg.Reputation == nil {
		cfg.Reputation = DefaultReputation{}
	}
	return &Fetcher{
		resolver:  resolver,
		transport: transport,
		cfg:       cfg,
	}, nil
}

// Fetch retrieves a single piece identified by cid. It:
//  1. resolves candidate peers via the attached Resolver,
//  2. ranks by latency × reputation, keeping the top-K,
//  3. races parallel fetches over the transport,
//  4. takes the first response whose plaintext hashes back to cid,
//  5. cancels the other in-flight requests.
//
// Returns ErrNoPeers when no peer advertises the CID, or
// ErrAllFetchesFailed when every candidate responded with an error or
// mismatched bytes.
func (f *Fetcher) Fetch(ctx context.Context, cid string) ([]byte, error) {
	data, _, err := f.FetchWithPeer(ctx, cid)
	return data, err
}

// FetchWithPeer behaves like Fetch but additionally returns the peerID of
// the candidate whose response satisfied the CID verification. Callers
// that need to attribute the fetched bytes to a specific source — for
// example, the opportunistic replicator charging AbuseGuard quotas —
// should use this variant.
//
// The returned peerID is empty on error.
func (f *Fetcher) FetchWithPeer(ctx context.Context, cid string) ([]byte, string, error) {
	if cid == "" {
		return nil, "", errors.New("distributed: Fetch needs a non-empty cid")
	}
	if f.cfg.OverallTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.cfg.OverallTimeout)
		defer cancel()
	}

	answers, err := f.resolver.Find(ctx, cid)
	if err != nil {
		return nil, "", fmt.Errorf("distributed: fetcher resolve: %w", err)
	}
	if len(answers) == 0 {
		return nil, "", ErrNoPeers
	}

	candidates := rankCandidates(answers, f.cfg.Reputation, f.cfg.TopK)
	if len(candidates) == 0 {
		return nil, "", ErrNoPeers
	}

	raceCtx, cancelRace := context.WithCancel(ctx)
	defer cancelRace()

	type result struct {
		data []byte
		err  error
		peer string
	}

	results := make(chan result, len(candidates))
	var wg sync.WaitGroup
	for _, peer := range candidates {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			attemptCtx, cancel := context.WithTimeout(raceCtx, f.cfg.PerPeerTimeout)
			defer cancel()

			resp, err := f.transport.FetchPiece(attemptCtx, peerID, FetchRequest{CID: cid})
			if err != nil {
				results <- result{err: err, peer: peerID}
				return
			}
			if resp.Error != "" {
				results <- result{err: fmt.Errorf("peer %s: %s", peerID, resp.Error), peer: peerID}
				return
			}
			if resp.CID != "" && resp.CID != cid {
				results <- result{err: fmt.Errorf("peer %s: cid echo mismatch %s", peerID, resp.CID), peer: peerID}
				return
			}
			// Canonical verification: the payload MUST hash to the expected
			// CID. Mesh trust hinges on this check; skipping it would let a
			// malicious peer poison our cache.
			if computePieceCID(resp.Payload) != cid {
				results <- result{err: fmt.Errorf("peer %s: cid verification failed", peerID), peer: peerID}
				return
			}
			results <- result{data: resp.Payload, peer: peerID}
		}(peer)
	}

	// Drain asynchronously so we don't leak the channel if early-exit wins.
	go func() {
		wg.Wait()
		close(results)
	}()

	var errs []error
	for r := range results {
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		// First valid fetch wins. Cancel the rest.
		cancelRace()
		return r.data, r.peer, nil
	}

	if ctx.Err() != nil {
		return nil, "", ctx.Err()
	}
	return nil, "", fmt.Errorf("%w: %d attempts: %v", ErrAllFetchesFailed, len(errs), errs)
}

// rankCandidates sorts PeerAnswers by score = reputation / (1 + latencyHintMs/1000).
// Highest score first. Returns at most topK entries.
//
// TODO(phase3-replication): add diversity jitter so we don't always pick the
// same 3 peers under stable conditions — ROADMAP.md "Diversity" row.
func rankCandidates(answers []PeerAnswer, rep Reputation, topK int) []string {
	type scored struct {
		peerID string
		score  float64
	}
	ranked := make([]scored, 0, len(answers))
	for _, a := range answers {
		reputation := rep.Score(a.PeerID)
		if reputation <= 0 {
			// Refuse to fetch from peers with zero reputation (bad actors).
			// This is a conservative default; Plutus may lower the floor.
			continue
		}
		latencyFactor := 1.0 + float64(a.LatencyHintMs)/1000.0
		if latencyFactor < 1.0 {
			latencyFactor = 1.0
		}
		ranked = append(ranked, scored{peerID: a.PeerID, score: reputation / latencyFactor})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].score == ranked[j].score {
			// Tie-break on PeerID for deterministic tests.
			return ranked[i].peerID < ranked[j].peerID
		}
		return ranked[i].score > ranked[j].score
	})
	if topK > 0 && len(ranked) > topK {
		ranked = ranked[:topK]
	}
	out := make([]string, 0, len(ranked))
	for _, s := range ranked {
		out = append(out, s.peerID)
	}
	return out
}

// EncodeFetchRequest / EncodeFetchResponse helpers for transport impls that
// want the canonical wire form.
func EncodeFetchRequest(r FetchRequest) ([]byte, error)   { return json.Marshal(&r) }
func DecodeFetchRequest(b []byte) (FetchRequest, error) {
	var r FetchRequest
	if err := json.Unmarshal(b, &r); err != nil {
		return FetchRequest{}, err
	}
	return r, nil
}
func EncodeFetchResponse(r FetchResponse) ([]byte, error) { return json.Marshal(&r) }
func DecodeFetchResponse(b []byte) (FetchResponse, error) {
	var r FetchResponse
	if err := json.Unmarshal(b, &r); err != nil {
		return FetchResponse{}, err
	}
	return r, nil
}
