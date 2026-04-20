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
	"time"
)

// TopicBlobFind is the Whisper RequestResponse topic used to ask "who has
// piece CID X?". Callers MUST pre-register this topic with their engine
// using whisper.RequestResponse mode and attach the Resolver as the
// topic handler (see Resolver.Handler).
const TopicBlobFind = "plutus.blob.find"

// FindRequest is the payload sent on a plutus.blob.find query.
type FindRequest struct {
	// CID is the piece we're looking for.
	CID string `json:"cid"`
	// RequesterPeerID is the asker's identity. Responders may use this for
	// tit-for-tat bookkeeping (Phase 3).
	RequesterPeerID string `json:"requesterPeerId,omitempty"`
	// Nonce is an optional client-scoped ID; responses echo it back so the
	// caller can correlate across cover-query batches.
	Nonce string `json:"nonce,omitempty"`
}

// FindResponse is the payload returned by a peer that holds the CID.
// An empty PeerAnswer list encodes "I do not hold it" — in that case the
// responder should return nil bytes rather than a wrapped empty response.
type FindResponse struct {
	Answers []PeerAnswer `json:"answers"`
	Nonce   string       `json:"nonce,omitempty"`
}

// PeerAnswer describes one peer that claims to hold a CID.
type PeerAnswer struct {
	// PeerID is the responder's stable mesh identifier. Required.
	PeerID string `json:"peerId"`
	// LastSeenUnixMs is when the responder most recently observed the CID
	// in its own cache. For self-answers this is time.Now().
	LastSeenUnixMs int64 `json:"lastSeenMs"`
	// LatencyHintMs is a best-effort RTT hint in milliseconds (0 if
	// unknown). The fetcher multiplies this by reputation to rank peers.
	LatencyHintMs int64 `json:"latencyMs,omitempty"`
}

// QueryTopic is the subset of *whisper.Engine used by the Resolver caller
// side. Callers pass the same *whisper.Engine that registered the topic.
type QueryTopic interface {
	Query(ctx context.Context, topic string, payload []byte) ([][]byte, error)
}

// HasChecker is the subset of Store used by the Resolver responder side.
// Store satisfies this via Has.
type HasChecker interface {
	Has(ctx context.Context, cid string) (bool, error)
}

// ResolverConfig tunes responder behaviour + self-identity.
type ResolverConfig struct {
	// PeerID identifies this node. Required.
	PeerID string
	// Topic overrides TopicBlobFind (tests).
	Topic string
	// Now, if set, replaces time.Now() for deterministic tests.
	Now func() time.Time
	// LatencyHintMs is reported back to askers as this node's RTT hint.
	// Zero means "unknown" (askers may fall back to measuring via Aether).
	LatencyHintMs int64
}

// Resolver owns both halves of the blob-find protocol:
//
//   - Responder: answers incoming Query on TopicBlobFind.
//   - Caller:    issues Find queries and deduplicates responses.
//
// Construct once per node and register as the topic handler.
type Resolver struct {
	store  HasChecker
	engine QueryTopic
	cfg    ResolverConfig
	topic  string
	now    func() time.Time
}

// NewResolver constructs a Resolver. The engine may be nil on a
// responder-only node (tests); in that case Find will return an error.
func NewResolver(store HasChecker, engine QueryTopic, cfg ResolverConfig) (*Resolver, error) {
	if store == nil {
		return nil, errors.New("distributed: resolver needs a non-nil store")
	}
	if cfg.PeerID == "" {
		return nil, errors.New("distributed: resolver needs a non-empty PeerID")
	}
	topic := cfg.Topic
	if topic == "" {
		topic = TopicBlobFind
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Resolver{
		store:  store,
		engine: engine,
		cfg:    cfg,
		topic:  topic,
		now:    now,
	}, nil
}

// Topic returns the effective topic name. Callers pass this to
// engine.RegisterTopic(...).
func (r *Resolver) Topic() string { return r.topic }

// ─── Responder side ────────────────────────────────────────────────────────

// OnMessage implements the whisper.TopicHandler contract. On a RequestResponse
// topic this is invoked once per incoming query; the returned bytes are sent
// back to the asker. Returning (nil, nil) means "I don't hold it; no reply".
func (r *Resolver) OnMessage(ctx context.Context, from string, topic string, payload []byte) ([]byte, error) {
	if topic != r.topic {
		return nil, fmt.Errorf("distributed: resolver topic mismatch: %q", topic)
	}
	req, err := decodeFindRequest(payload)
	if err != nil {
		return nil, err
	}
	if req.CID == "" {
		return nil, nil
	}
	has, err := r.store.Has(ctx, req.CID)
	if err != nil {
		return nil, fmt.Errorf("distributed: resolver has-check: %w", err)
	}
	if !has {
		return nil, nil
	}
	resp := FindResponse{
		Answers: []PeerAnswer{{
			PeerID:         r.cfg.PeerID,
			LastSeenUnixMs: r.now().UnixMilli(),
			LatencyHintMs:  r.cfg.LatencyHintMs,
		}},
		Nonce: req.Nonce,
	}
	out, err := json.Marshal(&resp)
	if err != nil {
		return nil, fmt.Errorf("distributed: resolver marshal: %w", err)
	}
	return out, nil
}

// ─── Caller side ───────────────────────────────────────────────────────────

// Find broadcasts a FindRequest on the topic and aggregates responses. Returns
// an empty slice when nobody claims the CID (NOT an error). Duplicate
// PeerAnswer entries (same PeerID) are collapsed, keeping the freshest one.
func (r *Resolver) Find(ctx context.Context, cid string) ([]PeerAnswer, error) {
	if r.engine == nil {
		return nil, errors.New("distributed: resolver has no engine for Find")
	}
	if cid == "" {
		return nil, errors.New("distributed: Find needs a non-empty cid")
	}
	req := FindRequest{
		CID:             cid,
		RequesterPeerID: r.cfg.PeerID,
	}
	payload, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("distributed: Find marshal: %w", err)
	}

	// TODO(phase3-privacy): batch this query with k-1 dummy queries for
	// random CIDs before publishing so a passive observer cannot correlate
	// the real CID with the asker. See ROADMAP.md "Query correlation".
	raw, err := r.engine.Query(ctx, r.topic, payload)
	if err != nil {
		return nil, fmt.Errorf("distributed: Find query: %w", err)
	}
	return dedupPeerAnswers(decodeAnswers(raw)), nil
}

// ─── helpers ───────────────────────────────────────────────────────────────

func decodeFindRequest(b []byte) (*FindRequest, error) {
	if len(b) == 0 {
		return nil, errors.New("distributed: empty find-request")
	}
	var req FindRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return nil, fmt.Errorf("distributed: find-request decode: %w", err)
	}
	return &req, nil
}

func decodeAnswers(responses [][]byte) []PeerAnswer {
	var out []PeerAnswer
	for _, raw := range responses {
		if len(raw) == 0 {
			continue
		}
		var resp FindResponse
		if err := json.Unmarshal(raw, &resp); err != nil {
			// Skip malformed responses rather than fail the whole query —
			// one bad peer should not deny us the rest.
			continue
		}
		for _, a := range resp.Answers {
			if a.PeerID == "" {
				continue
			}
			out = append(out, a)
		}
	}
	return out
}

// dedupPeerAnswers keeps the freshest record per PeerID. Order of the
// remaining answers is stable by first appearance.
func dedupPeerAnswers(in []PeerAnswer) []PeerAnswer {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]int, len(in))
	out := make([]PeerAnswer, 0, len(in))
	for _, a := range in {
		if idx, ok := seen[a.PeerID]; ok {
			if a.LastSeenUnixMs > out[idx].LastSeenUnixMs {
				out[idx] = a
			}
			continue
		}
		seen[a.PeerID] = len(out)
		out = append(out, a)
	}
	return out
}

