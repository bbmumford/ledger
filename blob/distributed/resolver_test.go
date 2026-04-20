/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"encoding/json"
	"testing"
)

// stubHas lets tests specify which CIDs the resolver "holds" without
// needing a full Store.
type stubHas struct{ have map[string]bool }

func (s stubHas) Has(ctx context.Context, cid string) (bool, error) {
	return s.have[cid], nil
}

// canned QueryTopic that returns a preset list of response payloads.
type cannedQuery struct{ responses [][]byte }

func (c cannedQuery) Query(ctx context.Context, topic string, payload []byte) ([][]byte, error) {
	return c.responses, nil
}

func TestResolverOnMessageHoldsCID(t *testing.T) {
	r, err := NewResolver(
		stubHas{have: map[string]bool{"cid-1": true}},
		nil,
		ResolverConfig{PeerID: "node-1", LatencyHintMs: 42},
	)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	reqPayload, _ := json.Marshal(&FindRequest{CID: "cid-1", Nonce: "n-1"})
	resp, err := r.OnMessage(context.Background(), "peer-a", TopicBlobFind, reqPayload)
	if err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if len(resp) == 0 {
		t.Fatal("expected non-empty response when CID is held")
	}
	var parsed FindResponse
	if err := json.Unmarshal(resp, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Nonce != "n-1" {
		t.Errorf("Nonce = %q want n-1", parsed.Nonce)
	}
	if len(parsed.Answers) != 1 {
		t.Fatalf("Answers = %d want 1", len(parsed.Answers))
	}
	if parsed.Answers[0].PeerID != "node-1" {
		t.Errorf("Answer.PeerID = %q want node-1", parsed.Answers[0].PeerID)
	}
	if parsed.Answers[0].LatencyHintMs != 42 {
		t.Errorf("LatencyHintMs = %d want 42", parsed.Answers[0].LatencyHintMs)
	}
}

func TestResolverOnMessageDoesNotHaveCID(t *testing.T) {
	r, _ := NewResolver(
		stubHas{have: map[string]bool{}},
		nil,
		ResolverConfig{PeerID: "node-1"},
	)
	reqPayload, _ := json.Marshal(&FindRequest{CID: "unknown"})
	resp, err := r.OnMessage(context.Background(), "peer-a", TopicBlobFind, reqPayload)
	if err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil response for unknown CID, got %d bytes", len(resp))
	}
}

func TestResolverOnMessageEmptyCID(t *testing.T) {
	r, _ := NewResolver(stubHas{}, nil, ResolverConfig{PeerID: "node-1"})
	reqPayload, _ := json.Marshal(&FindRequest{CID: ""})
	resp, err := r.OnMessage(context.Background(), "peer", TopicBlobFind, reqPayload)
	if err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if resp != nil {
		t.Error("expected nil response for empty CID")
	}
}

func TestResolverOnMessageRejectsWrongTopic(t *testing.T) {
	r, _ := NewResolver(stubHas{}, nil, ResolverConfig{PeerID: "node-1"})
	reqPayload, _ := json.Marshal(&FindRequest{CID: "x"})
	_, err := r.OnMessage(context.Background(), "p", "other.topic", reqPayload)
	if err == nil {
		t.Error("expected error on wrong topic")
	}
}

func TestResolverFindDedups(t *testing.T) {
	// Three responses: two from peer-A (one older, one newer) and one from
	// peer-B. Final result must keep peer-A's newer record + peer-B.
	mkResp := func(peer string, ts int64) []byte {
		b, _ := json.Marshal(&FindResponse{
			Answers: []PeerAnswer{{PeerID: peer, LastSeenUnixMs: ts}},
		})
		return b
	}
	q := cannedQuery{responses: [][]byte{
		mkResp("peer-A", 100),
		mkResp("peer-B", 150),
		mkResp("peer-A", 200), // freshest for A
	}}
	r, err := NewResolver(stubHas{}, q, ResolverConfig{PeerID: "asker"})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	answers, err := r.Find(context.Background(), "cid-z")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(answers) != 2 {
		t.Fatalf("answers = %d want 2: %+v", len(answers), answers)
	}
	byPeer := map[string]int64{}
	for _, a := range answers {
		byPeer[a.PeerID] = a.LastSeenUnixMs
	}
	if byPeer["peer-A"] != 200 {
		t.Errorf("peer-A LastSeen = %d want 200", byPeer["peer-A"])
	}
	if byPeer["peer-B"] != 150 {
		t.Errorf("peer-B LastSeen = %d want 150", byPeer["peer-B"])
	}
}

func TestResolverFindEmpty(t *testing.T) {
	q := cannedQuery{responses: nil}
	r, _ := NewResolver(stubHas{}, q, ResolverConfig{PeerID: "asker"})
	answers, err := r.Find(context.Background(), "cid")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(answers) != 0 {
		t.Errorf("answers = %d want 0", len(answers))
	}
}

func TestResolverFindIgnoresMalformed(t *testing.T) {
	q := cannedQuery{responses: [][]byte{
		[]byte("not-json"),
		mustJSON(&FindResponse{Answers: []PeerAnswer{{PeerID: "peer-C", LastSeenUnixMs: 1}}}),
		[]byte(""),
	}}
	r, _ := NewResolver(stubHas{}, q, ResolverConfig{PeerID: "asker"})
	answers, err := r.Find(context.Background(), "cid")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(answers) != 1 || answers[0].PeerID != "peer-C" {
		t.Errorf("answers = %+v, want single peer-C", answers)
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestResolverFindRejectsNilEngine(t *testing.T) {
	r, _ := NewResolver(stubHas{}, nil, ResolverConfig{PeerID: "asker"})
	if _, err := r.Find(context.Background(), "cid"); err == nil {
		t.Error("expected error when engine is nil")
	}
}
