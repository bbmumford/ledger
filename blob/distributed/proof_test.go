/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// fakePieceSource is an in-memory PieceSource for responder tests.
type fakePieceSource struct {
	pieces map[string][]byte
}

func (f *fakePieceSource) Get(_ context.Context, cid string) ([]byte, error) {
	p, ok := f.pieces[cid]
	if !ok {
		return nil, ErrBlobNotFound
	}
	return p, nil
}

func (f *fakePieceSource) Has(_ context.Context, cid string) (bool, error) {
	_, ok := f.pieces[cid]
	return ok, nil
}

// routedTransport pipes an issuer's Ask() call directly to a
// ChallengeResponder.OnMessage() in-process, skipping any actual
// gossip wire.
type routedTransport struct {
	responder *ChallengeResponder
	// On Ask, inject a hook that can override the returned bytes
	// (for wrong-hash and timeout scenarios). Nil = pass through.
	intercept func(raw []byte) ([]byte, error)
}

func (r *routedTransport) Ask(ctx context.Context, peerID string, payload []byte) ([]byte, error) {
	if r.intercept != nil {
		return r.intercept(payload)
	}
	return r.responder.OnMessage(ctx, peerID, r.responder.Topic(), payload)
}

func mustKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	return priv
}

func TestChallenge_SuccessRoundtrip(t *testing.T) {
	piece := []byte("hello distributed blob world — this is a piece.")
	cid := computePieceCID(piece)

	key := mustKey(t)
	store := &fakePieceSource{pieces: map[string][]byte{cid: piece}}
	responder, err := NewChallengeResponder(store, ChallengeResponderConfig{
		PeerID:      "replicator-1",
		IdentityKey: key,
	})
	if err != nil {
		t.Fatalf("responder: %v", err)
	}

	rt := &routedTransport{responder: responder}
	issuer, err := NewChallengeIssuer(rt, ChallengeIssuerConfig{IssuerPeerID: "issuer-a"})
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}

	res, err := issuer.Challenge(context.Background(), "replicator-1", cid)
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}
	if !res.OK {
		t.Fatalf("challenge expected OK, got %+v", res)
	}
	if res.Response == nil {
		t.Fatalf("expected response")
	}
	if res.Response.CID != cid {
		t.Errorf("cid mismatch")
	}
	if err := issuer.VerifyResponse(res.Response, piece); err != nil {
		t.Errorf("VerifyResponse: %v", err)
	}
}

func TestChallenge_WrongHashFailsSig(t *testing.T) {
	piece := []byte("the real piece")
	cid := computePieceCID(piece)
	key := mustKey(t)

	store := &fakePieceSource{pieces: map[string][]byte{cid: piece}}
	responder, _ := NewChallengeResponder(store, ChallengeResponderConfig{
		PeerID: "r", IdentityKey: key,
	})

	// Intercept: tamper with the returned hash. Because the sig is computed
	// over the original hash and we return a mutated one, sig verification
	// fails at the issuer.
	rt := &routedTransport{
		responder: responder,
		intercept: func(raw []byte) ([]byte, error) {
			respRaw, err := responder.OnMessage(context.Background(), "r", responder.Topic(), raw)
			if err != nil || respRaw == nil {
				return respRaw, err
			}
			var resp ProofResponse
			_ = json.Unmarshal(respRaw, &resp)
			resp.Hash = []byte("deliberately-wrong-hash-deliberately-wrong-hash!")[:32]
			return json.Marshal(&resp)
		},
	}
	issuer, _ := NewChallengeIssuer(rt, ChallengeIssuerConfig{IssuerPeerID: "i"})
	res, err := issuer.Challenge(context.Background(), "r", cid)
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}
	if res.OK {
		t.Fatalf("expected NOT ok; reason=%s", res.Reason)
	}
	if res.Reason != "sig_fail" {
		t.Errorf("expected sig_fail, got %s", res.Reason)
	}
}

func TestChallenge_TimeoutRecordsMiss(t *testing.T) {
	cid := computePieceCID([]byte("no-such-piece"))
	key := mustKey(t)

	store := &fakePieceSource{pieces: map[string][]byte{}}
	responder, _ := NewChallengeResponder(store, ChallengeResponderConfig{
		PeerID: "r", IdentityKey: key,
	})

	rt := &routedTransport{
		responder: responder,
		intercept: func(raw []byte) ([]byte, error) {
			// Simulate a slow peer that never replies. We make the
			// timeout shorter via the issuer cfg so the test is fast.
			return nil, context.DeadlineExceeded
		},
	}
	issuer, _ := NewChallengeIssuer(rt, ChallengeIssuerConfig{
		IssuerPeerID:    "i",
		ResponseTimeout: 50 * time.Millisecond,
	})

	res, err := issuer.Challenge(context.Background(), "r", cid)
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}
	if res.OK {
		t.Errorf("expected miss")
	}
	if res.Reason != "timeout" {
		t.Errorf("expected timeout, got %s", res.Reason)
	}
}

func TestChallenge_CIDNotHeldReturnsMiss(t *testing.T) {
	// Responder genuinely doesn't have the CID. OnMessage returns (nil, nil),
	// which the routedTransport surfaces as empty_response.
	key := mustKey(t)
	store := &fakePieceSource{pieces: map[string][]byte{}}
	responder, _ := NewChallengeResponder(store, ChallengeResponderConfig{
		PeerID: "r", IdentityKey: key,
	})
	rt := &routedTransport{responder: responder}

	issuer, _ := NewChallengeIssuer(rt, ChallengeIssuerConfig{IssuerPeerID: "i"})
	res, err := issuer.Challenge(context.Background(), "r", computePieceCID([]byte("nope")))
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}
	if res.OK {
		t.Errorf("expected miss")
	}
	if res.Reason != "empty_response" {
		t.Errorf("expected empty_response, got %s", res.Reason)
	}
}

func TestChallenge_WrongKeyRejected(t *testing.T) {
	piece := []byte("piece bytes")
	cid := computePieceCID(piece)
	responderKey := mustKey(t)

	store := &fakePieceSource{pieces: map[string][]byte{cid: piece}}
	responder, _ := NewChallengeResponder(store, ChallengeResponderConfig{
		PeerID: "r", IdentityKey: responderKey,
	})

	// Intercept: swap the pubkey field to a different key. The sig was
	// computed under responderKey but the message now claims to be from
	// another key — verification fails.
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	rt := &routedTransport{
		responder: responder,
		intercept: func(raw []byte) ([]byte, error) {
			respRaw, err := responder.OnMessage(context.Background(), "r", responder.Topic(), raw)
			if err != nil || respRaw == nil {
				return respRaw, err
			}
			var resp ProofResponse
			_ = json.Unmarshal(respRaw, &resp)
			resp.PubKey = otherPub
			return json.Marshal(&resp)
		},
	}
	issuer, _ := NewChallengeIssuer(rt, ChallengeIssuerConfig{IssuerPeerID: "i"})
	res, _ := issuer.Challenge(context.Background(), "r", cid)
	if res.OK {
		t.Errorf("expected rejection on pubkey swap")
	}
	if res.Reason != "sig_fail" {
		t.Errorf("expected sig_fail, got %s", res.Reason)
	}
}

func TestVerifyResponse_LocalPiece(t *testing.T) {
	piece := []byte("verify-against-local-piece")
	key := mustKey(t)
	nonce, _ := NewNonce()
	cid := computePieceCID(piece)
	resp := BuildResponseFor(cid, piece, nonce, key)

	issuer, _ := NewChallengeIssuer(nil, ChallengeIssuerConfig{IssuerPeerID: "i"})
	if err := issuer.VerifyResponse(&resp, piece); err != nil {
		t.Errorf("VerifyResponse: %v", err)
	}

	// Wrong piece → fail.
	if err := issuer.VerifyResponse(&resp, []byte("wrong")); err == nil {
		t.Errorf("expected hash mismatch")
	}
}

func TestComputeProofHash_Deterministic(t *testing.T) {
	p := []byte("abc")
	n := []byte("nonce12345nonce12345nonce12345xx") // 32 bytes
	h1 := ComputeProofHash(p, n)
	h2 := ComputeProofHash(p, n)
	if !bytes.Equal(h1, h2) {
		t.Fatalf("expected deterministic hash")
	}
	if len(h1) != 32 {
		t.Fatalf("expected 32-byte hash, got %d", len(h1))
	}
}

func TestChallenge_RecentResultsRing(t *testing.T) {
	piece := []byte("ring-test")
	cid := computePieceCID(piece)
	key := mustKey(t)
	store := &fakePieceSource{pieces: map[string][]byte{cid: piece}}
	responder, _ := NewChallengeResponder(store, ChallengeResponderConfig{
		PeerID: "r", IdentityKey: key,
	})
	rt := &routedTransport{responder: responder}
	issuer, _ := NewChallengeIssuer(rt, ChallengeIssuerConfig{IssuerPeerID: "i"})

	for i := 0; i < 5; i++ {
		if _, err := issuer.Challenge(context.Background(), "r", cid); err != nil {
			t.Fatalf("challenge %d: %v", i, err)
		}
	}
	got := issuer.RecentResults()
	if len(got) != 5 {
		t.Fatalf("expected 5 results, got %d", len(got))
	}
	for i, r := range got {
		if !r.OK {
			t.Errorf("result %d expected OK", i)
		}
	}
}

// TestChallenge_IssuerRejectsSignedButFabricatedHash covers the Fix-3
// attack: a peer with a valid Ed25519 identity key signs a hash
// computed over bytes it never actually held. Without the PieceSource
// cross-check the issuer would accept the response (signature is good,
// CID + nonce bind correctly). With the cross-check the issuer refuses
// because the claimed hash does not match its local copy.
func TestChallenge_IssuerRejectsSignedButFabricatedHash(t *testing.T) {
	realPiece := []byte("the real piece that the issuer actually holds")
	cid := computePieceCID(realPiece)
	responderKey := mustKey(t)

	// Responder side: don't actually know the real piece — only hold a
	// different one, but sign a hash over the fake piece. The
	// routedTransport intercept returns a response whose Hash is
	// computed over FAKE content, signed with the responder's key.
	fakePiece := []byte("totally different content the attacker made up!")

	responderStore := &fakePieceSource{pieces: map[string][]byte{cid: fakePiece}}
	responder, _ := NewChallengeResponder(responderStore, ChallengeResponderConfig{
		PeerID: "r", IdentityKey: responderKey,
	})
	rt := &routedTransport{responder: responder}

	// Issuer DOES hold the real piece locally — the cross-check will
	// hash realPiece and notice responder.Hash (from fakePiece) doesn't
	// match.
	issuerStore := &fakePieceSource{pieces: map[string][]byte{cid: realPiece}}
	issuer, err := NewChallengeIssuerWithPieceSource(rt, issuerStore, ChallengeIssuerConfig{IssuerPeerID: "i"})
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}

	res, err := issuer.Challenge(context.Background(), "r", cid)
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}
	if res.OK {
		t.Fatalf("issuer accepted a fabricated-hash response; wanted rejection")
	}
	if res.Reason != "wrong_hash" {
		t.Fatalf("expected reason=wrong_hash, got %s", res.Reason)
	}
}

// TestChallenge_IssuerSkipsCrossCheckWhenPieceMissing documents the
// graceful fallback: if the issuer doesn't hold the piece locally, the
// cross-check is skipped (liveness over strict verification — we can't
// penalize a responder for bytes we can't independently verify).
func TestChallenge_IssuerSkipsCrossCheckWhenPieceMissing(t *testing.T) {
	piece := []byte("a piece the responder holds and the issuer does not")
	cid := computePieceCID(piece)
	responderKey := mustKey(t)

	responderStore := &fakePieceSource{pieces: map[string][]byte{cid: piece}}
	responder, _ := NewChallengeResponder(responderStore, ChallengeResponderConfig{
		PeerID: "r", IdentityKey: responderKey,
	})
	rt := &routedTransport{responder: responder}

	// Issuer PieceSource is empty — no cross-check possible.
	issuerStore := &fakePieceSource{pieces: map[string][]byte{}}
	issuer, _ := NewChallengeIssuerWithPieceSource(rt, issuerStore, ChallengeIssuerConfig{IssuerPeerID: "i"})

	res, err := issuer.Challenge(context.Background(), "r", cid)
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected OK (fallback to signature-only), got reason=%s", res.Reason)
	}
}

// Sanity: responder rejects wrong-topic calls.
func TestResponder_WrongTopicRejected(t *testing.T) {
	key := mustKey(t)
	responder, _ := NewChallengeResponder(&fakePieceSource{pieces: map[string][]byte{}}, ChallengeResponderConfig{
		PeerID: "r", IdentityKey: key,
	})
	_, err := responder.OnMessage(context.Background(), "x", "plutus.blob.find", []byte("{}"))
	if err == nil {
		t.Fatalf("expected error on wrong topic")
	}
	if !errors.Is(err, err) { // sanity: error is set
		t.Fatalf("expected non-nil error sentinel")
	}
}
