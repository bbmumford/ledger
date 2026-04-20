/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"
)

// pinningPublisher captures published topic/payload pairs for inspection.
// Separate from fakePublisher in advertiser_test.go to avoid redeclaration
// in the same _test package.
type pinningPublisher struct {
	mu       sync.Mutex
	messages []struct {
		Topic   string
		Payload []byte
	}
	fail error
}

func (f *pinningPublisher) Publish(topic string, payload []byte) error {
	if f.fail != nil {
		return f.fail
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, struct {
		Topic   string
		Payload []byte
	}{topic, append([]byte(nil), payload...)})
	return nil
}

// inlineSubscriber is a test-only PinAcceptanceSubscriber that just
// holds a single handler per requestID and lets tests call Deliver to
// inject acceptances synchronously.
type inlineSubscriber struct {
	mu       sync.Mutex
	handlers map[string]func(PinAcceptance)
}

func newInlineSubscriber() *inlineSubscriber {
	return &inlineSubscriber{handlers: map[string]func(PinAcceptance){}}
}

func (s *inlineSubscriber) SubscribeAcceptances(reqID string, h func(PinAcceptance)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[reqID] = h
}

func (s *inlineSubscriber) UnsubscribeAcceptances(reqID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handlers, reqID)
}

func (s *inlineSubscriber) Deliver(a PinAcceptance) {
	s.mu.Lock()
	h := s.handlers[a.RequestID]
	s.mu.Unlock()
	if h != nil {
		h(a)
	}
}

// fakeEscrow records CreatePin / PostBond calls and returns
// deterministic addresses so tests can assert.
type fakeEscrow struct {
	failCreate error
	failBond   error
	addrSeq    int
	mu         sync.Mutex
	created    []CreatePinParams
	bonded     []string
}

func (f *fakeEscrow) CreatePin(_ context.Context, p CreatePinParams) (string, error) {
	if f.failCreate != nil {
		return "", f.failCreate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addrSeq++
	addr := fmt.Sprintf("0xescrow%d", f.addrSeq)
	f.created = append(f.created, p)
	return addr, nil
}

func (f *fakeEscrow) PostBond(_ context.Context, addr string) (string, error) {
	if f.failBond != nil {
		return "", f.failBond
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bonded = append(f.bonded, addr)
	return "tx-" + addr, nil
}

// OpenChallenge / ExpireChallenge are unused by the pin-side tests
// (they belong to the proof-of-storage escalation path, which is
// exercised by plutus/blob's own storage_escrow_client_test.go and
// the mesh runtime_test.go). Stubbed here so fakeEscrow still
// satisfies the extended EscrowClient interface.
func (f *fakeEscrow) OpenChallenge(_ context.Context, _ [32]byte, _ [32]byte, _ [32]byte) (string, error) {
	return "", nil
}

func (f *fakeEscrow) ExpireChallenge(_ context.Context, _ [32]byte) (string, error) {
	return "", nil
}

func mustPrivKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	return priv
}

func TestPinRequest_SignVerifyRoundtrip(t *testing.T) {
	key := mustPrivKey(t)
	req := PinRequest{
		RequestID:             "req-1",
		CID:                   "bafy-example",
		DurationSeconds:       int64((7 * 24 * time.Hour).Seconds()),
		ReplicationFactor:     3,
		FeePerReplicator:      "1000000",
		TotalEscrowed:         "3000000",
		AcceptedPaymentChains: []string{"polygon"},
		RecordType:            "listing",
		IssuedAtUnixMs:        time.Now().UnixMilli(),
	}
	if err := req.Sign(key); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if err := req.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}

	// Tamper → verify fails.
	req.CID = "tampered"
	if err := req.Verify(); err == nil {
		t.Fatalf("expected verify to fail on tamper")
	}
}

func TestPinAcceptance_SignVerifyRoundtrip(t *testing.T) {
	key := mustPrivKey(t)
	ack := PinAcceptance{
		RequestID:        "req-1",
		CID:              "bafy",
		ReplicatorID:     "rep-1",
		EscrowChain:      "polygon",
		EscrowAddress:    "0xabc",
		AcceptedAtUnixMs: time.Now().UnixMilli(),
	}
	if err := ack.Sign(key); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if err := ack.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}

	// Swap pubkey → fail.
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	ack.ReplicatorPub = otherPub
	if err := ack.Verify(); err == nil {
		t.Fatalf("expected verify to fail after pubkey swap")
	}
}

func TestPinner_PublishesAndCollectsFirstN(t *testing.T) {
	pub := &pinningPublisher{}
	sub := newInlineSubscriber()
	sellerKey := mustPrivKey(t)

	pinner, err := NewPinner(pub, sub, PinnerConfig{
		IdentityKey: sellerKey,
		// 2s (not 100ms) to stay robust under -race: the test relies on
		// two synchronous handler invocations completing before the
		// outer select marks itself completed — under race scheduling
		// that can easily exceed 100ms.
		AcceptanceTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("pinner: %v", err)
	}

	req, err := pinner.BuildRequest(PinRequestParams{
		CID:                   "bafy-cid",
		Duration:              7 * 24 * time.Hour,
		ReplicationFactor:     2,
		FeePerReplicator:      big.NewInt(1_000_000),
		AcceptedPaymentChains: []string{"polygon"},
		RecordType:            "listing",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Deliver 3 acceptances in staggered order; only first 2 (timestamp-wise) should win.
	deliver := func(replicatorID string, ts int64) {
		repKey := mustPrivKey(t)
		ack := PinAcceptance{
			RequestID:        req.RequestID,
			CID:              req.CID,
			ReplicatorID:     replicatorID,
			EscrowChain:      "polygon",
			EscrowAddress:    "0x" + replicatorID,
			AcceptedAtUnixMs: ts,
		}
		if err := ack.Sign(repKey); err != nil {
			t.Fatalf("ack sign: %v", err)
		}
		sub.Deliver(ack)
	}

	// Deliver 2 acceptances — exactly matches ReplicationFactor so the
	// collection loop fills and returns. Timestamp-sort picks the earlier
	// of the two as winner[0].
	// Small sleep to guarantee PublishPinRequest has subscribed before
	// the deliveries fire (inlineSubscriber has nil-handler semantics
	// when a deliver beats the subscribe — silently drops).
	go func() {
		time.Sleep(100 * time.Millisecond)
		deliver("r1", 200)
		deliver("r3", 50) // earliest
	}()

	winners, err := pinner.PublishPinRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(winners) != 2 {
		t.Fatalf("expected 2 winners, got %d (%v)", len(winners), winners)
	}
	// Timestamp-ordered: r3 (50) then r1 (200).
	if winners[0] != "r3" {
		t.Errorf("expected r3 first, got %s", winners[0])
	}
	if winners[1] != "r1" {
		t.Errorf("expected r1 second, got %s", winners[1])
	}

	// Publisher saw exactly one request publish.
	if len(pub.messages) != 1 {
		t.Errorf("expected 1 published message, got %d", len(pub.messages))
	}
	if pub.messages[0].Topic != TopicPinRequest {
		t.Errorf("wrong topic: %s", pub.messages[0].Topic)
	}
}

func TestPinner_TimeoutReturnsWhatWeHave(t *testing.T) {
	pub := &pinningPublisher{}
	sub := newInlineSubscriber()
	sellerKey := mustPrivKey(t)

	pinner, _ := NewPinner(pub, sub, PinnerConfig{
		IdentityKey: sellerKey,
		// Was 50ms — too tight under -race. 500ms leaves headroom for
		// the single go-routine deliver() to land while still finishing
		// the test fast in the happy path.
		AcceptanceTimeout: 500 * time.Millisecond,
	})
	req, _ := pinner.BuildRequest(PinRequestParams{
		CID:               "bafy",
		Duration:          7 * 24 * time.Hour,
		ReplicationFactor: 5,
		FeePerReplicator:  big.NewInt(1_000_000),
	})

	// Pre-build the signed acceptance so the delivery goroutine only
	// has to push bytes (minimises the race between subscribe/deliver).
	repKey := mustPrivKey(t)
	ack := PinAcceptance{
		RequestID:        req.RequestID,
		CID:              req.CID,
		ReplicatorID:     "only-one",
		AcceptedAtUnixMs: 1,
	}
	if err := ack.Sign(repKey); err != nil {
		t.Fatalf("sign: %v", err)
	}
	// Only one acceptance arrives; we asked for 5. Sleep briefly so the
	// PublishPinRequest call below has definitively subscribed before we
	// deliver — otherwise the handler may be nil at deliver time and the
	// acceptance is silently dropped (inlineSubscriber semantics).
	go func() {
		time.Sleep(100 * time.Millisecond)
		sub.Deliver(ack)
	}()

	winners, err := pinner.PublishPinRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(winners) != 1 {
		t.Fatalf("expected 1 winner after timeout, got %d", len(winners))
	}
	if winners[0] != "only-one" {
		t.Errorf("unexpected winner: %s", winners[0])
	}
}

func TestPinner_RejectsInvalidSig(t *testing.T) {
	pub := &pinningPublisher{}
	sub := newInlineSubscriber()
	sellerKey := mustPrivKey(t)
	pinner, _ := NewPinner(pub, sub, PinnerConfig{IdentityKey: sellerKey})

	// Build + tamper.
	req, _ := pinner.BuildRequest(PinRequestParams{
		CID:               "bafy",
		Duration:          7 * 24 * time.Hour,
		ReplicationFactor: 1,
		FeePerReplicator:  big.NewInt(1_000_000),
	})
	req.CID = "tampered"
	_, err := pinner.PublishPinRequest(context.Background(), req)
	if err == nil {
		t.Fatalf("expected refusal to publish tampered request")
	}
}

func TestAcceptor_HappyPath(t *testing.T) {
	sellerKey := mustPrivKey(t)
	repKey := mustPrivKey(t)
	pub := &pinningPublisher{}
	escrow := &fakeEscrow{}

	policy := PinPolicyConfig{
		Enabled:             true,
		MaxTotalPinnedBytes: 0, // unlimited
		AllowedRecordTypes:  []string{"listing"},
		MaxCommitmentWeeks:  52,
		MinSellerReputation: 0.0,
		PreferredChains:     []string{"polygon"},
		PreferredChainID:    "polygon",
	}
	acceptor, err := NewPinAcceptor(PinAcceptorConfig{
		PeerID:      "rep-1",
		IdentityKey: repKey,
		Policy:      policy,
	}, escrow, pub)
	if err != nil {
		t.Fatalf("acceptor: %v", err)
	}

	// Build seller's request.
	p, _ := NewPinner(pub, nil, PinnerConfig{IdentityKey: sellerKey})
	req, _ := p.BuildRequest(PinRequestParams{
		CID:                   "bafy-1",
		Duration:              4 * 7 * 24 * time.Hour, // 4 weeks
		ReplicationFactor:     3,
		FeePerReplicator:      big.NewInt(10_000_000),
		AcceptedPaymentChains: []string{"polygon", "arbitrum"},
		RecordType:            "listing",
	})

	addr, err := acceptor.HandlePinRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if addr == "" {
		t.Fatalf("expected non-empty escrow address")
	}
	if len(escrow.created) != 1 {
		t.Fatalf("expected 1 CreatePin call, got %d", len(escrow.created))
	}
	if len(escrow.bonded) != 1 {
		t.Fatalf("expected 1 PostBond call, got %d", len(escrow.bonded))
	}
	if escrow.created[0].ChainID != "polygon" {
		t.Errorf("wrong chain: %s", escrow.created[0].ChainID)
	}

	// Acceptance was broadcast.
	found := false
	for _, msg := range pub.messages {
		if msg.Topic == TopicPinAccept {
			found = true
			ack, err := DecodePinAcceptance(msg.Payload)
			if err != nil {
				t.Errorf("decode ack: %v", err)
			}
			if ack.EscrowAddress != addr {
				t.Errorf("ack address mismatch: %s vs %s", ack.EscrowAddress, addr)
			}
			if err := ack.Verify(); err != nil {
				t.Errorf("ack verify: %v", err)
			}
		}
	}
	if !found {
		t.Errorf("no acceptance broadcast")
	}
}

func TestAcceptor_SilentPolicyRejection(t *testing.T) {
	sellerKey := mustPrivKey(t)
	repKey := mustPrivKey(t)
	pub := &pinningPublisher{}
	escrow := &fakeEscrow{}

	// Record type not allowed.
	policy := PinPolicyConfig{
		Enabled:            true,
		AllowedRecordTypes: []string{"listing"},
		MaxCommitmentWeeks: 52,
	}
	acceptor, _ := NewPinAcceptor(PinAcceptorConfig{
		IdentityKey: repKey,
		Policy:      policy,
	}, escrow, pub)

	p, _ := NewPinner(pub, nil, PinnerConfig{IdentityKey: sellerKey})
	req, _ := p.BuildRequest(PinRequestParams{
		CID:               "bafy",
		Duration:          7 * 24 * time.Hour,
		ReplicationFactor: 1,
		FeePerReplicator:  big.NewInt(1_000_000),
		RecordType:        "profile", // not in allowed list
	})
	// Clear publisher state after BuildRequest didn't publish; none should.
	pub.mu.Lock()
	pub.messages = nil
	pub.mu.Unlock()

	addr, err := acceptor.HandlePinRequest(context.Background(), req)
	if addr != "" {
		t.Errorf("expected empty addr, got %s", addr)
	}
	var pre *PolicyRejectError
	if !errors.As(err, &pre) {
		t.Fatalf("expected PolicyRejectError, got %v", err)
	}
	if len(escrow.created) != 0 {
		t.Errorf("expected no escrow creation on policy reject")
	}
	// No broadcast.
	pub.mu.Lock()
	msgs := len(pub.messages)
	pub.mu.Unlock()
	if msgs != 0 {
		t.Errorf("expected silent rejection, got %d messages", msgs)
	}
}

func TestAcceptor_InvalidSellerSigRejected(t *testing.T) {
	repKey := mustPrivKey(t)
	pub := &pinningPublisher{}
	escrow := &fakeEscrow{}
	acceptor, _ := NewPinAcceptor(PinAcceptorConfig{
		IdentityKey: repKey,
		Policy:      PinPolicyConfig{Enabled: true, MaxCommitmentWeeks: 52},
	}, escrow, pub)

	req := PinRequest{
		RequestID:         "r",
		CID:               "bafy",
		DurationSeconds:   int64((7 * 24 * time.Hour).Seconds()),
		ReplicationFactor: 1,
		FeePerReplicator:  "1000000",
		SellerPubKey:      bytes32Pub(), // random 32 bytes — not a valid Ed25519 pubkey for any existing signer
		SellerSig:         make([]byte, 64),
		IssuedAtUnixMs:    time.Now().UnixMilli(),
	}
	_, err := acceptor.HandlePinRequest(context.Background(), req)
	var pre *PolicyRejectError
	if !errors.As(err, &pre) {
		t.Fatalf("expected policy-reject from bad sig, got %v", err)
	}
}

func bytes32Pub() []byte {
	b := make([]byte, ed25519.PublicKeySize)
	_, _ = rand.Read(b)
	return b
}

func TestAcceptor_EscrowFailureDoesNotBroadcast(t *testing.T) {
	sellerKey := mustPrivKey(t)
	repKey := mustPrivKey(t)
	pub := &pinningPublisher{}
	escrow := &fakeEscrow{failCreate: errors.New("chain offline")}

	acceptor, _ := NewPinAcceptor(PinAcceptorConfig{
		IdentityKey: repKey,
		Policy: PinPolicyConfig{
			Enabled:            true,
			AllowedRecordTypes: []string{"listing"},
			MaxCommitmentWeeks: 52,
			PreferredChainID:   "polygon",
		},
	}, escrow, pub)

	p, _ := NewPinner(pub, nil, PinnerConfig{IdentityKey: sellerKey})
	req, _ := p.BuildRequest(PinRequestParams{
		CID:               "bafy",
		Duration:          7 * 24 * time.Hour,
		ReplicationFactor: 1,
		FeePerReplicator:  big.NewInt(1_000_000),
		RecordType:        "listing",
	})
	pub.mu.Lock()
	pub.messages = nil
	pub.mu.Unlock()

	_, err := acceptor.HandlePinRequest(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error on escrow failure")
	}
	pub.mu.Lock()
	msgs := len(pub.messages)
	pub.mu.Unlock()
	if msgs != 0 {
		t.Errorf("expected no broadcast on failure, got %d", msgs)
	}
}

func TestAcceptor_BondFailureDoesNotBroadcast(t *testing.T) {
	sellerKey := mustPrivKey(t)
	repKey := mustPrivKey(t)
	pub := &pinningPublisher{}
	escrow := &fakeEscrow{failBond: errors.New("insufficient funds")}

	acceptor, _ := NewPinAcceptor(PinAcceptorConfig{
		IdentityKey: repKey,
		Policy: PinPolicyConfig{
			Enabled:            true,
			AllowedRecordTypes: []string{"listing"},
			MaxCommitmentWeeks: 52,
			PreferredChainID:   "polygon",
		},
	}, escrow, pub)

	p, _ := NewPinner(pub, nil, PinnerConfig{IdentityKey: sellerKey})
	req, _ := p.BuildRequest(PinRequestParams{
		CID:               "bafy",
		Duration:          7 * 24 * time.Hour,
		ReplicationFactor: 1,
		FeePerReplicator:  big.NewInt(1_000_000),
		RecordType:        "listing",
	})
	pub.mu.Lock()
	pub.messages = nil
	pub.mu.Unlock()

	_, err := acceptor.HandlePinRequest(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error on bond failure")
	}
	pub.mu.Lock()
	msgs := len(pub.messages)
	pub.mu.Unlock()
	if msgs != 0 {
		t.Errorf("expected no broadcast on bond failure, got %d", msgs)
	}
	// Escrow was created but never bonded (the caller must unwind manually —
	// the escrow contract has a seller-side refundUnbonded path).
	if len(escrow.created) != 1 {
		t.Errorf("expected 1 created escrow, got %d", len(escrow.created))
	}
	if len(escrow.bonded) != 0 {
		t.Errorf("expected 0 bonded escrows, got %d", len(escrow.bonded))
	}
}

func TestAcceptor_CapacityGate(t *testing.T) {
	repKey := mustPrivKey(t)
	sellerKey := mustPrivKey(t)
	pub := &pinningPublisher{}
	escrow := &fakeEscrow{}
	acceptor, _ := NewPinAcceptor(PinAcceptorConfig{
		IdentityKey: repKey,
		Policy: PinPolicyConfig{
			Enabled:             true,
			MaxTotalPinnedBytes: 100,
			AllowedRecordTypes:  []string{"listing"},
			MaxCommitmentWeeks:  52,
		},
		CurrentPinnedBytes: func() int64 { return 200 }, // over capacity
	}, escrow, pub)

	p, _ := NewPinner(pub, nil, PinnerConfig{IdentityKey: sellerKey})
	req, _ := p.BuildRequest(PinRequestParams{
		CID: "bafy", Duration: 7 * 24 * time.Hour, ReplicationFactor: 1,
		FeePerReplicator: big.NewInt(1_000_000), RecordType: "listing",
	})
	_, err := acceptor.HandlePinRequest(context.Background(), req)
	var pre *PolicyRejectError
	if !errors.As(err, &pre) {
		t.Fatalf("expected policy reject, got %v", err)
	}
}
