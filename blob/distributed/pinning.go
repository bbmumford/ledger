/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

// Paid-pinning workflow.
//
// OVERVIEW:
//   On top of opportunistic replication (which happens automatically as
//   viewers cache pieces), sellers can optionally pay for GUARANTEED
//   retention on N replicator nodes for a fixed duration.
//
// FLOW:
//   1. Seller publishes a PinRequest on `plutus.<fp>.blob.pin.request`
//      (Whisper BroadcastOnly). The request is signed by the seller's
//      Ed25519 identity.
//   2. Eligible replicators evaluate the request against their local
//      PinPolicyConfig. If all conditions pass, they:
//        a. Create their own PlutusStorageEscrow.sol instance on-chain
//           (seller pre-approved totalEscrowed to the factory; the
//           replicator calls bondReplicator to post 10% bond).
//        b. Broadcast a PinAcceptance back on
//           `plutus.<fp>.blob.pin.accept`.
//   3. The seller collects acceptances; the first N (timestamp-ordered)
//      win the slots. Others can silently drop.
//
// ON-CHAIN ESCROW INTEGRATION:
//   This package defines an EscrowClient INTERFACE only. The concrete
//   implementation lives in `_PACKAGES/plutus/blob/` and plugs in at
//   mesh_embed wiring time. Tests pass a mock.
//
// WHY SILENT REJECTIONS:
//   If every ineligible replicator broadcast a NACK, the mesh would see
//   N-of-100 peers × noise per pin request. Instead, we rely on
//   positive ACKs only. Sellers time-out if no N accept within a short
//   window.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"
)

// TopicPinRequest is the BroadcastOnly topic on which sellers publish
// pin requests. Consumers may prefix with a fingerprint (plutus.<fp>)
// to scope to a specific maintainer namespace; this constant is the
// global suffix and callers compose the fingerprint at registration
// time.
const TopicPinRequest = "plutus.blob.pin.request"

// TopicPinAccept is the BroadcastOnly topic on which replicators
// broadcast their acceptance of a pin request. Same namespace rules.
const TopicPinAccept = "plutus.blob.pin.accept"

// DefaultAcceptanceTimeout bounds how long a seller waits for N
// replicator acceptances before declaring the pin "short-filled".
const DefaultAcceptanceTimeout = 2 * time.Minute

// DefaultBondDeadlineSkew is how far in the future the seller will
// set the bond-deadline on the escrow contract, giving accepted
// replicators time to post their bond before the seller can refund.
const DefaultBondDeadlineSkew = 1 * time.Hour

// PinRequest is the payload seller broadcasts. Signed by the seller's
// Ed25519 identity key over the canonical bytes returned by
// PinRequest.signBytes().
type PinRequest struct {
	// RequestID uniquely identifies this pin request. Replicators use
	// it in their PinAcceptance so the seller can correlate.
	RequestID string `json:"requestId"`
	// CID is the content-address of the piece/manifest to pin.
	CID string `json:"cid"`
	// Duration in seconds. Must be a whole number of weeks.
	DurationSeconds int64 `json:"durationSeconds"`
	// ReplicationFactor is how many replicators the seller wants.
	ReplicationFactor int `json:"replicationFactor"`
	// FeePerReplicator in the smallest on-chain unit (e.g. USDC satoshis).
	FeePerReplicator string `json:"feePerReplicator"` // big.Int string
	// TotalEscrowed = FeePerReplicator × ReplicationFactor; informational,
	// replicators do not read it.
	TotalEscrowed string `json:"totalEscrowed"`
	// AcceptedPaymentChains is the seller's allowlist of chains the
	// replicator's escrow contract may be deployed on.
	AcceptedPaymentChains []string `json:"acceptedPaymentChains"`
	// RecordType binds the pin to a use-case (listing, evidence,
	// avatar, ...). Replicators filter by this in their policy.
	RecordType string `json:"recordType"`
	// SellerPubKey is the seller's Ed25519 public key (32 bytes).
	SellerPubKey []byte `json:"sellerPubKey"`
	// SellerSig is Ed25519(canonical(this request without SellerSig)).
	SellerSig []byte `json:"sellerSig,omitempty"`
	// IssuedAtUnixMs is the wall-clock time the request was built.
	IssuedAtUnixMs int64 `json:"issuedAtMs"`
	// SellerReputation is a hint the seller publishes; replicators
	// may independently verify via the reputation subsystem but
	// including it lets low-reputation requests be filtered fast.
	SellerReputation float64 `json:"sellerReputation,omitempty"`
}

// PinAcceptance is what a replicator broadcasts on TopicPinAccept
// after it has (a) evaluated policy and (b) deployed the on-chain
// escrow. Presence of EscrowAddress tells the seller to consider
// this replicator committed.
type PinAcceptance struct {
	RequestID      string `json:"requestId"`
	CID            string `json:"cid"`
	ReplicatorID   string `json:"replicatorId"`
	ReplicatorPub  []byte `json:"replicatorPub"`
	EscrowChain    string `json:"escrowChain"`
	EscrowAddress  string `json:"escrowAddress"`
	AcceptedAtUnixMs int64 `json:"acceptedAtMs"`
	Sig            []byte `json:"sig,omitempty"`
}

// ReplicatorID is a lightweight alias so call sites reading
// PublishPinRequest return values self-document.
type ReplicatorID = string

// signPayload returns the canonical message bytes we sign. Same
// approach as proof.go (newline-delimited, no JSON key order deps).
func (r PinRequest) signPayload() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(r.RequestID)
	buf.WriteByte('\n')
	buf.WriteString(r.CID)
	buf.WriteByte('\n')
	fmt.Fprintf(buf, "%d\n%d\n", r.DurationSeconds, r.ReplicationFactor)
	buf.WriteString(r.FeePerReplicator)
	buf.WriteByte('\n')
	buf.WriteString(r.TotalEscrowed)
	buf.WriteByte('\n')
	for _, c := range r.AcceptedPaymentChains {
		buf.WriteString(c)
		buf.WriteByte(',')
	}
	buf.WriteByte('\n')
	buf.WriteString(r.RecordType)
	buf.WriteByte('\n')
	buf.Write(r.SellerPubKey)
	buf.WriteByte('\n')
	fmt.Fprintf(buf, "%d", r.IssuedAtUnixMs)
	return buf.Bytes()
}

// Sign fills in r.SellerSig. Caller must have already set SellerPubKey
// + other fields.
func (r *PinRequest) Sign(key ed25519.PrivateKey) error {
	if len(key) != ed25519.PrivateKeySize {
		return errors.New("distributed: bad signer key")
	}
	r.SellerPubKey = key.Public().(ed25519.PublicKey)
	r.SellerSig = ed25519.Sign(key, r.signPayload())
	return nil
}

// Verify checks r.SellerSig against r.SellerPubKey. Returns a clear
// error on any mismatch. An unsigned request (SellerSig == nil) is
// rejected.
func (r PinRequest) Verify() error {
	if len(r.SellerPubKey) != ed25519.PublicKeySize {
		return errors.New("distributed: bad seller pubkey")
	}
	if len(r.SellerSig) == 0 {
		return errors.New("distributed: request not signed")
	}
	if !ed25519.Verify(ed25519.PublicKey(r.SellerPubKey), r.signPayload(), r.SellerSig) {
		return errors.New("distributed: seller sig verification failed")
	}
	return nil
}

// acceptanceSignBytes builds the canonical bytes for a PinAcceptance
// signature (everything except Sig).
func (a PinAcceptance) signBytes() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(a.RequestID)
	buf.WriteByte('\n')
	buf.WriteString(a.CID)
	buf.WriteByte('\n')
	buf.WriteString(a.ReplicatorID)
	buf.WriteByte('\n')
	buf.Write(a.ReplicatorPub)
	buf.WriteByte('\n')
	buf.WriteString(a.EscrowChain)
	buf.WriteByte('\n')
	buf.WriteString(a.EscrowAddress)
	buf.WriteByte('\n')
	fmt.Fprintf(buf, "%d", a.AcceptedAtUnixMs)
	return buf.Bytes()
}

// Sign fills in a.Sig.
func (a *PinAcceptance) Sign(key ed25519.PrivateKey) error {
	if len(key) != ed25519.PrivateKeySize {
		return errors.New("distributed: bad signer key")
	}
	a.ReplicatorPub = key.Public().(ed25519.PublicKey)
	a.Sig = ed25519.Sign(key, a.signBytes())
	return nil
}

// Verify checks a.Sig against a.ReplicatorPub.
func (a PinAcceptance) Verify() error {
	if len(a.ReplicatorPub) != ed25519.PublicKeySize {
		return errors.New("distributed: bad replicator pubkey")
	}
	if len(a.Sig) == 0 {
		return errors.New("distributed: acceptance not signed")
	}
	if !ed25519.Verify(ed25519.PublicKey(a.ReplicatorPub), a.signBytes(), a.Sig) {
		return errors.New("distributed: acceptance sig verification failed")
	}
	return nil
}

// ─── EscrowClient interface (plugs into plutus) ───────────────────

// EscrowClient is the minimum surface pin-side code needs to deploy
// and manage a PlutusStorageEscrow.sol instance. The concrete impl
// lives in plutus/blob; tests pass a mock.
//
// The interface covers THREE conceptual subsystems that all hit the
// same contract family:
//
//   1. Pinning deployment      — CreatePin, PostBond.
//   2. Proof-of-storage escalation — OpenChallenge, ExpireChallenge.
//   3. Replicator bond reclaim — WithdrawBond.
//
// All three live on one interface because the mesh runtime injects a
// single EscrowClient at startup and routes every on-chain mutation
// through it. Callers that only need a subset (e.g. a browser-WASM
// seller that never replicates) can safely pass an impl that returns
// an explicit not-supported error on the unused methods — the runtime
// falls back gracefully when any mutating call errors.
type EscrowClient interface {
	// CreatePin deploys a new PlutusStorageEscrow instance for
	// (cid, replicator) on the chosen chain. Seller must have already
	// approved FeePerReplicator to the escrow factory (or directly to
	// the deploying address, depending on the chain adapter).
	//
	// On successful deployment returns the escrow address.
	CreatePin(ctx context.Context, params CreatePinParams) (string, error)
	// PostBond deposits the 10% bond on behalf of the replicator. Must
	// be called after CreatePin by the replicator side. Returns the
	// tx hash.
	PostBond(ctx context.Context, escrowAddress string) (string, error)
	// OpenChallenge submits the challenger's (nonce, expectedHash)
	// pair against the PlutusStorageEscrow contract for cid.
	// `cid` is the keccak256(cid-ASCII) contract key; `nonce` is a
	// fresh 32-byte random; `expectedHash` MUST be
	// keccak256(piece || nonce) — use ComputeOnChainExpectedHash in
	// this package. Returns the tx hash. Error is non-nil on chain
	// failure; the mesh loop does NOT retry automatically.
	OpenChallenge(ctx context.Context, cid [32]byte, nonce [32]byte, expectedHash [32]byte) (string, error)
	// ExpireChallenge submits expireChallenge() on the contract for
	// cid, AFTER the 24h window has elapsed. Callable by any peer.
	// A revert with ChallengeWindowNotExpired / NoOpenChallenge is
	// BENIGN (the replicator responded in time) — the client impl
	// should return that as an error, and the mesh loop classifies it.
	ExpireChallenge(ctx context.Context, cid [32]byte) (string, error)
}

// CreatePinParams is the payload CreatePin receives.
type CreatePinParams struct {
	ChainID           string
	CID               string
	Seller            string
	Replicator        string
	Duration          time.Duration
	ReplicationFactor int
	FeePerReplicator  *big.Int
	BondDeadline      time.Time
}

// ─── Seller side ───────────────────────────────────────────────────

// PinPublisher is the minimum surface Pinner needs to broadcast on
// Whisper. Matches the whisper.Engine Publish signature.
type PinPublisher interface {
	Publish(topic string, payload []byte) error
}

// PinAcceptanceSubscriber is the minimum surface the seller needs to
// register a BroadcastOnly subscriber for incoming acceptances. In
// production this is whisper.Engine.Subscribe + a TopicHandler; we
// decouple here so tests can inject acceptances directly.
//
// See also: whisper.Subscriber.OnBroadcast.
type PinAcceptanceSubscriber interface {
	SubscribeAcceptances(requestID string, handler func(PinAcceptance))
	UnsubscribeAcceptances(requestID string)
}

// PinnerConfig tunes the seller-side publisher.
type PinnerConfig struct {
	// PeerID is the seller's mesh identifier (logged).
	PeerID string
	// IdentityKey signs each PinRequest. Required.
	IdentityKey ed25519.PrivateKey
	// RequestTopic overrides TopicPinRequest (tests / namespacing).
	RequestTopic string
	// AcceptanceTimeout bounds how long PublishPinRequest waits for
	// replicator acceptances. Defaults to DefaultAcceptanceTimeout.
	AcceptanceTimeout time.Duration
	// Now, if set, replaces time.Now() for deterministic tests.
	Now func() time.Time
}

// Pinner publishes pin requests on behalf of a seller and collects
// acceptances from eligible replicators.
type Pinner struct {
	pub        PinPublisher
	subscriber PinAcceptanceSubscriber
	cfg        PinnerConfig
	topic      string
	timeout    time.Duration
	now        func() time.Time
}

// NewPinner wires the publisher + subscriber. Subscriber may be nil —
// in that case PublishPinRequest broadcasts the request but returns
// immediately (no collection). Useful for blast-and-pray flows that
// read acceptances out-of-band.
func NewPinner(pub PinPublisher, sub PinAcceptanceSubscriber, cfg PinnerConfig) (*Pinner, error) {
	if pub == nil {
		return nil, errors.New("distributed: pinner needs a publisher")
	}
	if len(cfg.IdentityKey) != ed25519.PrivateKeySize {
		return nil, errors.New("distributed: pinner needs a valid Ed25519 private key")
	}
	topic := cfg.RequestTopic
	if topic == "" {
		topic = TopicPinRequest
	}
	timeout := cfg.AcceptanceTimeout
	if timeout <= 0 {
		timeout = DefaultAcceptanceTimeout
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Pinner{
		pub:        pub,
		subscriber: sub,
		cfg:        cfg,
		topic:      topic,
		timeout:    timeout,
		now:        now,
	}, nil
}

// BuildRequest constructs a signed PinRequest from params. Caller can
// inspect / adjust before calling PublishPinRequest.
func (p *Pinner) BuildRequest(params PinRequestParams) (PinRequest, error) {
	if params.CID == "" {
		return PinRequest{}, errors.New("distributed: empty cid")
	}
	if params.Duration < 7*24*time.Hour {
		return PinRequest{}, errors.New("distributed: duration must be >= 1 week")
	}
	if params.ReplicationFactor <= 0 {
		return PinRequest{}, errors.New("distributed: replication factor must be > 0")
	}
	if params.FeePerReplicator == nil || params.FeePerReplicator.Sign() <= 0 {
		return PinRequest{}, errors.New("distributed: fee must be positive")
	}
	total := new(big.Int).Mul(params.FeePerReplicator, big.NewInt(int64(params.ReplicationFactor)))
	req := PinRequest{
		RequestID:             params.RequestID,
		CID:                   params.CID,
		DurationSeconds:       int64(params.Duration.Seconds()),
		ReplicationFactor:     params.ReplicationFactor,
		FeePerReplicator:      params.FeePerReplicator.String(),
		TotalEscrowed:         total.String(),
		AcceptedPaymentChains: append([]string(nil), params.AcceptedPaymentChains...),
		RecordType:            params.RecordType,
		IssuedAtUnixMs:        p.now().UnixMilli(),
		SellerReputation:      params.SellerReputation,
	}
	if req.RequestID == "" {
		req.RequestID = fmt.Sprintf("%x-%d", p.cfg.IdentityKey.Public().(ed25519.PublicKey)[:8], req.IssuedAtUnixMs)
	}
	if err := req.Sign(p.cfg.IdentityKey); err != nil {
		return PinRequest{}, err
	}
	return req, nil
}

// PinRequestParams is the caller-facing shape for BuildRequest.
type PinRequestParams struct {
	RequestID             string
	CID                   string
	Duration              time.Duration
	ReplicationFactor     int
	FeePerReplicator      *big.Int
	AcceptedPaymentChains []string
	RecordType            string
	SellerReputation      float64
}

// PublishPinRequest broadcasts a signed PinRequest and waits up to
// AcceptanceTimeout for N replicator acceptances. Returns the list of
// ReplicatorIDs that accepted first (timestamp-ordered). An empty
// return + nil error means "no eligible replicators accepted in
// time" — the seller can retry with higher fees or shorter duration.
//
// If the Pinner was constructed with a nil subscriber, returns (nil,
// nil) immediately after publishing (no collection).
func (p *Pinner) PublishPinRequest(ctx context.Context, req PinRequest) ([]ReplicatorID, error) {
	if err := req.Verify(); err != nil {
		return nil, fmt.Errorf("distributed: refusing to publish unsigned/invalid request: %w", err)
	}
	payload, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("distributed: marshal pin request: %w", err)
	}
	if err := p.pub.Publish(p.topic, payload); err != nil {
		return nil, fmt.Errorf("distributed: publish: %w", err)
	}
	if p.subscriber == nil {
		return nil, nil
	}

	var (
		mu        sync.Mutex
		collected []PinAcceptance
		completed bool // set true once the outer select exits; under mu
		done      = make(chan struct{})
	)
	// Unsubscribe safely: wrap so we only fire the handler below when
	// we still care. The `completed` guard closes a subtle race: the
	// subscriber may deliver an acceptance concurrent with (or after)
	// the outer select firing on timeout/ctx.Done; that late delivery
	// must NOT append to collected after we've taken our snapshot.
	p.subscriber.SubscribeAcceptances(req.RequestID, func(a PinAcceptance) {
		if a.RequestID != req.RequestID || a.CID != req.CID {
			return
		}
		if err := a.Verify(); err != nil {
			return
		}
		mu.Lock()
		if completed {
			// Snapshot already taken; drop the late arrival on the
			// floor. Without this guard, a delivery that slips in
			// between close(done) and UnsubscribeAcceptances could
			// mutate `collected` after we've returned the snapshot
			// to the caller.
			mu.Unlock()
			return
		}
		// Dedup by replicator pubkey (not ReplicatorID — replicators
		// could lie about their ID; pubkey is the hard identity).
		for _, c := range collected {
			if bytes.Equal(c.ReplicatorPub, a.ReplicatorPub) {
				mu.Unlock()
				return
			}
		}
		collected = append(collected, a)
		filled := len(collected) >= req.ReplicationFactor
		mu.Unlock()
		if filled {
			select {
			case <-done:
				// already closed
			default:
				close(done)
			}
		}
	})
	defer p.subscriber.UnsubscribeAcceptances(req.RequestID)

	timer := time.NewTimer(p.timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		// Ran out of patience — return whatever we have.
	case <-timer.C:
		// Timeout — return whatever we have.
	case <-done:
		// Filled before timeout.
	}

	// Mark complete + snapshot atomically so any handler invocation
	// that wins a race with UnsubscribeAcceptances early-returns at
	// the `completed` guard above.
	mu.Lock()
	completed = true
	snapshot := append([]PinAcceptance(nil), collected...)
	mu.Unlock()

	// Timestamp-ordered selection of first N.
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].AcceptedAtUnixMs < snapshot[j].AcceptedAtUnixMs
	})
	n := req.ReplicationFactor
	if n > len(snapshot) {
		n = len(snapshot)
	}
	out := make([]ReplicatorID, 0, n)
	for _, a := range snapshot[:n] {
		out = append(out, a.ReplicatorID)
	}
	return out, nil
}

// ─── Replicator side ──────────────────────────────────────────────

// PinPolicyConfig mirrors the TOML snippet in ROADMAP.md section 2.
type PinPolicyConfig struct {
	Enabled                bool
	MaxTotalPinnedBytes    int64
	MinFeeRatePerMBMonth   float64 // in whatever unit the seller's chain uses; the replicator computes against this
	MaxCommitmentWeeks     int
	AllowedRecordTypes     []string
	MinSellerReputation    float64
	PreferredChains        []string // non-empty = only accept if at least one match is in req.AcceptedPaymentChains
	PreferredChainID       string   // chain the replicator's escrow will be deployed on
	// MinPieceSizeBytes / MaxPieceSizeBytes gate by size. Zero = no limit.
	MinPieceSizeBytes int64
	MaxPieceSizeBytes int64
}

// PinAcceptorConfig ties together identity, policy and escrow client.
type PinAcceptorConfig struct {
	PeerID      string
	IdentityKey ed25519.PrivateKey
	Policy      PinPolicyConfig
	// CurrentPinnedBytes returns the current total of pinned bytes on
	// this node. The policy gate uses this to enforce MaxTotalPinnedBytes.
	CurrentPinnedBytes func() int64
	// Now, if set, replaces time.Now().
	Now func() time.Time
}

// PinAcceptor is the replicator-side decision engine. It evaluates
// incoming PinRequests against the local policy and — if all gates
// pass — creates the on-chain escrow + broadcasts an acceptance.
type PinAcceptor struct {
	cfg       PinAcceptorConfig
	escrow    EscrowClient
	pub       PinPublisher
	topic     string
	now       func() time.Time
}

// NewPinAcceptor wires policy + escrow client + publisher. The
// acceptor is returned even when Enabled=false (so callers can pass
// it through Wire code unconditionally), but HandlePinRequest returns
// early in that case.
func NewPinAcceptor(cfg PinAcceptorConfig, escrow EscrowClient, pub PinPublisher) (*PinAcceptor, error) {
	if len(cfg.IdentityKey) != ed25519.PrivateKeySize {
		return nil, errors.New("distributed: acceptor needs a valid Ed25519 private key")
	}
	if escrow == nil {
		return nil, errors.New("distributed: acceptor needs an EscrowClient")
	}
	if pub == nil {
		return nil, errors.New("distributed: acceptor needs a publisher")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &PinAcceptor{
		cfg:    cfg,
		escrow: escrow,
		pub:    pub,
		topic:  TopicPinAccept,
		now:    now,
	}, nil
}

// ErrPolicyReject is returned by evaluatePolicy when the request fails
// one or more policy gates. It's wrapped with a reason string; callers
// get the reason via errors.As on PolicyRejectError below.
type PolicyRejectError struct {
	Reason string
}

// Error implements error.
func (e *PolicyRejectError) Error() string { return "distributed: policy reject: " + e.Reason }

// EvaluatePolicy runs the gate logic without any side effects. Exposed
// for tests + diagnostics.
func (a *PinAcceptor) EvaluatePolicy(req PinRequest) error {
	p := a.cfg.Policy
	if !p.Enabled {
		return &PolicyRejectError{Reason: "acceptor disabled"}
	}
	if err := req.Verify(); err != nil {
		return &PolicyRejectError{Reason: "bad signature: " + err.Error()}
	}
	// Duration / commitment gate.
	weeks := req.DurationSeconds / int64(7*24*3600)
	if p.MaxCommitmentWeeks > 0 && weeks > int64(p.MaxCommitmentWeeks) {
		return &PolicyRejectError{Reason: fmt.Sprintf("duration %dw > max %dw", weeks, p.MaxCommitmentWeeks)}
	}
	if p.MinSellerReputation > 0 && req.SellerReputation < p.MinSellerReputation {
		return &PolicyRejectError{Reason: fmt.Sprintf("seller reputation %.3f < min %.3f", req.SellerReputation, p.MinSellerReputation)}
	}
	if len(p.AllowedRecordTypes) > 0 {
		ok := false
		for _, rt := range p.AllowedRecordTypes {
			if rt == req.RecordType {
				ok = true
				break
			}
		}
		if !ok {
			return &PolicyRejectError{Reason: "record type not allowed: " + req.RecordType}
		}
	}
	// Chain preferences.
	if len(p.PreferredChains) > 0 {
		ok := false
		for _, pref := range p.PreferredChains {
			for _, acc := range req.AcceptedPaymentChains {
				if pref == acc {
					ok = true
					break
				}
			}
			if ok {
				break
			}
		}
		if !ok {
			return &PolicyRejectError{Reason: "no chain preference overlap"}
		}
	}
	// Total pinned capacity gate.
	if p.MaxTotalPinnedBytes > 0 && a.cfg.CurrentPinnedBytes != nil {
		if a.cfg.CurrentPinnedBytes() >= p.MaxTotalPinnedBytes {
			return &PolicyRejectError{Reason: "at pin capacity"}
		}
	}
	return nil
}

// HandlePinRequest is the entry point a Whisper topic handler calls
// for each inbound PinRequest. It evaluates policy; if all gates pass
// it creates the on-chain escrow; on success it broadcasts an
// acceptance. A policy rejection is SILENT (no broadcast) by design —
// see package comment.
//
// The return value is primarily for tests + instrumentation:
//   - (address, nil)  → accepted, escrow deployed, acceptance published
//   - ("", PolicyRejectError)  → rejected by policy, no network traffic
//   - ("", other err)  → attempted accept but escrow or publish failed;
//     acceptor has NOT broadcast anything. Caller should log.
func (a *PinAcceptor) HandlePinRequest(ctx context.Context, req PinRequest) (string, error) {
	if err := a.EvaluatePolicy(req); err != nil {
		return "", err
	}

	// Derive replicator chain address from identity pubkey in whatever
	// format the target chain expects. This package doesn't know — the
	// escrow client resolves it internally. We pass the pubkey (hex)
	// as the "replicator id" string; the concrete EscrowClient maps it
	// to its on-chain address.
	pub := a.cfg.IdentityKey.Public().(ed25519.PublicKey)
	replicatorID := fmt.Sprintf("%x", pub)

	feePerRep, ok := new(big.Int).SetString(req.FeePerReplicator, 10)
	if !ok {
		return "", fmt.Errorf("distributed: bad FeePerReplicator: %s", req.FeePerReplicator)
	}

	chain := a.cfg.Policy.PreferredChainID
	if chain == "" && len(a.cfg.Policy.PreferredChains) > 0 {
		chain = a.cfg.Policy.PreferredChains[0]
	}

	params := CreatePinParams{
		ChainID:           chain,
		CID:               req.CID,
		Seller:            fmt.Sprintf("%x", req.SellerPubKey),
		Replicator:        replicatorID,
		Duration:          time.Duration(req.DurationSeconds) * time.Second,
		ReplicationFactor: req.ReplicationFactor,
		FeePerReplicator:  feePerRep,
		BondDeadline:      a.now().Add(DefaultBondDeadlineSkew),
	}

	addr, err := a.escrow.CreatePin(ctx, params)
	if err != nil {
		return "", fmt.Errorf("distributed: create escrow: %w", err)
	}
	if _, err := a.escrow.PostBond(ctx, addr); err != nil {
		// Escrow was deployed but bond failed. Surface the error so the
		// caller can retry or alert; DO NOT broadcast acceptance (seller
		// must not consider us committed without bond).
		return "", fmt.Errorf("distributed: post bond: %w", err)
	}

	ack := PinAcceptance{
		RequestID:        req.RequestID,
		CID:              req.CID,
		ReplicatorID:     replicatorID,
		EscrowChain:      chain,
		EscrowAddress:    addr,
		AcceptedAtUnixMs: a.now().UnixMilli(),
	}
	if err := ack.Sign(a.cfg.IdentityKey); err != nil {
		return addr, fmt.Errorf("distributed: sign acceptance: %w", err)
	}
	payload, err := json.Marshal(&ack)
	if err != nil {
		return addr, fmt.Errorf("distributed: marshal acceptance: %w", err)
	}
	if err := a.pub.Publish(TopicPinAccept, payload); err != nil {
		return addr, fmt.Errorf("distributed: publish acceptance: %w", err)
	}
	return addr, nil
}

// DecodePinRequest is a convenience for topic handlers reading raw
// Whisper payloads.
func DecodePinRequest(b []byte) (PinRequest, error) {
	var r PinRequest
	if err := json.Unmarshal(b, &r); err != nil {
		return PinRequest{}, fmt.Errorf("distributed: pin-request decode: %w", err)
	}
	return r, nil
}

// DecodePinAcceptance mirrors DecodePinRequest for the acceptance side.
func DecodePinAcceptance(b []byte) (PinAcceptance, error) {
	var a PinAcceptance
	if err := json.Unmarshal(b, &a); err != nil {
		return PinAcceptance{}, fmt.Errorf("distributed: pin-acceptance decode: %w", err)
	}
	return a, nil
}
