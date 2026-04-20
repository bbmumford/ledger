/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

// Proof-of-storage challenge/response protocol.
//
// WHY:
//   Pinning replicators have been paid up front + posted a 10% bond. To
//   deter seeders from claiming they hold pieces they've evicted, any peer
//   aware of a pinned CID may probabilistically issue a challenge:
//
//       challenger → {cid, nonce, ts}  ← ProofChallenge
//       replicator → {cid, nonce, hash=BLAKE2b(piece||nonce), sig}  ← ProofResponse
//
//   The challenger verifies `hash` against the plaintext they themselves
//   fetched + the signature against the replicator's identity pubkey. A
//   missed / wrong response is reported to PlutusStorageEscrow.sol which
//   slashes the bond after a 2nd miss in the same week.
//
// CHALLENGE FREQUENCY:
//   ROADMAP.md section 2 specifies an aggregate ~1 challenge per 24h per
//   pinned CID. Each peer independently rolls at probability
//   (1 / expectedInterval) × tickRate on each tick. With 24 peers tick-ing
//   once an hour, the expected aggregate rate across all peers is 1/day.
//
// CRYPTO:
//   - hash = BLAKE2b-256(piece || nonce) — same primitive used for CIDs.
//   - sig  = Ed25519 over the canonical bytes {cid, nonce, hash}.
//
// TRANSPORT:
//   Whisper RequestResponse on topic `plutus.blob.challenge`. The issuer
//   publishes a query; the responder (if it holds the cid) answers with
//   the signed response. If no response arrives within the challenge
//   window the issuer records a miss locally AND — when wired — submits
//   a miss to the on-chain PlutusStorageEscrow contract so the bond can
//   be slashed.
//
// ON-CHAIN HASH NOTE:
//   The contract's `expectedHash` uses keccak256 (gas-cheap), NOT BLAKE2b.
//   Use ComputeOnChainExpectedHash (proof_onchain.go) when building the
//   openChallenge(nonce, expectedHash) payload. ComputeProofHash in this
//   file is the BLAKE2b off-chain primitive used by the Whisper response
//   envelope and MUST NOT be submitted on-chain.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/crypto/blake2b"
)

// TopicBlobChallenge is the Whisper RequestResponse topic on which
// proof-of-storage challenges are exchanged. Callers MUST pre-register
// the topic with a ChallengeResponder attached as the handler before
// challenges can be served.
const TopicBlobChallenge = "plutus.blob.challenge"

// ChallengeNonceSize is the length of the random nonce in bytes.
const ChallengeNonceSize = 32

// DefaultChallengeResponseTimeout bounds how long a challenger waits for
// a synchronous Whisper response before recording a timeout-miss. The
// on-chain window is 24h; the synchronous timeout here is far shorter
// because the response flows over an active gossip round.
const DefaultChallengeResponseTimeout = 30 * time.Second

// ProofChallenge is the outbound query payload. Encoded as JSON on the
// wire — the nonce is 32 bytes so the envelope is ~60 bytes uncompressed.
type ProofChallenge struct {
	CID       string    `json:"cid"`
	Nonce     []byte    `json:"nonce"`
	Timestamp time.Time `json:"ts"`
	// Issuer is the challenger's PeerID. Present so the responder can
	// bind the signed response envelope to a specific requester (avoids
	// replay of signed responses across unrelated challengers).
	Issuer string `json:"issuer,omitempty"`
}

// ProofResponse is the signed reply. Hash = BLAKE2b-256(piece || nonce);
// Sig = Ed25519 signature over canonical bytes.
type ProofResponse struct {
	CID   string `json:"cid"`
	Nonce []byte `json:"nonce"`
	Hash  []byte `json:"hash"`
	// PubKey is the responder's Ed25519 public key. Included so the
	// challenger can verify without a directory lookup; also serves as
	// a weak identity binding when the on-chain replicator address is
	// not directly resolvable to the mesh identity.
	PubKey []byte `json:"pubkey"`
	// Sig is the Ed25519 signature over canonical(cid, nonce, hash,
	// pubkey). See signMessage for the exact encoding.
	Sig []byte `json:"sig"`
}

// signMessage builds the canonical byte string the signer/verifier
// sign over. Keep deterministic: newline-delimited fields, no
// whitespace, no JSON (JSON key ordering isn't guaranteed stable
// enough for signature verification).
func signMessage(cid string, nonce, hash, pub []byte) []byte {
	buf := make([]byte, 0, len(cid)+len(nonce)+len(hash)+len(pub)+16)
	buf = append(buf, []byte(cid)...)
	buf = append(buf, '\n')
	buf = append(buf, nonce...)
	buf = append(buf, '\n')
	buf = append(buf, hash...)
	buf = append(buf, '\n')
	buf = append(buf, pub...)
	return buf
}

// ComputeProofHash computes BLAKE2b-256(piece || nonce). This is the
// OFF-CHAIN gossip primitive: the ProofResponse.Hash field + the
// VerifyResponse / responder signature are all bound to this BLAKE2b
// digest. Exposed so tests + consumers can precompute the expected
// hash for in-mesh challenge verification.
//
// ┌───────────────────────────────────────────────────────────────┐
// │ WARNING — NOT for on-chain use.                               │
// │ Submitting this value as PlutusStorageEscrow.openChallenge's  │
// │ `expectedHash` argument will ALWAYS cause the replicator's    │
// │ keccak256 respond(bytes32) to mismatch, producing unjustified │
// │ misses. Use ComputeOnChainExpectedHash (proof_onchain.go) for │
// │ any EVM-bound payload. See proof_onchain.go for the full      │
// │ rationale (BLAKE2b vs keccak256 gas trade-off).               │
// └───────────────────────────────────────────────────────────────┘
func ComputeProofHash(piece, nonce []byte) []byte {
	h, _ := blake2b.New256(nil)
	h.Write(piece)
	h.Write(nonce)
	sum := h.Sum(nil)
	return sum
}

// NewNonce returns ChallengeNonceSize cryptographically random bytes.
func NewNonce() ([]byte, error) {
	n := make([]byte, ChallengeNonceSize)
	if _, err := rand.Read(n); err != nil {
		return nil, fmt.Errorf("distributed: nonce generation: %w", err)
	}
	return n, nil
}

// ─── Issuer (challenger role) ──────────────────────────────────────

// ChallengeTransport is the minimum surface the issuer needs: issue a
// RequestResponse-style query to a specific peer and await their reply.
// Tests pass a fake; production wraps whisper.Engine.Query (when it
// supports direct-peer queries) OR a Minerva direct-RPC channel.
type ChallengeTransport interface {
	// Ask sends the encoded ProofChallenge to peerID and waits for a
	// single ProofResponse envelope. Returns ctx.Err() on cancellation
	// or nil + empty bytes on "peer replied with no answer" (treated
	// as a miss by the caller).
	Ask(ctx context.Context, peerID string, payload []byte) ([]byte, error)
}

// ChallengeResult records the outcome of one Challenge call.
type ChallengeResult struct {
	CID      string
	PeerID   string
	Issued   time.Time
	Received time.Time
	OK       bool   // response arrived + hash matched
	Reason   string // populated on non-OK outcomes (timeout, wrong_hash, sig_fail, ...)
	Response *ProofResponse
}

// ChallengeIssuerConfig tunes the issuer side.
type ChallengeIssuerConfig struct {
	// IssuerPeerID stamps each ProofChallenge with our PeerID so
	// responders can bind the reply to this issuer.
	IssuerPeerID string
	// ResponseTimeout bounds the synchronous wait. Defaults to
	// DefaultChallengeResponseTimeout when zero.
	ResponseTimeout time.Duration
	// Now, if set, replaces time.Now() for deterministic tests.
	Now func() time.Time
}

// ChallengeIssuer owns the challenger side: build the challenge, ship
// it over the transport, verify the response + signature, return the
// pass/fail decision. Results feed the Scoreboard in memory and —
// under the on-chain extension — the escrow contract.
//
// pieces, if set, is consulted on every successful response so the
// issuer can compare the responder's claimed hash against a locally
// fetched copy of the piece. Without this source a malicious
// replicator that never actually held the piece could sign a
// fabricated hash + pass the signature check. See
// NewChallengeIssuerWithPieceSource.
type ChallengeIssuer struct {
	transport ChallengeTransport
	cfg       ChallengeIssuerConfig
	now       func() time.Time
	timeout   time.Duration
	pieces    PieceSource // may be nil (reduced-security mode)

	mu      sync.Mutex
	results []ChallengeResult // bounded ring; see recordResult
}

// NewChallengeIssuer wires a transport. The transport may be nil when
// constructed for local verification use only (in tests that drive
// VerifyResponse directly), but Challenge() will fail with a clear
// error in that case.
//
// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECURITY NOTE — reduced-security mode.                          ║
// ║  An issuer built with this constructor has NO local PieceSource, ║
// ║  so Challenge() cannot cross-check a responder's claimed hash    ║
// ║  against the actual piece bytes. A malicious replicator that     ║
// ║  never held the piece can sign a fabricated hash and pass the    ║
// ║  signature + CID/nonce-binding checks. Acceptable for tests;     ║
// ║  production wiring MUST use NewChallengeIssuerWithPieceSource.   ║
// ╚══════════════════════════════════════════════════════════════════╝
func NewChallengeIssuer(t ChallengeTransport, cfg ChallengeIssuerConfig) (*ChallengeIssuer, error) {
	return NewChallengeIssuerWithPieceSource(t, nil, cfg)
}

// NewChallengeIssuerWithPieceSource is the production-grade constructor:
// pass a PieceSource (typically the local distributed.Store) so the
// issuer can verify that the responder's hash matches what a locally
// held copy of the piece would produce. When the local store does not
// hold the piece, verification is skipped (Challenge will still verify
// CID + nonce + signature); this preserves liveness for challengers
// that are challenging CIDs they have not themselves fetched.
func NewChallengeIssuerWithPieceSource(t ChallengeTransport, pieces PieceSource, cfg ChallengeIssuerConfig) (*ChallengeIssuer, error) {
	timeout := cfg.ResponseTimeout
	if timeout <= 0 {
		timeout = DefaultChallengeResponseTimeout
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &ChallengeIssuer{
		transport: t,
		cfg:       cfg,
		now:       now,
		timeout:   timeout,
		pieces:    pieces,
	}, nil
}

// Challenge issues one proof-of-storage challenge. On success returns
// the verified ProofResponse (the caller can now advance its local
// success counter / skip any on-chain miss-submission). On failure the
// ChallengeResult in the returned error's struct-field wrapper carries
// the reason ("timeout", "wrong_hash", "sig_fail", "cid_mismatch").
func (ci *ChallengeIssuer) Challenge(ctx context.Context, peerID, cid string) (ChallengeResult, error) {
	if ci.transport == nil {
		return ChallengeResult{}, errors.New("distributed: challenge issuer has no transport")
	}
	if peerID == "" {
		return ChallengeResult{}, errors.New("distributed: empty peerID")
	}
	if cid == "" {
		return ChallengeResult{}, errors.New("distributed: empty cid")
	}

	nonce, err := NewNonce()
	if err != nil {
		return ChallengeResult{}, err
	}
	chal := ProofChallenge{
		CID:       cid,
		Nonce:     nonce,
		Timestamp: ci.now(),
		Issuer:    ci.cfg.IssuerPeerID,
	}
	payload, err := json.Marshal(&chal)
	if err != nil {
		return ChallengeResult{}, fmt.Errorf("distributed: challenge marshal: %w", err)
	}

	askCtx, cancel := context.WithTimeout(ctx, ci.timeout)
	defer cancel()

	raw, askErr := ci.transport.Ask(askCtx, peerID, payload)
	res := ChallengeResult{
		CID:    cid,
		PeerID: peerID,
		Issued: chal.Timestamp,
	}
	if askErr != nil {
		// Context cancellation / timeout = miss.
		if errors.Is(askErr, context.DeadlineExceeded) || errors.Is(askErr, context.Canceled) {
			res.Reason = "timeout"
		} else {
			res.Reason = "transport:" + askErr.Error()
		}
		ci.recordResult(res)
		return res, nil
	}
	if len(raw) == 0 {
		res.Reason = "empty_response"
		res.Received = ci.now()
		ci.recordResult(res)
		return res, nil
	}

	var resp ProofResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		res.Reason = "decode_error"
		res.Received = ci.now()
		ci.recordResult(res)
		return res, nil
	}
	res.Response = &resp
	res.Received = ci.now()

	// CID + nonce must match what we asked for (prevents replay / wrong
	// response stitching).
	if resp.CID != cid {
		res.Reason = "cid_mismatch"
		ci.recordResult(res)
		return res, nil
	}
	if !bytes.Equal(resp.Nonce, nonce) {
		res.Reason = "nonce_mismatch"
		ci.recordResult(res)
		return res, nil
	}

	// Signature check.
	if len(resp.PubKey) != ed25519.PublicKeySize {
		res.Reason = "bad_pubkey"
		ci.recordResult(res)
		return res, nil
	}
	if !ed25519.Verify(ed25519.PublicKey(resp.PubKey), signMessage(resp.CID, resp.Nonce, resp.Hash, resp.PubKey), resp.Sig) {
		res.Reason = "sig_fail"
		ci.recordResult(res)
		return res, nil
	}

	// Local-piece cross-check: if the issuer has its own copy of the
	// piece, verify the responder's claimed hash matches. This closes
	// the loophole where a responder with a valid identity key signs a
	// fabricated hash for content they never actually held. When the
	// local store does not have the piece, fall through and accept the
	// signature-only proof (the issuer has no way to challenge content
	// they have never seen). See PieceSource docs + the SECURITY NOTE
	// on NewChallengeIssuer.
	if ci.pieces != nil {
		has, hErr := ci.pieces.Has(ctx, cid)
		if hErr == nil && has {
			piece, gErr := ci.pieces.Get(ctx, cid)
			if gErr == nil {
				expected := ComputeProofHash(piece, resp.Nonce)
				if !bytes.Equal(expected, resp.Hash) {
					res.Reason = "wrong_hash"
					ci.recordResult(res)
					return res, nil
				}
			}
			// Transient fetch error → skip cross-check (do not penalize
			// the responder for our local storage error).
		}
	}

	res.OK = true
	ci.recordResult(res)
	return res, nil
}

// VerifyResponse checks a ProofResponse against the plaintext piece
// bytes the challenger holds locally. Returns nil when the response is
// fully valid (hash matches + signature verifies). Used by the issuer
// once it has separately fetched the piece (via Fetcher.Fetch) to
// cross-check a claim.
func (ci *ChallengeIssuer) VerifyResponse(resp *ProofResponse, pieceBytes []byte) error {
	if resp == nil {
		return errors.New("distributed: nil response")
	}
	if len(resp.PubKey) != ed25519.PublicKeySize {
		return errors.New("distributed: bad pubkey length")
	}
	expected := ComputeProofHash(pieceBytes, resp.Nonce)
	if !bytes.Equal(expected, resp.Hash) {
		return fmt.Errorf("distributed: hash mismatch: expected %s got %s",
			hex.EncodeToString(expected), hex.EncodeToString(resp.Hash))
	}
	if !ed25519.Verify(ed25519.PublicKey(resp.PubKey), signMessage(resp.CID, resp.Nonce, resp.Hash, resp.PubKey), resp.Sig) {
		return errors.New("distributed: signature verification failed")
	}
	return nil
}

// RecentResults returns a snapshot of the last N challenge results.
// Useful for the status tracker / diagnostics.
func (ci *ChallengeIssuer) RecentResults() []ChallengeResult {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	out := make([]ChallengeResult, len(ci.results))
	copy(out, ci.results)
	return out
}

const challengeResultCap = 256

func (ci *ChallengeIssuer) recordResult(r ChallengeResult) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	if len(ci.results) >= challengeResultCap {
		// Drop oldest.
		ci.results = ci.results[1:]
	}
	ci.results = append(ci.results, r)
}

// ─── Responder (replicator role) ───────────────────────────────────

// ChallengeResponderConfig tunes the replicator side.
type ChallengeResponderConfig struct {
	// PeerID is this node's identity (not strictly required — for logs).
	PeerID string
	// IdentityKey signs each ProofResponse. Required.
	IdentityKey ed25519.PrivateKey
	// Topic overrides TopicBlobChallenge (tests).
	Topic string
	// Now, if set, replaces time.Now() for deterministic tests.
	Now func() time.Time
}

// ChallengeResponder handles inbound ProofChallenge messages: looks up
// the piece in the backing store, computes the hash, signs the envelope
// and returns the bytes. Satisfies whisper.TopicHandler via OnMessage.
//
// If the CID is not held locally, returns (nil, nil) so Whisper does
// NOT reply — the issuer's transport will time out and record a miss,
// which is the correct outcome (we genuinely don't have it).
type ChallengeResponder struct {
	store PieceSource
	cfg   ChallengeResponderConfig
	topic string
	now   func() time.Time
}

// PieceSource is the minimum surface the responder needs: fetch the
// piece's plaintext by CID. Store satisfies this via Get.
type PieceSource interface {
	Get(ctx context.Context, cid string) ([]byte, error)
	Has(ctx context.Context, cid string) (bool, error)
}

// NewChallengeResponder wires a store to a responder. IdentityKey must
// be a valid ed25519 private key. The topic defaults to
// TopicBlobChallenge when unset.
func NewChallengeResponder(store PieceSource, cfg ChallengeResponderConfig) (*ChallengeResponder, error) {
	if store == nil {
		return nil, errors.New("distributed: responder needs a non-nil store")
	}
	if len(cfg.IdentityKey) != ed25519.PrivateKeySize {
		return nil, errors.New("distributed: responder needs a valid Ed25519 private key")
	}
	topic := cfg.Topic
	if topic == "" {
		topic = TopicBlobChallenge
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &ChallengeResponder{
		store: store,
		cfg:   cfg,
		topic: topic,
		now:   now,
	}, nil
}

// Topic returns the effective topic name. Callers pass this to
// engine.RegisterTopic(...).
func (r *ChallengeResponder) Topic() string { return r.topic }

// OnMessage implements whisper.TopicHandler. See package comment for
// semantics.
func (r *ChallengeResponder) OnMessage(ctx context.Context, from string, topic string, payload []byte) ([]byte, error) {
	if topic != r.topic {
		return nil, fmt.Errorf("distributed: responder topic mismatch: %q", topic)
	}
	if len(payload) == 0 {
		return nil, errors.New("distributed: empty challenge payload")
	}
	var chal ProofChallenge
	if err := json.Unmarshal(payload, &chal); err != nil {
		return nil, fmt.Errorf("distributed: challenge decode: %w", err)
	}
	if chal.CID == "" || len(chal.Nonce) == 0 {
		return nil, errors.New("distributed: challenge missing fields")
	}

	// Return early when we don't hold the piece.
	has, err := r.store.Has(ctx, chal.CID)
	if err != nil || !has {
		return nil, nil
	}

	piece, err := r.store.Get(ctx, chal.CID)
	if err != nil {
		// Transient — treat as "don't hold" so the issuer records a miss.
		return nil, nil
	}

	hash := ComputeProofHash(piece, chal.Nonce)
	pub := r.cfg.IdentityKey.Public().(ed25519.PublicKey)
	sig := ed25519.Sign(r.cfg.IdentityKey, signMessage(chal.CID, chal.Nonce, hash, pub))

	resp := ProofResponse{
		CID:    chal.CID,
		Nonce:  chal.Nonce,
		Hash:   hash,
		PubKey: pub,
		Sig:    sig,
	}
	return json.Marshal(&resp)
}

// BuildResponseFor is a helper exposed for tests / direct use: given a
// raw piece + nonce + identity key, produce a signed ProofResponse.
// Mirrors the server-side code path without touching a Store.
func BuildResponseFor(cid string, piece, nonce []byte, identityKey ed25519.PrivateKey) ProofResponse {
	hash := ComputeProofHash(piece, nonce)
	pub := identityKey.Public().(ed25519.PublicKey)
	sig := ed25519.Sign(identityKey, signMessage(cid, nonce, hash, pub))
	return ProofResponse{
		CID:    cid,
		Nonce:  nonce,
		Hash:   hash,
		PubKey: pub,
		Sig:    sig,
	}
}
