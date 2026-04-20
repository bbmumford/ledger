/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package anchor

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

var (
	ErrInvalidSignature   = errors.New("anchor: invalid snapshot signature")
	ErrInvalidKey         = errors.New("anchor: invalid public key")
	ErrInsufficientQuorum = errors.New("anchor: insufficient signature quorum")
	ErrDuplicateSigner    = errors.New("anchor: duplicate signer in multi-sig")
	ErrUnknownSigner      = errors.New("anchor: signature from unknown anchor")
)

// MultiSigConfig configures multi-signature verification requirements.
type MultiSigConfig struct {
	RequiredSignatures int                          // Minimum valid signatures required (e.g., 2)
	TotalAnchors       int                          // Total anchor nodes (e.g., 3)
	TrustedAnchors     map[string]ed25519.PublicKey // nodeID -> public key
}

// DefaultMultiSigConfig returns a 2-of-3 configuration.
func DefaultMultiSigConfig() MultiSigConfig {
	return MultiSigConfig{
		RequiredSignatures: 2,
		TotalAnchors:       3,
		TrustedAnchors:     make(map[string]ed25519.PublicKey),
	}
}

// AnchorSignature represents a single anchor's signature on a snapshot.
type AnchorSignature struct {
	NodeID    string `json:"node_id"`
	Signature []byte `json:"sig"`
}

// MultiSigSnapshot extends Snapshot with multiple anchor signatures.
type MultiSigSnapshot struct {
	Header     MultiSigHeader    `json:"header"`
	Records    []Record          `json:"records,omitempty"` // Use local Record type placeholder
	Signatures []AnchorSignature `json:"signatures"`
}

// MultiSigHeader contains metadata for multi-sig snapshots.
type MultiSigHeader struct {
	Timestamp  int64  `json:"ts"`
	Sequence   uint64 `json:"seq"`
	MerkleRoot []byte `json:"merkle_root,omitempty"` // Optional: Merkle root of records
}

// Record placeholder - imported from parent package in real use
type Record = interface{}

// Signer signs snapshots using an Ed25519 private key.
type Signer struct {
	privateKey ed25519.PrivateKey
	nodeID     string
}

// NewSigner creates a signer from a hex-encoded private key.
func NewSigner(nodeID, privateKeyHex string) (*Signer, error) {
	keyBytes, err := hex.DecodeString(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("decode private key: %w", err)
	}
	if len(keyBytes) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid private key length")
	}
	return &Signer{
		privateKey: ed25519.PrivateKey(keyBytes),
		nodeID:     nodeID,
	}, nil
}

// Sign signs the content and returns the signature.
func (s *Signer) Sign(content *SnapshotContent) ([]byte, error) {
	data, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal content: %w", err)
	}
	return ed25519.Sign(s.privateKey, data), nil
}

// SignMultiSig creates a signature for a multi-sig snapshot.
func (s *Signer) SignMultiSig(snapshot *MultiSigSnapshot) (*AnchorSignature, error) {
	// Sign header + records (excluding existing signatures)
	content := struct {
		Header  MultiSigHeader `json:"header"`
		Records []Record       `json:"records"`
	}{
		Header:  snapshot.Header,
		Records: snapshot.Records,
	}

	data, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal multi-sig content: %w", err)
	}

	sig := ed25519.Sign(s.privateKey, data)
	return &AnchorSignature{
		NodeID:    s.nodeID,
		Signature: sig,
	}, nil
}

// Verify checks the signature of a snapshot against a trusted public key.
func Verify(snapshot *Snapshot, pubKey ed25519.PublicKey) error {
	// Reconstruct the content that was signed
	content := SnapshotContent{
		Timestamp: snapshot.Header.Timestamp,
		Sequence:  snapshot.Header.Sequence,
		NodeID:    snapshot.Header.NodeID,
		Records:   snapshot.Records,
	}

	data, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("marshal content for verification: %w", err)
	}

	if !ed25519.Verify(pubKey, data, snapshot.Header.Signature) {
		return ErrInvalidSignature
	}

	return nil
}

// MultiSigVerifier verifies multi-signature snapshots.
type MultiSigVerifier struct {
	config MultiSigConfig
}

// NewMultiSigVerifier creates a verifier with the given configuration.
func NewMultiSigVerifier(config MultiSigConfig) *MultiSigVerifier {
	return &MultiSigVerifier{config: config}
}

// AddTrustedAnchor adds a trusted anchor node's public key.
func (v *MultiSigVerifier) AddTrustedAnchor(nodeID string, pubKeyHex string) error {
	keyBytes, err := hex.DecodeString(pubKeyHex)
	if err != nil {
		return fmt.Errorf("decode public key: %w", err)
	}
	if len(keyBytes) != ed25519.PublicKeySize {
		return ErrInvalidKey
	}
	v.config.TrustedAnchors[nodeID] = ed25519.PublicKey(keyBytes)
	return nil
}

// Verify checks that the snapshot has sufficient valid signatures from trusted anchors.
// Returns nil if quorum is met, error otherwise.
func (v *MultiSigVerifier) Verify(snapshot *MultiSigSnapshot) error {
	if len(snapshot.Signatures) < v.config.RequiredSignatures {
		return fmt.Errorf("%w: got %d, need %d", ErrInsufficientQuorum,
			len(snapshot.Signatures), v.config.RequiredSignatures)
	}

	// Prepare content for verification (header + records, no signatures)
	content := struct {
		Header  MultiSigHeader `json:"header"`
		Records []Record       `json:"records"`
	}{
		Header:  snapshot.Header,
		Records: snapshot.Records,
	}

	data, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("marshal content for verification: %w", err)
	}

	// Track which signers we've seen to prevent duplicates
	seenSigners := make(map[string]bool)
	validCount := 0

	for _, sig := range snapshot.Signatures {
		// Check for duplicate signer
		if seenSigners[sig.NodeID] {
			return fmt.Errorf("%w: %s", ErrDuplicateSigner, sig.NodeID)
		}
		seenSigners[sig.NodeID] = true

		// Get trusted public key for this signer
		pubKey, ok := v.config.TrustedAnchors[sig.NodeID]
		if !ok {
			// Unknown signer - skip but don't fail (might be a new anchor)
			continue
		}

		// Verify signature
		if ed25519.Verify(pubKey, data, sig.Signature) {
			validCount++
		}
	}

	if validCount < v.config.RequiredSignatures {
		return fmt.Errorf("%w: %d valid of %d required", ErrInsufficientQuorum,
			validCount, v.config.RequiredSignatures)
	}

	return nil
}

// VerifyStrict is like Verify but rejects unknown signers.
func (v *MultiSigVerifier) VerifyStrict(snapshot *MultiSigSnapshot) error {
	// Prepare content for verification
	content := struct {
		Header  MultiSigHeader `json:"header"`
		Records []Record       `json:"records"`
	}{
		Header:  snapshot.Header,
		Records: snapshot.Records,
	}

	data, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("marshal content for verification: %w", err)
	}

	seenSigners := make(map[string]bool)
	validCount := 0

	for _, sig := range snapshot.Signatures {
		if seenSigners[sig.NodeID] {
			return fmt.Errorf("%w: %s", ErrDuplicateSigner, sig.NodeID)
		}
		seenSigners[sig.NodeID] = true

		pubKey, ok := v.config.TrustedAnchors[sig.NodeID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnknownSigner, sig.NodeID)
		}

		if !ed25519.Verify(pubKey, data, sig.Signature) {
			return fmt.Errorf("%w from %s", ErrInvalidSignature, sig.NodeID)
		}
		validCount++
	}

	if validCount < v.config.RequiredSignatures {
		return fmt.Errorf("%w: %d valid of %d required", ErrInsufficientQuorum,
			validCount, v.config.RequiredSignatures)
	}

	return nil
}

// GetValidSigners returns the list of anchor nodes that provided valid signatures.
func (v *MultiSigVerifier) GetValidSigners(snapshot *MultiSigSnapshot) ([]string, error) {
	content := struct {
		Header  MultiSigHeader `json:"header"`
		Records []Record       `json:"records"`
	}{
		Header:  snapshot.Header,
		Records: snapshot.Records,
	}

	data, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}

	var validSigners []string
	for _, sig := range snapshot.Signatures {
		pubKey, ok := v.config.TrustedAnchors[sig.NodeID]
		if !ok {
			continue
		}
		if ed25519.Verify(pubKey, data, sig.Signature) {
			validSigners = append(validSigners, sig.NodeID)
		}
	}

	sort.Strings(validSigners)
	return validSigners, nil
}
