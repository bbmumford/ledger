/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package anchor

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

func TestSignAndVerify(t *testing.T) {
	// Generate keys
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	nodeID := "node1"
	privHex := hex.EncodeToString(priv)

	// Create signer
	signer, err := NewSigner(nodeID, privHex)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	// Create generator
	gen := NewGenerator(signer)

	// Create dummy records
	records := []lad.Record{
		{
			Topic:     lad.TopicMember,
			NodeID:    "node2",
			Timestamp: time.Now().UTC(),
		},
	}

	// Generate snapshot
	snap, err := gen.Generate(records, 1)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	// Verify
	if err := Verify(snap, pub); err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	// Tamper with snapshot
	snap.Header.Sequence = 2
	if err := Verify(snap, pub); err == nil {
		t.Fatal("Verify succeeded on tampered snapshot")
	}
}
