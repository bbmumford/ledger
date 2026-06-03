/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"
)

// TestSignRecordVerifyRoundtrip locks the SignRecord/VerifyRecord
// canonical-bytes ordering: AuthorPubKey is assigned BEFORE
// signatureContent runs, so the signer hashes the same bytes the
// receiver reconstructs.
func TestSignRecordVerifyRoundtrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	rec := Record{
		Topic:        "test.topic",
		NodeID:       "vl1_test",
		TenantID:     "tenant-a",
		Body:         []byte(`{"hello":"world"}`),
		Timestamp:    time.Date(2026, 6, 4, 12, 0, 0, 123456789, time.UTC),
		LamportClock: 42,
		Seq:          7,
		HLCTimestamp: 1717495200000000000,
		ExpiresAt:    time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC),
	}
	SignRecord(&rec, priv)
	if len(rec.AuthorPubKey) != ed25519.PublicKeySize {
		t.Fatalf("AuthorPubKey should be 32 bytes after SignRecord, got %d", len(rec.AuthorPubKey))
	}
	if string(rec.AuthorPubKey) != string(pub) {
		t.Fatal("AuthorPubKey should match the signer's public key")
	}
	if !VerifyRecord(rec) {
		t.Fatal("VerifyRecord returned false on a record signed by SignRecord — canonical-bytes ordering is broken")
	}
}

// TestSignRecordRejectsBodyTamper guards the canonical signing input —
// any byte flipped in the signed payload must break verification.
func TestSignRecordRejectsBodyTamper(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	rec := Record{
		Topic:     "test.topic",
		NodeID:    "vl1_test",
		Body:      []byte("original"),
		Timestamp: time.Now().UTC(),
	}
	SignRecord(&rec, priv)
	rec.Body = []byte("tampered")
	if VerifyRecord(rec) {
		t.Fatal("VerifyRecord must reject a record whose Body has been mutated post-signing")
	}
}

// TestSignRecordRejectsPubKeyTamper covers the rebind-attack defense:
// signatureContent includes AuthorPubKey, so swapping it post-sign
// breaks verification even if the attacker re-signs with the new key.
func TestSignRecordRejectsPubKeyTamper(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	rec := Record{
		Topic:     "test.topic",
		NodeID:    "vl1_test",
		Body:      []byte("body"),
		Timestamp: time.Now().UTC(),
	}
	SignRecord(&rec, priv)
	rec.AuthorPubKey = otherPub
	if VerifyRecord(rec) {
		t.Fatal("VerifyRecord must reject a record whose AuthorPubKey has been swapped")
	}
}

// TestVerifyRecordRejectsEmptyFields shortcircuits — empty pubkey or
// signature is not a valid record, regardless of payload contents.
func TestVerifyRecordRejectsEmptyFields(t *testing.T) {
	if VerifyRecord(Record{}) {
		t.Fatal("VerifyRecord must reject a record with no AuthorPubKey/Signature")
	}
	rec := Record{AuthorPubKey: make([]byte, ed25519.PublicKeySize)}
	if VerifyRecord(rec) {
		t.Fatal("VerifyRecord must reject a record with empty Signature")
	}
}
