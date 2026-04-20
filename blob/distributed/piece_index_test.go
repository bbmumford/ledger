/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func TestSignAndValidateManifest(t *testing.T) {
	data := make([]byte, 512*1024)
	rand.Read(data)
	m, _, err := Split(data, 0)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	sig, err := SignManifest(m, priv)
	if err != nil {
		t.Fatalf("SignManifest: %v", err)
	}
	if err := ValidateManifest(m, sig, pub); err != nil {
		t.Fatalf("ValidateManifest: %v", err)
	}
}

func TestValidateManifestTamperedRoot(t *testing.T) {
	data := make([]byte, 512*1024)
	rand.Read(data)
	m, _, _ := Split(data, 0)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	sig, _ := SignManifest(m, priv)

	// Tamper with root cid AFTER signing.
	orig := m.RootCID
	runes := []byte(m.RootCID)
	runes[0] ^= 0x01
	m.RootCID = string(runes)
	if err := ValidateManifest(m, sig, pub); err == nil {
		t.Fatal("expected failure on tampered root, got nil")
	}
	m.RootCID = orig
}

func TestValidateManifestTamperedSignature(t *testing.T) {
	data := make([]byte, 200*1024)
	rand.Read(data)
	m, _, _ := Split(data, 0)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	sig, _ := SignManifest(m, priv)

	sig[0] ^= 0x01
	if err := ValidateManifest(m, sig, pub); err == nil {
		t.Fatal("expected failure on tampered sig, got nil")
	}
}

func TestValidateManifestTamperedPiece(t *testing.T) {
	data := make([]byte, 600*1024)
	rand.Read(data)
	m, _, _ := Split(data, 0)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	sig, _ := SignManifest(m, priv)

	// Tamper with one piece's CID. Signature still matches the modified
	// manifest bytes? No -- signature was over unmodified bytes, so any
	// mutation should trip the signature check. This doubles as a test
	// that signatures are tight.
	orig := m.Pieces[0].CID
	runes := []byte(m.Pieces[0].CID)
	runes[0] ^= 0x01
	m.Pieces[0].CID = string(runes)
	if err := ValidateManifest(m, sig, pub); err == nil {
		t.Fatal("expected failure on tampered piece cid, got nil")
	}
	m.Pieces[0].CID = orig
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	data := make([]byte, 100*1024)
	rand.Read(data)
	m, _, _ := Split(data, 0)
	enc, err := EncodeManifest(m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeManifest(enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.RootCID != m.RootCID || got.TotalBytes != m.TotalBytes {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, m)
	}
}
