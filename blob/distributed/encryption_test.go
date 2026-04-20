/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestEncryptDecryptRoundtrip(t *testing.T) {
	seed := make([]byte, 32)
	rand.Read(seed)
	key, err := DeriveStorageKey(seed)
	if err != nil {
		t.Fatalf("DeriveStorageKey: %v", err)
	}
	if len(key) != 32 {
		t.Fatalf("key size: got %d want 32", len(key))
	}

	plaintext := make([]byte, 4096)
	rand.Read(plaintext)
	cid := computePieceCID(plaintext)

	ct, err := EncryptForStorage(key, cid, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if bytes.Equal(ct, plaintext) {
		t.Fatal("ciphertext equals plaintext")
	}

	pt, err := DecryptFromStorage(key, cid, ct)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatal("decrypted bytes differ from plaintext")
	}
}

func TestDecryptRejectsTamperedCiphertext(t *testing.T) {
	seed := make([]byte, 32)
	rand.Read(seed)
	key, _ := DeriveStorageKey(seed)
	pt := []byte("hello world")
	cid := computePieceCID(pt)
	ct, _ := EncryptForStorage(key, cid, pt)

	ct[0] ^= 0x01
	if _, err := DecryptFromStorage(key, cid, ct); err == nil {
		t.Fatal("expected AEAD failure, got nil")
	}
}

func TestDecryptRejectsWrongCID(t *testing.T) {
	seed := make([]byte, 32)
	rand.Read(seed)
	key, _ := DeriveStorageKey(seed)
	pt := []byte("plaintext")
	cid := computePieceCID(pt)
	ct, _ := EncryptForStorage(key, cid, pt)

	// Flip CID (changes AD AND nonce) -- must fail.
	otherCID := computePieceCID([]byte("other"))
	if _, err := DecryptFromStorage(key, otherCID, ct); err == nil {
		t.Fatal("expected failure with wrong CID, got nil")
	}
}
