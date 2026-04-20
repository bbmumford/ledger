/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// StorageKeyContext is the HKDF context string for deriving the on-disk
// storage key from the node's identity private key.
const StorageKeyContext = "plutus-blob-storage-v1"

// ErrShortCiphertext is returned when a ciphertext is too small to contain
// the AEAD tag.
var ErrShortCiphertext = errors.New("distributed: ciphertext too short")

// DeriveStorageKey produces a 32-byte XChaCha20-Poly1305 key from a node's
// identity material via HKDF-SHA256. The input may be a 32-byte Ed25519
// seed or the full 64-byte private key -- HKDF treats it as opaque IKM.
func DeriveStorageKey(identityPriv []byte) ([]byte, error) {
	if len(identityPriv) == 0 {
		return nil, errors.New("distributed: empty identity key")
	}
	h := hkdf.New(sha256.New, identityPriv, nil, []byte(StorageKeyContext))
	key := make([]byte, chacha20poly1305.KeySize) // 32
	if _, err := h.Read(key); err != nil {
		return nil, fmt.Errorf("distributed: hkdf: %w", err)
	}
	return key, nil
}

// nonceForCID derives a 24-byte XChaCha20-Poly1305 nonce from the first 24
// bytes of BLAKE2b-256(cid). Deterministic -- the same (key, cid) pair
// always produces the same nonce, which is safe because CIDs are
// content-addressed (if the plaintext changes, the CID changes).
func nonceForCID(cid string) []byte {
	raw, err := hex.DecodeString(cid)
	if err != nil || len(raw) == 0 {
		raw = []byte(cid)
	}
	sum := blake2b.Sum256(raw)
	nonce := make([]byte, chacha20poly1305.NonceSizeX) // 24
	copy(nonce, sum[:])
	return nonce
}

// EncryptForStorage encrypts a plaintext chunk for on-disk storage. The
// CID must be the CID of the PLAINTEXT chunk. The CID is bound in as
// associated data so a chunk cannot be stored under the wrong key.
func EncryptForStorage(key []byte, cid string, plaintext []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("distributed: bad key size %d", len(key))
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("distributed: aead: %w", err)
	}
	nonce := nonceForCID(cid)
	return aead.Seal(nil, nonce, plaintext, []byte(cid)), nil
}

// DecryptFromStorage reverses EncryptForStorage. Returns an error if the
// AEAD tag fails (tampered ciphertext or wrong key/CID).
func DecryptFromStorage(key []byte, cid string, ciphertext []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("distributed: bad key size %d", len(key))
	}
	if len(ciphertext) < chacha20poly1305.Overhead {
		return nil, ErrShortCiphertext
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("distributed: aead: %w", err)
	}
	nonce := nonceForCID(cid)
	pt, err := aead.Open(nil, nonce, ciphertext, []byte(cid))
	if err != nil {
		return nil, fmt.Errorf("distributed: decrypt: %w", err)
	}
	return pt, nil
}
