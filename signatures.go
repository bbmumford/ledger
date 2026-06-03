/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
)

var (
	ErrSignatureRequired = errors.New("ledger: record requires signature")
	ErrSignatureInvalid  = errors.New("ledger: invalid record signature")
)

// SignRecord signs a record's content with the given Ed25519 private key.
// Sets AuthorPubKey and Signature fields on the record. AuthorPubKey
// MUST be assigned before computing signatureContent — signatureContent
// reads rec.AuthorPubKey, so signing the record with an empty pubkey
// here produces bytes the receiver cannot reproduce (the wire carries
// the 32-byte pubkey, but the signed bytes would carry a 4-byte zero
// length). The ordering bug here is what every signed-topic record on
// the wire was failing verify against — VerifyRecord's
// reconstructed content always disagreed with the signer's content.
func SignRecord(rec *Record, privateKey ed25519.PrivateKey) {
	rec.AuthorPubKey = privateKey.Public().(ed25519.PublicKey)
	content := signatureContent(rec)
	rec.Signature = ed25519.Sign(privateKey, content)
}

// VerifyRecord checks the Ed25519 signature on a record.
func VerifyRecord(rec Record) bool {
	if len(rec.AuthorPubKey) == 0 || len(rec.Signature) == 0 {
		return false
	}
	content := signatureContent(&rec)
	return ed25519.Verify(rec.AuthorPubKey, content, rec.Signature)
}

// signatureContent builds the canonical byte string that is signed.
//
// Covers every field that travels on the wire (gossip codec writes
// fields 1-15 of LADRecord, so every one of them must be in the
// signature input or peers can tamper with the unsigned ones in
// transit).
//
// Fields signed (in order):
//
//	Topic, NodeID, TenantID, Body
//	Timestamp (RFC3339Nano)
//	LamportClock (BE u64)
//	Seq (BE u64)
//	HLCTimestamp (BE u64)
//	ExpiresAt.UnixNano (BE i64, 0 when IsZero)
//	Tombstone (1 byte)
//	DeletedAt.UnixNano (BE i64, 0 when IsZero)
//	TombstoneReason (len-prefixed bytes)
//	AuthorPubKey (len-prefixed bytes)
//	BlobCID (len-prefixed bytes)
func signatureContent(rec *Record) []byte {
	sep := []byte{0} // null byte separator

	var buf []byte
	buf = append(buf, []byte(rec.Topic)...)
	buf = append(buf, sep...)
	buf = append(buf, []byte(rec.NodeID)...)
	buf = append(buf, sep...)
	buf = append(buf, []byte(rec.TenantID)...)
	buf = append(buf, sep...)
	buf = appendLenPrefixed(buf, rec.Body)

	// Timestamp as RFC3339Nano for deterministic encoding
	buf = append(buf, []byte(rec.Timestamp.UTC().Format("2006-01-02T15:04:05.000000000Z"))...)
	buf = append(buf, sep...)

	// LamportClock as big-endian uint64
	var u64Buf [8]byte
	binary.BigEndian.PutUint64(u64Buf[:], rec.LamportClock)
	buf = append(buf, u64Buf[:]...)
	buf = append(buf, sep...)

	// Seq as big-endian uint64
	binary.BigEndian.PutUint64(u64Buf[:], rec.Seq)
	buf = append(buf, u64Buf[:]...)
	buf = append(buf, sep...)

	// HLCTimestamp — IBLT tie-break key. Unsigned in v1; a relayer
	// could rewrite this to manipulate causal-ordering decisions on
	// receivers.
	binary.BigEndian.PutUint64(u64Buf[:], rec.HLCTimestamp)
	buf = append(buf, u64Buf[:]...)
	buf = append(buf, sep...)

	// ExpiresAt — TTL eviction trigger. Unsigned in v1; a relayer
	// could extend or shorten any record's lifetime.
	var i64Buf [8]byte
	if rec.ExpiresAt.IsZero() {
		binary.BigEndian.PutUint64(i64Buf[:], 0)
	} else {
		binary.BigEndian.PutUint64(i64Buf[:], uint64(rec.ExpiresAt.UnixNano()))
	}
	buf = append(buf, i64Buf[:]...)
	buf = append(buf, sep...)

	// Tombstone bit + DeletedAt + TombstoneReason — unsigned in v1
	// meant any peer could fabricate a deletion marker by flipping
	// these on an otherwise-valid record.
	var tombByte byte
	if rec.Tombstone {
		tombByte = 1
	}
	buf = append(buf, tombByte)
	buf = append(buf, sep...)

	if rec.DeletedAt.IsZero() {
		binary.BigEndian.PutUint64(i64Buf[:], 0)
	} else {
		binary.BigEndian.PutUint64(i64Buf[:], uint64(rec.DeletedAt.UnixNano()))
	}
	buf = append(buf, i64Buf[:]...)
	buf = append(buf, sep...)

	buf = appendLenPrefixed(buf, []byte(rec.TombstoneReason))

	// AuthorPubKey — included so an attacker can't strip+resign with a
	// different key, leaving a record that claims an unrelated author.
	buf = appendLenPrefixed(buf, rec.AuthorPubKey)

	// BlobCID — content-addressed blob reference. Unsigned in v1; a
	// relayer could swap in any CID, redirecting blob resolution.
	buf = appendLenPrefixed(buf, []byte(rec.BlobCID))

	return buf
}

// appendLenPrefixed appends len(b) as a big-endian uint32 followed by
// b itself. Used inside signatureContent so variable-length fields can
// be concatenated unambiguously without separator-injection ambiguity.
func appendLenPrefixed(dst []byte, b []byte) []byte {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	dst = append(dst, lenBuf[:]...)
	dst = append(dst, b...)
	return dst
}
