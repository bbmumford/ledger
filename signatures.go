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
// Sets AuthorPubKey and Signature fields on the record.
func SignRecord(rec *Record, privateKey ed25519.PrivateKey) {
	content := signatureContent(rec)
	rec.AuthorPubKey = privateKey.Public().(ed25519.PublicKey)
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
// Includes: Topic, NodeID, TenantID, Body, Timestamp, LamportClock, Seq
func signatureContent(rec *Record) []byte {
	sep := []byte{0} // null byte separator

	var buf []byte
	buf = append(buf, []byte(rec.Topic)...)
	buf = append(buf, sep...)
	buf = append(buf, []byte(rec.NodeID)...)
	buf = append(buf, sep...)
	buf = append(buf, []byte(rec.TenantID)...)
	buf = append(buf, sep...)
	buf = append(buf, []byte(rec.Body)...)
	buf = append(buf, sep...)

	// Timestamp as RFC3339Nano for deterministic encoding
	buf = append(buf, []byte(rec.Timestamp.UTC().Format("2006-01-02T15:04:05.000000000Z"))...)
	buf = append(buf, sep...)

	// LamportClock as big-endian uint64
	var lcBuf [8]byte
	binary.BigEndian.PutUint64(lcBuf[:], rec.LamportClock)
	buf = append(buf, lcBuf[:]...)
	buf = append(buf, sep...)

	// Seq as big-endian uint64
	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], rec.Seq)
	buf = append(buf, seqBuf[:]...)

	return buf
}
