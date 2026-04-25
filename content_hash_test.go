/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ledger

import (
	"encoding/json"
	"testing"
	"time"
)

// TestContentHash_Stable verifies the same record content produces
// the same hash across calls. Foundation property — without it,
// dedup and reconciliation break.
func TestContentHash_Stable(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"key": "value"})
	r := Record{
		Topic:        "members",
		TenantID:     "tenant-a",
		NodeID:       "node-1",
		Body:         body,
		HLCTimestamp: 1234567890,
	}
	h1 := r.ContentHash()
	h2 := r.ContentHash()
	if h1 != h2 {
		t.Errorf("ContentHash should be stable; got %x then %x", h1, h2)
	}
}

// TestContentHash_DifferentNodesDiffer verifies records from
// different nodes produce different hashes even with identical
// other fields. NodeID is part of the canonical form.
func TestContentHash_DifferentNodesDiffer(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"k": "v"})
	a := Record{Topic: "members", NodeID: "a", Body: body, HLCTimestamp: 1}
	b := Record{Topic: "members", NodeID: "b", Body: body, HLCTimestamp: 1}
	if a.ContentHash() == b.ContentHash() {
		t.Error("different nodes should produce different hashes")
	}
}

// TestContentHash_SignatureExcluded verifies signature changes
// don't affect the hash. Two valid signatures over the same content
// (e.g. after key rotation) must dedup as identical content.
func TestContentHash_SignatureExcluded(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"k": "v"})
	a := Record{Topic: "t", NodeID: "n", Body: body, HLCTimestamp: 1, Signature: []byte("sig-1")}
	b := Record{Topic: "t", NodeID: "n", Body: body, HLCTimestamp: 1, Signature: []byte("sig-2")}
	if a.ContentHash() != b.ContentHash() {
		t.Error("signature should not affect ContentHash")
	}
}

// TestContentHash_TimestampExcluded verifies receive-timestamp
// doesn't affect the hash. HLC IS in the hash; wall-clock Timestamp
// is not.
func TestContentHash_TimestampExcluded(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"k": "v"})
	a := Record{Topic: "t", NodeID: "n", Body: body, HLCTimestamp: 1, Timestamp: time.Unix(100, 0)}
	b := Record{Topic: "t", NodeID: "n", Body: body, HLCTimestamp: 1, Timestamp: time.Unix(200, 0)}
	if a.ContentHash() != b.ContentHash() {
		t.Error("Timestamp (wall-clock) should not affect ContentHash; HLC does")
	}
}

// TestContentHash_TombstoneAffects verifies tombstoned vs
// non-tombstoned records of the same content produce different
// hashes. Tombstones are records, not the absence of records.
func TestContentHash_TombstoneAffects(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"k": "v"})
	live := Record{Topic: "t", NodeID: "n", Body: body, HLCTimestamp: 1}
	tombstoned := live
	tombstoned.Tombstone = true
	if live.ContentHash() == tombstoned.ContentHash() {
		t.Error("tombstone should affect ContentHash")
	}
}
