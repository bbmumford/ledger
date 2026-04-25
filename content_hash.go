/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ledger

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// ContentHashLen is the size of a content hash in bytes (16 — half
// of SHA-256, sufficient at fleet scales up to ~10⁸ records).
const ContentHashLen = 16

// ContentHash is the canonical content-addressed identifier for a
// record. Used by:
//   - whisper.iblt for set reconciliation cell keys
//   - whisper.RumorPusher for ACK identity (rumor ID = content hash)
//   - whisper.ReconcileStore.FetchByID lookups
//   - cache projection layers (IdentityView)
//
// Deterministic across nodes given the same record content. Two
// records with identical (Topic, NodeID, Body, HLCTimestamp,
// Tombstone) produce the same hash. Signature is deliberately
// excluded — multiple valid signatures over the same content (e.g.
// after a key rotation) all produce the same hash, so dedup
// remains correct across rotations.
type ContentHash [ContentHashLen]byte

// ComputeContentHash returns the canonical content hash for a
// record. Stable across encoder versions: hashes the canonicalised
// byte representation of (Topic, TenantID, NodeID, Body, HLC,
// Tombstone) — never the signature, never the receive timestamp.
//
// The canonical form is fixed at:
//
//	[len(topic)|topic][len(tenant)|tenant][len(node)|node]
//	[len(body)|body][hlc_uint64][tombstone_byte]
//
// All length prefixes are big-endian uint32. SHA-256 truncated to
// the first 16 bytes.
func (r Record) ContentHash() ContentHash {
	h := sha256.New()
	writeLenPrefixed(h, []byte(r.Topic))
	writeLenPrefixed(h, []byte(r.TenantID))
	writeLenPrefixed(h, []byte(r.NodeID))
	writeLenPrefixed(h, r.Body)
	var hlc [8]byte
	binary.BigEndian.PutUint64(hlc[:], r.HLCTimestamp)
	h.Write(hlc[:])
	if r.Tombstone {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	full := h.Sum(nil)
	var out ContentHash
	copy(out[:], full[:ContentHashLen])
	return out
}

// writeLenPrefixed writes a length-prefixed byte slice for hash
// canonicalisation. 4-byte big-endian length, then the bytes.
func writeLenPrefixed(h interface{ Write([]byte) (int, error) }, b []byte) {
	var ln [4]byte
	binary.BigEndian.PutUint32(ln[:], uint32(len(b)))
	h.Write(ln[:])
	h.Write(b)
}

// IdentityView is a cache-projection row representing a single
// (NodeID, Topic) pair's most recent record. Used by reconciliation
// cell generation so the IBLT operates over per-(node, topic) keys
// without requiring Member and Reach to unify on the wire.
//
// Cache-only — never serialised to the wire. The wire format keeps
// Member and Reach as separate signed records per §5.27 of the
// 2026-04-23 reach publisher redesign.
type IdentityView struct {
	NodeID      string
	Topic       string
	ContentHash ContentHash
	HLC         uint64
	UpdatedAt   int64 // unix nano; consumers convert to time.Time as needed
}

// IdentityViewKey returns the canonical key for an IdentityView,
// suitable for indexing in the cache projection map.
func IdentityViewKey(nodeID, topic string) string {
	return nodeID + "|" + topic
}

// SortedHashes returns the content hashes from a slice of
// IdentityViews in deterministic order. Used by reconciliation
// drivers that need a consistent IBLT input ordering across nodes.
func SortedHashes(views []IdentityView) []ContentHash {
	out := make([]ContentHash, len(views))
	for i, v := range views {
		out[i] = v.ContentHash
	}
	sort.Slice(out, func(i, j int) bool {
		for k := 0; k < ContentHashLen; k++ {
			if out[i][k] != out[j][k] {
				return out[i][k] < out[j][k]
			}
		}
		return false
	})
	return out
}
