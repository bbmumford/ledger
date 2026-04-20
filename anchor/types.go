/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package anchor

import (
	lad "github.com/bbmumford/ledger"
)

// Snapshot represents a compressed, authoritative view of the network state.
type Snapshot struct {
	Header  SnapshotHeader `json:"header"`
	Records []lad.Record   `json:"records"`
}

// SnapshotHeader contains metadata and the cryptographic signature.
type SnapshotHeader struct {
	Timestamp int64  `json:"ts"`
	Sequence  uint64 `json:"seq"`
	NodeID    string `json:"node_id"` // ID of the anchor node that generated this
	Signature []byte `json:"sig"`     // Ed25519 signature of the snapshot content
}

// SnapshotContent is the struct used for signing (excludes the signature itself).
type SnapshotContent struct {
	Timestamp int64        `json:"ts"`
	Sequence  uint64       `json:"seq"`
	NodeID    string       `json:"node_id"`
	Records   []lad.Record `json:"records"`
}
