/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package anchor

import (
	"time"

	lad "github.com/bbmumford/ledger"
)

// Generator generates signed snapshots from directory state.
type Generator struct {
	signer *Signer
}

// NewGenerator creates a new snapshot generator.
func NewGenerator(signer *Signer) *Generator {
	return &Generator{signer: signer}
}

// Generate creates a signed snapshot from the provided records.
func (g *Generator) Generate(records []lad.Record, seq uint64) (*Snapshot, error) {
	// Create content
	content := SnapshotContent{
		Timestamp: time.Now().UTC().Unix(),
		Sequence:  seq,
		NodeID:    g.signer.nodeID,
		Records:   records,
	}

	// Sign
	sig, err := g.signer.Sign(&content)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		Header: SnapshotHeader{
			Timestamp: content.Timestamp,
			Sequence:  content.Sequence,
			NodeID:    content.NodeID,
			Signature: sig,
		},
		Records: content.Records,
	}, nil
}
