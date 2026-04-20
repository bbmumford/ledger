/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
// Package ledger provides implementations of the LAD Ledger interface for
// storing and synchronizing peer discovery records.
//
// Three implementations are available:
//   - LocalDBLedger: Persistent storage using embedded libSQL (production default)
//   - MemoryLedger: In-memory storage for testing or ephemeral deployments
//   - MeshLedger: Raft-replicated storage via tursoraft (multi-node production)
//
// Selection is controlled by LADStorage config: "local" (default), "memory", or "mesh".
package storage

import (
	lad "github.com/bbmumford/ledger"
)

// subscriber represents a client subscribed to ledger updates.
// Used by both LocalDBLedger and MemoryLedger.
type subscriber struct {
	id     int
	topics map[lad.Topic]struct{}
	ch     chan lad.Record
}

// broadcast sends a record to all matching subscribers.
// Shared broadcast logic used by both ledger implementations.
func broadcast(subs []*subscriber, rec lad.Record) {
	for _, sub := range subs {
		// Filter by topic if subscriber has topic filter
		if len(sub.topics) > 0 {
			if _, ok := sub.topics[rec.Topic]; !ok {
				continue
			}
		}
		select {
		case sub.ch <- rec:
		default:
			// Channel full, drop record (subscriber too slow)
		}
	}
}
