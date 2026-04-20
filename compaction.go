/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import "time"

// CompactionPolicy controls how old records are pruned.
type CompactionPolicy struct {
	MaxTombstoneAge time.Duration // prune tombstones older than this (0 = keep all)
	MaxVersions     int           // keep last N versions per key (0 = unlimited)
	MaxRecords      int           // hard cap per topic (0 = unlimited)
}

// DefaultCompactionPolicy returns a sensible default.
func DefaultCompactionPolicy() CompactionPolicy {
	return CompactionPolicy{
		MaxTombstoneAge: 24 * time.Hour,
	}
}
