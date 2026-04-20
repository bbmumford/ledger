/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"sync"
	"time"
)

// ConflictRecord captures the details of a merge conflict resolution.
type ConflictRecord struct {
	Topic     Topic
	Key       string
	Winner    Record
	Loser     Record
	Timestamp time.Time
	MergeFunc string // name of merge function that resolved it
}

// ConflictLog is a ring buffer of recent merge conflicts per topic.
type ConflictLog struct {
	mu         sync.Mutex
	entries    []ConflictRecord
	maxEntries int
	enabled    bool
}

// NewConflictLog creates a conflict log with the given capacity.
func NewConflictLog(maxEntries int) *ConflictLog {
	if maxEntries <= 0 {
		maxEntries = 100
	}
	return &ConflictLog{
		entries:    make([]ConflictRecord, 0, maxEntries),
		maxEntries: maxEntries,
		enabled:    true,
	}
}

// Record adds a conflict to the log.
func (cl *ConflictLog) Record(c ConflictRecord) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if !cl.enabled {
		return
	}
	cl.entries = append(cl.entries, c)
	if len(cl.entries) > cl.maxEntries {
		cl.entries = cl.entries[1:]
	}
}

// Entries returns all logged conflicts, optionally filtered by topic.
func (cl *ConflictLog) Entries(topic Topic, limit int) []ConflictRecord {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	var result []ConflictRecord
	for i := len(cl.entries) - 1; i >= 0; i-- {
		if topic != "" && cl.entries[i].Topic != topic {
			continue
		}
		result = append(result, cl.entries[i])
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result
}

// SetEnabled enables or disables conflict logging.
func (cl *ConflictLog) SetEnabled(enabled bool) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.enabled = enabled
}

// Clear removes all logged conflicts.
func (cl *ConflictLog) Clear() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.entries = cl.entries[:0]
}
