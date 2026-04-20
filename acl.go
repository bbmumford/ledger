/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import "errors"

var (
	// ErrACLDenied is returned when an ACL check rejects a record.
	ErrACLDenied = errors.New("ledger: record rejected by ACL")
)

// ACLFunc checks whether a record is allowed to be applied to a topic.
// Called in Apply() before merge. Return nil to allow, error to reject.
type ACLFunc func(topic string, authorPubKey []byte, record Record) error

// IndexFunc derives secondary index keys from a record.
// Returns zero or more index keys that will be maintained automatically.
type IndexFunc func(Record) []string
