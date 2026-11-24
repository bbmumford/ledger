/*
 * Copyright (c) 2026 Brandon Miller-Mumford. Licensed under the MIT License; see LICENSE.
 */

package storage

import (
	"bytes"
	"testing"

	lad "github.com/bbmumford/ledger"
)

// authorKeyInOtherCaseSchema is a ladRecords table whose author key column is
// spelled authorPubkey, the spelling LAD files written before the column took
// its rule name hold, beside the seven columns every ladRecords table has.
const authorKeyInOtherCaseSchema = `CREATE TABLE ladRecords (
	topic TEXT NOT NULL,
	seq INTEGER NOT NULL,
	partitionId TEXT NOT NULL,
	nodeId TEXT NOT NULL,
	body TEXT NOT NULL,
	signature BLOB,
	timestamp TEXT NOT NULL,
	authorPubkey BLOB,
	PRIMARY KEY (topic, seq)
)`

// A table that spells a column in another case already holds it: SQLite matches
// column names without case, so the open adds only the columns the table lacks
// and the author key a row holds reads back under the rule name.
func TestLocalDBLedger_AColumnSpelledInAnotherCaseIsTheColumn(t *testing.T) {
	db := openSQLiteFile(t)
	if _, err := db.Exec(authorKeyInOtherCaseSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ladRecords (topic, seq, partitionId, nodeId, body, signature, timestamp, authorPubkey)
		VALUES ('role', 1, 'partition-1', 'node-9', '{}', x'0102', '2026-11-01T00:00:00Z', x'0a0b')`); err != nil {
		t.Fatal(err)
	}
	l, err := NewLocalDBLedger(db, LedgerConfig{})
	if err != nil {
		t.Fatalf("NewLocalDBLedger over a table spelling authorPubkey: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	var old lad.Record
	for _, r := range history(t, l) {
		if r.Topic == lad.TopicRole {
			old = r
		}
	}
	if !bytes.Equal(old.AuthorPubKey, []byte{0x0a, 0x0b}) {
		t.Fatalf("the row's author key = %x, want 0a0b read under authorPubKey", old.AuthorPubKey)
	}
}
