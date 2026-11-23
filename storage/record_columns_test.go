/*
 * Copyright (c) 2026 Brandon Miller-Mumford. Licensed under the MIT License; see LICENSE.
 */

package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	_ "modernc.org/sqlite"
)

// openSQLiteLedger opens a LocalDBLedger over a SQLite file of its own, the engine
// family the ledger runs on (a tursogo local file on a host).
func openSQLiteLedger(t *testing.T) (*LocalDBLedger, *sql.DB) {
	t.Helper()
	db := openSQLiteFile(t)
	l, err := NewLocalDBLedger(db, LedgerConfig{})
	if err != nil {
		t.Fatalf("NewLocalDBLedger: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, db
}

func openSQLiteFile(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "lad.db")+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func testKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}

// history reads every record the ledger holds through its stream's history.
func history(t *testing.T, l *LocalDBLedger) []lad.Record {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _, err := l.StreamWithEnd(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	head, err := l.Head(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var got []lad.Record
	timeout := time.After(5 * time.Second)
	for {
		select {
		case rec := <-ch:
			got = append(got, rec)
			if seqsCovered(got, head) {
				return got
			}
		case <-timeout:
			t.Fatalf("history delivered %d records before the timeout; head %v", len(got), head)
		}
	}
}

func seqsCovered(got []lad.Record, head lad.CausalWatermark) bool {
	seen := map[lad.Topic]uint64{}
	for _, r := range got {
		if r.Seq > seen[r.Topic] {
			seen[r.Topic] = r.Seq
		}
	}
	for topic, seq := range head {
		if seen[topic] < seq {
			return false
		}
	}
	return true
}

// snapshotRecords decodes every record Snapshot writes.
func snapshotRecords(t *testing.T, l *LocalDBLedger) []lad.Record {
	t.Helper()
	rc, err := l.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	dec := json.NewDecoder(rc)
	var out []lad.Record
	for dec.More() {
		var rec lad.Record
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("decode snapshot: %v", err)
		}
		out = append(out, rec)
	}
	return out
}

// fullRecord sets every field a Record carries to a value that is not its zero
// value, and signs it, so a field the store drops shows as a difference and as a
// signature that no longer verifies.
func fullRecord(t *testing.T, priv ed25519.PrivateKey) lad.Record {
	t.Helper()
	at := time.Date(2026, 11, 21, 10, 15, 30, 123456789, time.UTC)
	rec := lad.Record{
		Topic:           lad.TopicMember,
		Seq:             7,
		Partition:       "partition-1",
		NodeID:          "node-1",
		Body:            json.RawMessage(`{"nodeId":"node-1","partition":"partition-1"}`),
		Timestamp:       at,
		LamportClock:    41,
		HLCTimestamp:    1763700930123456789,
		Tombstone:       true,
		DeletedAt:       at.Add(987654321),
		TombstoneReason: "explicit",
		ExpiresAt:       at.Add(48*time.Hour + 5),
		BlobCID:         "bafkreigh2akiscaildc",
	}
	lad.SignRecord(&rec, priv)
	return rec
}

// sameRecord reports every field of got that differs from want.
func sameRecord(t *testing.T, where string, want, got lad.Record) {
	t.Helper()
	check := func(field string, ok bool) {
		if !ok {
			t.Errorf("%s: %s differs: got %+v", where, field, got)
		}
	}
	check("Topic", got.Topic == want.Topic)
	check("Seq", got.Seq == want.Seq)
	check("Partition", got.Partition == want.Partition)
	check("NodeID", got.NodeID == want.NodeID)
	check("Body", bytes.Equal(got.Body, want.Body))
	check("Signature", bytes.Equal(got.Signature, want.Signature))
	check("Timestamp", got.Timestamp.Equal(want.Timestamp))
	check("LamportClock", got.LamportClock == want.LamportClock)
	check("HLCTimestamp", got.HLCTimestamp == want.HLCTimestamp)
	check("Tombstone", got.Tombstone == want.Tombstone)
	check("DeletedAt", got.DeletedAt.Equal(want.DeletedAt))
	check("TombstoneReason", got.TombstoneReason == want.TombstoneReason)
	check("ExpiresAt", got.ExpiresAt.Equal(want.ExpiresAt))
	check("AuthorPubKey", bytes.Equal(got.AuthorPubKey, want.AuthorPubKey))
	check("BlobCID", got.BlobCID == want.BlobCID)
	if !lad.VerifyRecord(got) {
		t.Errorf("%s: the replayed record's signature does not verify", where)
	}
}

// A record read back through the stream's history and through Snapshot carries
// every field it was appended with, byte for byte, so its signature still
// verifies on replay.
func TestLocalDBLedger_AReplayedRecordKeepsEveryField(t *testing.T) {
	l, _ := openSQLiteLedger(t)
	want := fullRecord(t, testKey(t))
	if !lad.VerifyRecord(want) {
		t.Fatal("the record does not verify before it is stored")
	}
	if err := l.Append(context.Background(), want); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got := history(t, l)
	if len(got) != 1 {
		t.Fatalf("history holds %d records, want 1", len(got))
	}
	sameRecord(t, "history", want, got[0])

	snap := snapshotRecords(t, l)
	if len(snap) != 1 {
		t.Fatalf("snapshot holds %d records, want 1", len(snap))
	}
	sameRecord(t, "snapshot", want, snap[0])
}

// The body is stored as the bytes it was signed over, not re-encoded: a body
// with spacing a JSON encoder would remove reads back unchanged through the
// stream's history and still verifies.
func TestLocalDBLedger_StoresTheBodyBytesItWasGiven(t *testing.T) {
	l, _ := openSQLiteLedger(t)
	want := fullRecord(t, testKey(t))
	want.Body = json.RawMessage(`{ "nodeId": "node-1",  "partition": "partition-1" }`)
	lad.SignRecord(&want, testKey(t))
	if err := l.Append(context.Background(), want); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got := history(t, l)
	if len(got) != 1 {
		t.Fatalf("history holds %d records, want 1", len(got))
	}
	sameRecord(t, "spaced body", want, got[0])

	bad := fullRecord(t, testKey(t))
	bad.Seq = 9
	bad.Body = json.RawMessage(`{"nodeId":`)
	if err := l.Append(context.Background(), bad); err == nil {
		t.Error("a body that is not JSON was stored")
	}
}

// BatchAppend stores every field as Append does.
func TestLocalDBLedger_ABatchStoresEveryField(t *testing.T) {
	l, _ := openSQLiteLedger(t)
	priv := testKey(t)
	first := fullRecord(t, priv)
	second := fullRecord(t, priv)
	second.Seq = 8
	second.NodeID = "node-2"
	second.Tombstone = false
	second.DeletedAt = time.Time{}
	second.TombstoneReason = ""
	lad.SignRecord(&second, priv)
	if err := l.BatchAppend(context.Background(), []lad.Record{first, second}); err != nil {
		t.Fatalf("BatchAppend: %v", err)
	}
	got := map[uint64]lad.Record{}
	for _, r := range history(t, l) {
		got[r.Seq] = r
	}
	sameRecord(t, "batch first", first, got[7])
	sameRecord(t, "batch second", second, got[8])
}

// memberRecord is a signed live member record for node in partition at at.
func memberRecord(t *testing.T, priv ed25519.PrivateKey, seq uint64, node, partition string, at time.Time) lad.Record {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{NodeID: node, Partition: partition, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	rec := lad.Record{Topic: lad.TopicMember, Seq: seq, Partition: partition, NodeID: node, Body: body, Timestamp: at}
	lad.SignRecord(&rec, priv)
	return rec
}

func hasMember(t *testing.T, c *cache.DirectoryCache, partition, node string) bool {
	t.Helper()
	members, err := c.Members(context.Background(), partition)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if m.NodeID == node {
			return true
		}
	}
	return false
}

// A deletion replays as a deletion: a directory cache rebuilt from the ledger's
// history, as loom's synchroniser rebuilds one at start, does not hold a member
// whose tombstone the ledger stored after its live record. The first cache
// applies every record as replayed; the second also verifies each live record's
// signature, as the ACL on loom's signed topics does, and still holds the live
// member.
func TestLocalDBLedger_ATombstoneReplaysAsADeletion(t *testing.T) {
	l, _ := openSQLiteLedger(t)
	priv := testKey(t)
	at := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	live := memberRecord(t, priv, 1, "node-1", "partition-1", at)
	gone := memberRecord(t, priv, 2, "node-1", "partition-1", at.Add(time.Minute))
	gone.Tombstone = true
	gone.DeletedAt = at.Add(time.Minute)
	gone.TombstoneReason = "explicit"
	lad.SignRecord(&gone, priv)
	other := memberRecord(t, priv, 3, "node-2", "partition-1", at.Add(2*time.Minute))
	for _, rec := range []lad.Record{live, gone, other} {
		if err := l.Append(context.Background(), rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	replayed := history(t, l)

	plain := cache.NewDirectoryCache()
	for _, rec := range replayed {
		if err := plain.Apply(rec); err != nil {
			t.Errorf("Apply seq %d: %v", rec.Seq, err)
		}
	}
	if hasMember(t, plain, "partition-1", "node-1") {
		t.Error("node-1's tombstone replayed as a live record: the rebuilt cache holds the deleted member")
	}
	if !hasMember(t, plain, "partition-1", "node-2") {
		t.Error("the rebuilt cache does not hold node-2")
	}

	signed := cache.NewDirectoryCache()
	signed.RegisterACL(lad.TopicMember, func(_ string, _ []byte, rec lad.Record) error {
		if !lad.VerifyRecord(rec) {
			return lad.ErrSignatureInvalid
		}
		return nil
	})
	for _, rec := range replayed {
		if err := signed.Apply(rec); err != nil {
			t.Errorf("signed Apply seq %d: %v", rec.Seq, err)
		}
	}
	if hasMember(t, signed, "partition-1", "node-1") {
		t.Error("the signature-checking cache holds the deleted member")
	}
	if !hasMember(t, signed, "partition-1", "node-2") {
		t.Error("node-2's live record was refused on replay: the signature-checking cache does not hold it")
	}
}

// sevenColumnSchema is a ladRecords table holding only seven of the record's
// columns: topic, seq, partitionId, nodeId, body, signature and timestamp.
const sevenColumnSchema = `CREATE TABLE ladRecords (
	topic TEXT NOT NULL,
	seq INTEGER NOT NULL,
	partitionId TEXT NOT NULL,
	nodeId TEXT NOT NULL,
	body TEXT NOT NULL,
	signature BLOB,
	timestamp TEXT NOT NULL,
	PRIMARY KEY (topic, seq)
)`

// Opening a ledger over a ladRecords table with only seven of its columns adds
// the others. A row the table already held keeps what it holds and reads back
// with the added fields at their zero values; a record appended once the columns
// are added keeps every field.
func TestLocalDBLedger_OpeningAnOlderTableAddsTheMissingColumns(t *testing.T) {
	db := openSQLiteFile(t)
	if _, err := db.Exec(sevenColumnSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ladRecords (topic, seq, partitionId, nodeId, body, signature, timestamp)
		VALUES ('role', 1, 'partition-1', 'node-9', '{}', x'0102', '2026-11-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	l, err := NewLocalDBLedger(db, LedgerConfig{})
	if err != nil {
		t.Fatalf("NewLocalDBLedger over the older table: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	want := fullRecord(t, testKey(t))
	if err := l.Append(context.Background(), want); err != nil {
		t.Fatalf("Append after the columns were added: %v", err)
	}
	var old, added lad.Record
	for _, r := range history(t, l) {
		switch r.Topic {
		case lad.TopicRole:
			old = r
		case lad.TopicMember:
			added = r
		}
	}
	if old.NodeID != "node-9" || old.Seq != 1 || !bytes.Equal(old.Signature, []byte{1, 2}) || string(old.Body) != "{}" {
		t.Errorf("the older row changed: %+v", old)
	}
	if old.Tombstone || len(old.AuthorPubKey) != 0 || old.LamportClock != 0 || !old.DeletedAt.IsZero() || !old.ExpiresAt.IsZero() {
		t.Errorf("the older row reads added fields that are not zero: %+v", old)
	}
	sameRecord(t, "after the columns were added", want, added)

	// A second open finds every column and changes nothing.
	again, err := NewLocalDBLedger(db, LedgerConfig{})
	if err != nil {
		t.Fatalf("a second NewLocalDBLedger: %v", err)
	}
	_ = again.Close()
}
