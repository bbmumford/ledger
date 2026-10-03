package ledger

import (
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"
)

// The content hash is a DURABLE identity: peers dedupe and reconcile on it, so
// a change to the digest for unchanged inputs silently forks the directory —
// every peer treats the same record as a new one. Nothing about the Go source
// may move it: not renaming a field, not reordering the struct, not adding one.
//
// This pins the digest for a fixed record as a literal. It is deliberately a
// hard-coded expectation rather than a recomputation, because a test that
// recomputes the hash from the same code it is testing agrees with any change
// it makes and so proves nothing.
//
// The canonical form ComputeContentHash promises is
//
//	[len|topic][len|partition][len|node][len|body][hlc][tombstone]
//
// — VALUES only, never field names, which is what makes an identifier rename
// safe here. If this test fails after a rename, the rename touched a value.
func TestContentHashIsStableForAFixedRecord(t *testing.T) {
	rec := Record{
		Topic:        Topic("member"),
		Seq:          7,
		Partition:    "partition-a",
		NodeID:       "vl1_node_a",
		Body:         json.RawMessage(`{"role":"relay"}`),
		HLCTimestamp: 1234567890,
		Tombstone:    false,

		// Deliberately populated and deliberately NOT hashed: the canonical form
		// excludes the signature and the receive timestamp, so a re-signed or
		// re-received copy of the same content keeps its identity. If either
		// starts contributing, this test fails and that is the point.
		Signature: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		Timestamp: time.Unix(1700000000, 0).UTC(),
	}

	const want = "c45c2300f389d706f1113fb2773358ba"
	sum := rec.ContentHash()
	got := hex.EncodeToString(sum[:])
	if got != want {
		t.Errorf("content hash for the fixed record = %s, want %s\n"+
			"the digest moved for unchanged inputs — peers would treat existing "+
			"records as new ones. If this followed a field RENAME, something is "+
			"hashing a NAME rather than a VALUE.", got, want)
	}
}

// Signature and receive-time must stay outside the digest: two copies of one
// record that differ only in those fields are the SAME record to every peer.
func TestContentHashIgnoresSignatureAndReceiveTime(t *testing.T) {
	base := Record{
		Topic: Topic("member"), Seq: 1, Partition: "p1", NodeID: "n1",
		Body: json.RawMessage(`{}`), HLCTimestamp: 42,
	}
	resigned := base
	resigned.Signature = []byte{1, 2, 3}
	resigned.Timestamp = time.Unix(999, 0)

	if base.ContentHash() != resigned.ContentHash() {
		t.Error("re-signing or re-receiving a record changed its content hash; " +
			"the same content would reconcile as two distinct records")
	}
}

// Each hashed field must actually contribute, or two different records collide
// and one silently overwrites the other during reconciliation.
func TestContentHashSeparatesRecordsThatDifferInAHashedField(t *testing.T) {
	base := Record{
		Topic: Topic("member"), Seq: 1, Partition: "p1", NodeID: "n1",
		Body: json.RawMessage(`{}`), HLCTimestamp: 42,
	}
	for name, mutate := range map[string]func(*Record){
		"topic":     func(r *Record) { r.Topic = Topic("reach") },
		"partition": func(r *Record) { r.Partition = "p2" },
		"node":      func(r *Record) { r.NodeID = "n2" },
		"body":      func(r *Record) { r.Body = json.RawMessage(`{"x":1}`) },
		"hlc":       func(r *Record) { r.HLCTimestamp = 43 },
		"tombstone": func(r *Record) { r.Tombstone = true },
	} {
		other := base
		mutate(&other)
		if base.ContentHash() == other.ContentHash() {
			t.Errorf("records differing only in %s share a content hash; "+
				"reconciliation would treat them as one record", name)
		}
	}
}
