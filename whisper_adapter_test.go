/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package ledger_test

import (
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
)

func adapterOver(c *cache.DirectoryCache) *lad.CacheStateStore {
	return lad.NewCacheStateStore(
		c,
		func(r lad.Record) ([]byte, error) { return json.Marshal(r) },
		func(b []byte) (lad.Record, error) {
			var r lad.Record
			err := json.Unmarshal(b, &r)
			return r, err
		},
	)
}

func applyMember(t *testing.T, c *cache.DirectoryCache, node string, pubKey string, created time.Time) {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{Partition: "p-1", NodeID: node, PubKey: []byte(pubKey), CreatedAt: created})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{Topic: lad.TopicMember, Body: body, Timestamp: created}); err != nil {
		t.Fatalf("apply member %s: %v", node, err)
	}
}

// Whisper's G2 digest skips the G1 exchange when two peers report the same
// non-zero fingerprint. Two caches holding the same member under different
// contents hold different records, so the adapter must report different
// fingerprints for them.
func TestCacheStateStore_SameKeysDifferentContentDifferentFingerprints(t *testing.T) {
	created := time.Now().Add(-time.Hour).UTC()
	a, b := cache.NewDirectoryCache(), cache.NewDirectoryCache()
	applyMember(t, a, "node-1", "key-a", created)
	applyMember(t, b, "node-1", "key-b", created)

	if fa, fb := adapterOver(a).Fingerprint(), adapterOver(b).Fingerprint(); fa == fb {
		t.Fatalf("caches holding node-1 with different keys reported the same fingerprint %#x", fa)
	}
}

// Two caches holding the same records report the same non-zero fingerprint.
func TestCacheStateStore_SameRecordsSameFingerprint(t *testing.T) {
	a, b := twoCachesWithTheSameMembers(t)
	sa, sb := adapterOver(a), adapterOver(b)
	if sa.Fingerprint() == 0 || sa.Fingerprint() != sb.Fingerprint() {
		t.Fatalf("caches holding the same records reported %#x and %#x", sa.Fingerprint(), sb.Fingerprint())
	}
}

func twoCachesWithTheSameMembers(t *testing.T) (*cache.DirectoryCache, *cache.DirectoryCache) {
	t.Helper()
	created := time.Now().Add(-time.Hour).UTC()
	a, b := cache.NewDirectoryCache(), cache.NewDirectoryCache()
	applyMember(t, a, "node-1", "key-1", created)
	applyMember(t, a, "node-2", "key-2", created)
	applyMember(t, b, "node-2", "key-2", created)
	applyMember(t, b, "node-1", "key-1", created)
	return a, b
}

// Whisper passes Delta the local time of this node's last exchange with a
// peer. A member its author created before that moment but that reached
// this cache after it must still be in the next Delta for that peer.
func TestCacheStateStore_LateArrivalReachesTheNextDelta(t *testing.T) {
	c := cache.NewDirectoryCache()
	lastExchange := time.Now()
	time.Sleep(2 * time.Millisecond)
	applyMember(t, c, "node-late", "key-late", time.Now().Add(-time.Hour).UTC())

	got := adapterOver(c).Delta(lastExchange)
	found := false
	for _, rec := range got {
		var r lad.Record
		if json.Unmarshal(rec, &r) == nil && r.NodeID == "node-late" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Delta(last exchange) left out a member that reached the cache after it (%d records returned)", len(got))
	}
}
