/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package cache

import (
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

func mustApply(t *testing.T, c *DirectoryCache, topic lad.Topic, v any) {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	if err := c.Apply(lad.Record{Topic: topic, Body: body, Timestamp: time.Now()}); err != nil {
		t.Fatalf("apply %v: %v", topic, err)
	}
}

func applyMember(t *testing.T, c *DirectoryCache, tenant, node string) {
	mustApply(t, c, lad.TopicMember, lad.MemberRecord{
		TenantID: tenant, NodeID: node, PubKey: []byte("k-" + node), CreatedAt: time.Now(),
	})
}

func populateCache(t *testing.T, c *DirectoryCache) {
	t.Helper()
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		applyMember(t, c, "tenant-1", "member-"+n)
	}
	for _, n := range []string{"a", "b", "c"} {
		mustApply(t, c, lad.TopicRole, lad.RoleRecord{
			TenantID: "tenant-1", NodeID: "role-" + n, Roles: []string{"auth"}, Updated: time.Now(),
		})
	}
	for _, n := range []string{"a", "b"} {
		mustApply(t, c, lad.TopicReach, lad.ReachRecord{
			TenantID: "tenant-1", NodeID: "reach-" + n,
			Addresses: []lad.ReachAddress{{Host: "10.0.0.1", Port: 8000, Proto: "tcp", Scope: "lan"}},
			Region:    "iad", ExpiresAt: time.Now().Add(time.Hour),
		})
	}
}

// The defining invariant: the XOR of every bucket equals the scalar Fingerprint
// — same keys, same hash, just partitioned. This is what lets the cheap scalar
// probe stay the first line and the bucket vector be the second-level diff.
func TestBucketDigestSumsToFingerprint(t *testing.T) {
	c := NewDirectoryCache()
	populateCache(t, c)

	for _, n := range []uint16{1, 8, 64, 256} {
		buckets := c.BucketDigest(n)
		if len(buckets) != int(n) {
			t.Fatalf("BucketDigest(%d) returned %d buckets", n, len(buckets))
		}
		var xor uint64
		for _, b := range buckets {
			xor ^= b
		}
		if xor != c.Fingerprint() {
			t.Fatalf("XOR(BucketDigest(%d))=%#x != Fingerprint()=%#x", n, xor, c.Fingerprint())
		}
	}
}

// Adding one key must change exactly one bucket — the locality property that
// makes a single-record divergence cost ~1/N of a full cache dump.
func TestBucketDigestSingleKeyFlipsOneBucket(t *testing.T) {
	c := NewDirectoryCache()
	populateCache(t, c)

	before := c.BucketDigest(64)
	applyMember(t, c, "tenant-1", "member-new")
	after := c.BucketDigest(64)

	changed := 0
	for i := range before {
		if before[i] != after[i] {
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("adding one member changed %d buckets, want exactly 1", changed)
	}
}

func allBuckets(n int) []bool {
	w := make([]bool, n)
	for i := range w {
		w[i] = true
	}
	return w
}

func onlyBucket(b, n int) []bool {
	w := make([]bool, n)
	w[b] = true
	return w
}

func containsNode(recs []lad.Record, nodeID string) bool {
	for _, r := range recs {
		if r.NodeID == nodeID {
			return true
		}
	}
	return false
}

// Single-bucket dumps must partition the all-buckets dump — every bucketed
// record lands in exactly one bucket, no overlap, full coverage.
func TestDumpBuckets_Partitions(t *testing.T) {
	c := NewDirectoryCache()
	populateCache(t, c)
	all := c.DumpBuckets(64, allBuckets(64))
	if len(all) == 0 {
		t.Fatal("expected some bucketed records")
	}
	sum := 0
	for b := 0; b < 64; b++ {
		sum += len(c.DumpBuckets(64, onlyBucket(b, 64)))
	}
	if sum != len(all) {
		t.Fatalf("single-bucket dumps must partition the all-buckets dump: sum=%d all=%d", sum, len(all))
	}
}

// The decisive cross-check: BucketDigest and DumpBuckets must agree on a
// record's bucket. Adding one member flips exactly one digest bucket; that same
// member must appear in DumpBuckets for that bucket and no other — so a
// diverging-bucket set computed from two digests selects exactly the right
// records for the follow-up.
func TestDumpBuckets_AgreesWithBucketDigest(t *testing.T) {
	c := NewDirectoryCache()
	populateCache(t, c)
	before := c.BucketDigest(64)
	applyMember(t, c, "tenant-1", "member-probe")
	after := c.BucketDigest(64)

	d := -1
	for i := range before {
		if before[i] != after[i] {
			if d != -1 {
				t.Fatal("expected exactly one changed digest bucket")
			}
			d = i
		}
	}
	if d == -1 {
		t.Fatal("no digest bucket changed after adding a member")
	}
	if !containsNode(c.DumpBuckets(64, onlyBucket(d, 64)), "member-probe") {
		t.Fatalf("new member must be in its digest bucket %d", d)
	}
	others := allBuckets(64)
	others[d] = false
	if containsNode(c.DumpBuckets(64, others), "member-probe") {
		t.Fatal("new member must NOT appear in any other bucket")
	}
}

// buckets is clamped to [1, maxBucketDigest] so a caller cannot demand a
// zero-length or unbounded digest.
func TestBucketDigestClamps(t *testing.T) {
	c := NewDirectoryCache()
	populateCache(t, c)
	if got := len(c.BucketDigest(0)); got != 1 {
		t.Fatalf("BucketDigest(0) len = %d, want 1", got)
	}
	if got := len(c.BucketDigest(60000)); got != int(maxBucketDigest) {
		t.Fatalf("BucketDigest(60000) len = %d, want %d", got, maxBucketDigest)
	}
}
