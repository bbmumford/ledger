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

// A ledger stream delivers a record twice when the record lands in both its
// history read and its live queue, and a consumer that restarts its stream
// from an earlier watermark applies records it already applied. These tests
// apply such repeats and check the cache ends where one application left it.

const repeatPartition = "partition-1"

func repeatMember(t *testing.T, nodeID string, seq uint64, at time.Time) lad.Record {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{Partition: repeatPartition, NodeID: nodeID, PubKey: []byte("k-" + nodeID), CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return lad.Record{Topic: lad.TopicMember, Partition: repeatPartition, NodeID: nodeID, Seq: seq, Timestamp: at, Body: body}
}

func repeatTombstone(topic lad.Topic, nodeID string, seq uint64, at time.Time) lad.Record {
	return lad.Record{Topic: topic, Partition: repeatPartition, NodeID: nodeID, Seq: seq, Timestamp: at, Tombstone: true, DeletedAt: at, TombstoneReason: "explicit"}
}

func repeatReach(t *testing.T, nodeID string, seq uint64, at time.Time, host string) lad.Record {
	t.Helper()
	body, err := json.Marshal(lad.ReachRecord{
		Partition: repeatPartition,
		NodeID:    nodeID,
		Addresses: []lad.ReachAddress{{Host: host, Port: 8000, Proto: "tcp", Scope: "lan"}},
		ExpiresAt: at.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	return lad.Record{Topic: lad.TopicReach, Partition: repeatPartition, NodeID: nodeID, Seq: seq, Timestamp: at, Body: body}
}

func repeatRole(t *testing.T, nodeID string, seq uint64, at time.Time, roles ...string) lad.Record {
	t.Helper()
	body, err := json.Marshal(lad.RoleRecord{Partition: repeatPartition, NodeID: nodeID, Roles: roles, Updated: at})
	if err != nil {
		t.Fatal(err)
	}
	return lad.Record{Topic: lad.TopicRole, Partition: repeatPartition, NodeID: nodeID, Seq: seq, Timestamp: at, Body: body}
}

func applyAll(t *testing.T, c *DirectoryCache, recs ...lad.Record) {
	t.Helper()
	for _, rec := range recs {
		if err := c.Apply(rec); err != nil {
			t.Fatalf("Apply(%s seq %d tombstone %v): %v", rec.Topic, rec.Seq, rec.Tombstone, err)
		}
	}
}

func hasMember(c *DirectoryCache, nodeID string) bool {
	c.inMemMu.RLock()
	defer c.inMemMu.RUnlock()
	_, ok := c.store.GetMember(repeatPartition, nodeID)
	return ok
}

// A member tombstone applied twice removes the member once and announces the
// membership change once.
func TestApplyingAMemberTombstoneTwiceAnnouncesOneChange(t *testing.T) {
	c := NewDirectoryCache()
	changes := 0
	c.OnMembershipChange = func() { changes++ }
	t0 := time.Now().Add(-time.Minute)

	applyAll(t, c, repeatMember(t, "n1", 1, t0))
	tomb := repeatTombstone(lad.TopicMember, "n1", 2, t0.Add(time.Second))
	applyAll(t, c, tomb, tomb)

	if hasMember(c, "n1") {
		t.Fatal("the tombstoned member is still in the cache")
	}
	if changes != 2 {
		t.Fatalf("membership changes = %d, want 2 (the add, then one removal)", changes)
	}
}

// A tombstone applied again after a record without a sequence number was
// accepted beside it (the record check keeps such a tombstone) deletes
// nothing: the repeat is the tombstone already held.
func TestATombstoneRepeatedBesideAnUnsequencedRecordDeletesNothing(t *testing.T) {
	c := NewDirectoryCache()
	t0 := time.Now().Add(-time.Minute)
	tomb := repeatTombstone(lad.TopicMember, "n1", 1, t0)
	unsequenced := repeatMember(t, "n1", 0, t0.Add(time.Second))
	applyAll(t, c, tomb, unsequenced)
	if !hasMember(c, "n1") {
		t.Fatal("the record newer than the tombstone was not accepted")
	}
	applyAll(t, c, tomb)
	if !hasMember(c, "n1") {
		t.Fatal("the repeated tombstone deleted the record accepted beside it")
	}
}

// A tombstone for a member the cache does not hold changes no membership.
func TestATombstoneForAnAbsentMemberAnnouncesNoChange(t *testing.T) {
	c := NewDirectoryCache()
	changes := 0
	c.OnMembershipChange = func() { changes++ }
	applyAll(t, c, repeatTombstone(lad.TopicMember, "n1", 1, time.Now()))
	if changes != 0 {
		t.Fatalf("membership changes = %d, want 0", changes)
	}
}

// A member re-registers after its tombstone; the tombstone arriving again
// (a stream restarted from its first watermark replays it) leaves the
// re-registered member in place.
func TestATombstoneRepeatedAfterTheMemberReregisteredDeletesNothing(t *testing.T) {
	c := NewDirectoryCache()
	changes := 0
	c.OnMembershipChange = func() { changes++ }
	t0 := time.Now().Add(-time.Minute)
	first := repeatMember(t, "n1", 1, t0)
	tomb := repeatTombstone(lad.TopicMember, "n1", 2, t0.Add(time.Second))
	again := repeatMember(t, "n1", 3, t0.Add(2*time.Second))

	applyAll(t, c, first, tomb, again)
	if !hasMember(c, "n1") {
		t.Fatal("the re-registered member is missing before the repeat")
	}
	before := changes

	applyAll(t, c, first, tomb, again)
	if !hasMember(c, "n1") {
		t.Fatal("replaying the stream deleted the re-registered member")
	}
	if changes != before {
		t.Fatalf("replaying the stream announced %d membership changes, want 0", changes-before)
	}
}

// A tombstone no newer than a live record loses whichever arrives first,
// so the record and the tombstone in either order leave the member.
func TestALiveRecordBeatsAnOlderTombstoneInEitherOrder(t *testing.T) {
	t0 := time.Now().Add(-time.Minute)
	rec := repeatMember(t, "n1", 2, t0.Add(2*time.Second))
	tomb := repeatTombstone(lad.TopicMember, "n1", 1, t0.Add(time.Second))

	recordFirst := NewDirectoryCache()
	applyAll(t, recordFirst, rec, tomb)
	tombFirst := NewDirectoryCache()
	applyAll(t, tombFirst, tomb, rec)

	if !hasMember(tombFirst, "n1") {
		t.Fatal("tombstone then record: the member is missing")
	}
	if !hasMember(recordFirst, "n1") {
		t.Fatal("record then older tombstone: the tombstone deleted the newer member")
	}
	if _, ok := recordFirst.IdentityView("n1", string(lad.TopicMember)); !ok {
		t.Fatal("record then older tombstone: the refused tombstone dropped the live member's projection entry")
	}
}

// An exact repeat of the newest record changes nothing and reaches no topic
// subscriber a second time.
func TestAnExactRepeatOfTheNewestRecordIsNotAnnouncedAgain(t *testing.T) {
	c := NewDirectoryCache()
	seen := 0
	c.Subscribe(lad.TopicMember, func(lad.Record) { seen++ })
	rec := repeatMember(t, "n1", 1, time.Now())
	applyAll(t, c, rec, rec)
	if seen != 1 {
		t.Fatalf("subscriber saw the record %d times, want 1", seen)
	}
}

// A repeat of an evicted record re-adds it: the entry it was the newest
// record for is gone, so it is not a repeat of anything held.
func TestARecordWhoseEntryWasEvictedIsAppliedAgain(t *testing.T) {
	c := NewDirectoryCache()
	rec := repeatMember(t, "n1", 1, time.Now())
	applyAll(t, c, rec)
	c.inMemMu.Lock()
	c.store.DeleteMember(repeatPartition, "n1")
	c.inMemMu.Unlock()

	applyAll(t, c, rec)
	if !hasMember(c, "n1") {
		t.Fatal("the record of an evicted entry was skipped as a repeat")
	}
}

// An older reach record arriving after a newer one leaves the newer record
// and the newer delta base.
func TestAnOlderReachRecordLeavesTheNewerDeltaBase(t *testing.T) {
	c := NewDirectoryCache()
	t0 := time.Now()
	older := repeatReach(t, "n1", 3, t0, "10.0.0.3")
	newer := repeatReach(t, "n1", 5, t0.Add(time.Second), "10.0.0.5")
	applyAll(t, c, older, newer, older)

	if got := string(c.GetLastReachBody(repeatPartition, "n1")); got != string(newer.Body) {
		t.Fatalf("delta base after the older record's repeat = %s, want the newer body %s", got, newer.Body)
	}
}

// An older role record arriving after a newer one leaves the newer roles.
func TestAnOlderRoleRecordLeavesTheNewerRoles(t *testing.T) {
	c := NewDirectoryCache()
	t0 := time.Now()
	older := repeatRole(t, "n1", 1, t0, "old")
	newer := repeatRole(t, "n1", 2, t0.Add(time.Second), "new")
	applyAll(t, c, older, newer, older)

	c.inMemMu.RLock()
	role, ok := c.store.GetRole(repeatPartition, "n1")
	c.inMemMu.RUnlock()
	if !ok || len(role.Roles) != 1 || role.Roles[0] != "new" {
		t.Fatalf("roles after the older record's repeat = %v (held %v), want [new]", role.Roles, ok)
	}
}

// The sweep drops the liveAt mark of an entry that is gone, so liveAt holds
// no more marks than the cache holds entries.
func TestTheSweepDropsTheMarkOfAnEvictedEntry(t *testing.T) {
	c := NewDirectoryCache()
	applyAll(t, c, repeatMember(t, "n1", 1, time.Now()), repeatMember(t, "n2", 1, time.Now()))
	c.inMemMu.Lock()
	c.store.DeleteMember(repeatPartition, "n1")
	c.inMemMu.Unlock()

	c.EvictExpired()

	c.inMemMu.RLock()
	defer c.inMemMu.RUnlock()
	if _, ok := c.liveAt[tombstoneKey(lad.TopicMember, repeatPartition, "n1")]; ok {
		t.Fatal("the evicted member's mark survived the sweep")
	}
	if _, ok := c.liveAt[tombstoneKey(lad.TopicMember, repeatPartition, "n2")]; !ok {
		t.Fatal("the sweep dropped the mark of a live member")
	}
}
