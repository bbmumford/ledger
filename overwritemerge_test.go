// Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
// Queries: licensing@hstles.com

package ledger

import (
	"testing"
	"time"
)

// OverwriteMerge is the merge function registered for ALL SIX live LAD topics
// (loom node/runtime.go registers Member · Role · Reach · Latency · KeyOps ·
// Quorum, every one with Key: NodeIDKey). It is therefore the rule that
// decides which version of a node's record the mesh keeps.
//
// #M-614 / #R-1594: it used to key on LamportClock FIRST. That clock never
// merges (nothing calls LamportClock.Merge — the estate's merging clock is
// swarm.HLC.Observe), is held in memory, and RESETS TO 0 ON RESTART.
//
// The four cases below are the ones the rule has to get right; the first is
// the defect, and per #R-1594 ⑤ it is required to have failed against the old
// implementation before passing against the new one.

func mergeRec(lc, hlc uint64, ts time.Time) Record {
	return Record{NodeID: "node-a", LamportClock: lc, HLCTimestamp: hlc, Timestamp: ts}
}

// 🔴 THE DEFECT. A node restarts, its in-memory Lamport counter returns to 0,
// and its fresh record now carries a LOWER clock than its own pre-restart
// record still held by peers. Keyed on LamportClock, the stale record wins —
// and on topics without expiry it wins forever.
func TestRestartedNodesFreshRecordWinsOverItsOwnPreRestartRecord(t *testing.T) {
	before := time.Now().Add(-time.Hour)
	stale := mergeRec(5000, 0, before)             // published before the restart
	fresh := mergeRec(1, 0, before.Add(time.Hour)) // after: clock reset, later wall time

	if !fresh.Timestamp.After(stale.Timestamp) {
		t.Fatal("premise wrong: the fresh record is not actually later")
	}
	if fresh.LamportClock >= stale.LamportClock {
		t.Fatal("premise wrong: the restart did not lower the Lamport clock, " +
			"so this fixture does not reproduce the defect")
	}

	got, err := OverwriteMerge(stale, fresh)
	if err != nil {
		t.Fatalf("OverwriteMerge returned an error: %v", err)
	}
	if got.Timestamp != fresh.Timestamp {
		t.Fatal("the STALE pre-restart record was kept. A restarted node cannot " +
			"update its own Member/Role records in any peer that still holds the " +
			"old one — silently, until its tick count climbs back past the old " +
			"value, and on topics without expiry that is forever")
	}
}

// Where both records carry an HLC — the bridged path — order by it. The HLC
// merges on ingest (swarm.HLC.Observe), so it orders correctly across nodes
// and across restarts, which is exactly what LamportClock could not do.
func TestBothHLCsPresentOrdersByHLC(t *testing.T) {
	now := time.Now()
	// Timestamps deliberately INVERTED against the HLCs, so a result that
	// matches HLC order cannot have come from the timestamp comparison.
	older := mergeRec(0, 100, now)
	newer := mergeRec(0, 200, now.Add(-time.Hour))

	got, err := OverwriteMerge(older, newer)
	if err != nil {
		t.Fatalf("OverwriteMerge returned an error: %v", err)
	}
	if got.HLCTimestamp != 200 {
		t.Fatalf("kept HLC %d, want 200 — with both HLCs present the merge is "+
			"not ordering by the clock that actually merges", got.HLCTimestamp)
	}

	// And the reverse direction, so this is not passing on argument order.
	if got, _ := OverwriteMerge(newer, older); got.HLCTimestamp != 200 {
		t.Fatalf("kept HLC %d with the arguments reversed, want 200",
			got.HLCTimestamp)
	}
}

// 🪤 THE MIXED CASE (#M-614 ④). A node's own records carry no HLC; the same
// record arriving through loom's lad_reach_bridge carries one lifted from the
// swarm envelope. So a single NodeIDKey can see both shapes.
//
// "Highest HLC wins" unguarded would let ANY HLC-bearing copy beat a local
// HLC=0 record REGARDLESS OF AGE — trading the restart bug for an echo bug.
// When either side lacks an HLC, the comparison must fall to Timestamp.
func TestMixedHLCPresenceFallsBackToTimestampRatherThanFavouringTheHLC(t *testing.T) {
	now := time.Now()
	withHLC := mergeRec(0, 999_999, now.Add(-time.Hour)) // has an HLC, but OLD
	noHLC := mergeRec(0, 0, now)                         // no HLC, but NEWER

	got, err := OverwriteMerge(withHLC, noHLC)
	if err != nil {
		t.Fatalf("OverwriteMerge returned an error: %v", err)
	}
	if !got.Timestamp.Equal(noHLC.Timestamp) {
		t.Fatal("the older HLC-bearing record beat a newer record that simply " +
			"had no HLC — an echoed copy of a record now outranks the node's " +
			"own fresher one purely because it travelled through the bridge")
	}
}

// An exact tie keeps the EXISTING record. This is load-bearing rather than
// arbitrary: duplicate delivery is routine under gossip, and a tie that
// preferred the incoming copy would make every re-delivery a "change".
func TestExactTieKeepsTheExistingRecord(t *testing.T) {
	now := time.Now()
	existing := mergeRec(7, 42, now)
	incoming := mergeRec(7, 42, now)
	incoming.Body = []byte("duplicate delivery")

	got, err := OverwriteMerge(existing, incoming)
	if err != nil {
		t.Fatalf("OverwriteMerge returned an error: %v", err)
	}
	if len(got.Body) != 0 {
		t.Fatal("an exactly-tied incoming record replaced the existing one — " +
			"every duplicate gossip delivery now counts as a change")
	}
}
