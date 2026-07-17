/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package cache

import (
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// TestEvictExpired_CascadesRolesForDeadNode is the regression guard for
// immortal ghost nodes in the mesh topology.
//
// THE BUG: EvictExpired sweeps members (gossip liveness + cap), reach
// (ExpiresAt + liveness + cap) and latency (TTL + cap) — but never roles.
// The only code path that deletes a RoleRecord is EvictNode, which a node
// calls on ITSELF, for its OWN NodeID, from Runtime.Shutdown(), with reason
// "liveness-local" so the tombstone is deliberately NOT gossiped.
//
// So no node ever removes another node's role records. A node that dies
// abruptly — which is every `fly deploy`, since the machine is replaced —
// leaves its RoleRecord in every peer's cache permanently. Its member and
// reach records expire correctly on schedule; only the role survives.
//
// Measured on the live fleet against ~11 real machines: members=8 (swept
// clean), reach=8 (swept clean), roles=127 (never swept). The mesh-topology
// machines[] view is built from the role table, so it reported 40 nodes —
// every node that had ever booted — each rendering as "healthy/grade A"
// because that view echoes the record as written and nothing re-evaluates it.
//
// Dead entries are not inert: peers dial them, noise-UDP hangs to msg2
// timeout and WebSocket dials get 401/404 and fall back to TLS.
//
// The fix mirrors the cascade this function already performs for latency via
// evictLatencyForNode: a role record without a live member is orphaned by
// definition, so it dies with the member.
func TestEvictExpired_CascadesRolesForDeadNode(t *testing.T) {
	const (
		tenant = "t1"
		ghost  = "vl1_ghostnode"
		self   = "vl1_selfnode"
	)

	c := NewDirectoryCache()
	c.SetLocalNodeID(self)

	// A node last seen well beyond GossipLivenessTimeout (16m) — i.e. the
	// member sweep will liveness-evict it on this very pass.
	dead := time.Now().Add(-2 * GossipLivenessTimeout)
	c.store.PutMember(tenant, lad.MemberRecord{
		TenantID:  tenant,
		NodeID:    ghost,
		CreatedAt: dead,
	})
	c.store.PutRole(tenant, lad.RoleRecord{
		TenantID: tenant,
		NodeID:   ghost,
		Roles:    []string{"relay"},
		Updated:  dead,
	})

	c.EvictExpired()

	// Baseline: the member sweep works today. If this fails the test is not
	// exercising what it claims and the role assertion below proves nothing.
	if _, ok := c.store.GetMember(tenant, ghost); ok {
		t.Fatal("precondition: member was not liveness-evicted — test is not exercising the sweep")
	}

	if _, ok := c.store.GetRole(tenant, ghost); ok {
		t.Fatal("role record SURVIVED liveness eviction of its member — orphaned role " +
			"keeps a dead node in mesh-topology machines[] forever; peers keep dialling it " +
			"(noise-UDP msg2 timeout, WS 401/404 -> TLS fallback)")
	}
}

// TestEvictExpired_OnLivenessEvict_FiresForDeadNodeOutsideLock is the guard for
// the hook that finally lets a dead node be COLLECTIVELY forgotten.
//
// The sweep's verdict was trapped in-process: emitTombstone downgrades reason
// "liveness" to "liveness-local" so it is never gossiped (a node that blips
// must not be cascade-evicted fleet-wide). The price of that safety is that
// every peer evicts on its own independent clock, and any peer whose clock has
// not fired re-gossips the corpse straight back — the record ping-pongs
// forever. Measured on the live fleet: 11 real machines, 40 identities, ghosts
// surviving 15+ hours.
//
// The hook exports the observation without exporting authority. HSTLES turns it
// into a swarm observer attestation that only becomes a propagating death once
// K distinct anchors independently agree.
//
// The callback MUST fire outside inMemMu: it publishes to a transport whose
// delivery path can re-enter this cache. This test would deadlock, not fail, if
// that regressed — which is exactly why it re-enters the cache on purpose.
func TestEvictExpired_OnLivenessEvict_FiresForDeadNodeOutsideLock(t *testing.T) {
	const (
		tenant = "t1"
		ghost  = "vl1_ghostnode"
		self   = "vl1_selfnode"
	)

	c := NewDirectoryCache()
	c.SetLocalNodeID(self)

	var got []string
	c.SetOnLivenessEvict(func(nodeID string) {
		// Re-enter the cache from inside the callback. If the sweep ever
		// fires this under inMemMu again, this call blocks forever and the
		// test hangs — a louder signal than a failed assertion.
		_ = c.CacheStats()
		got = append(got, nodeID)
	})

	dead := time.Now().Add(-2 * GossipLivenessTimeout)
	c.store.PutMember(tenant, lad.MemberRecord{TenantID: tenant, NodeID: ghost, CreatedAt: dead})

	c.EvictExpired()

	if len(got) != 1 || got[0] != ghost {
		t.Fatalf("liveness hook did not report the dead node: got %v, want [%s] — "+
			"without it the verdict never leaves this process and the corpse is "+
			"re-gossiped back by any peer whose own 16-min clock has not fired",
			got, ghost)
	}
}

// TestEvictExpired_OnLivenessEvict_SilentForLiveNode: the hook must never fire
// for a node that is alive. Each call becomes an attestation that a peer is
// dead; attesting against a live node is the cascade the quorum exists to stop.
func TestEvictExpired_OnLivenessEvict_SilentForLiveNode(t *testing.T) {
	const (
		tenant = "t1"
		live   = "vl1_livenode"
		self   = "vl1_selfnode"
	)

	c := NewDirectoryCache()
	c.SetLocalNodeID(self)

	var fired int
	c.SetOnLivenessEvict(func(string) { fired++ })

	now := time.Now()
	c.store.PutMember(tenant, lad.MemberRecord{TenantID: tenant, NodeID: live, CreatedAt: now})
	c.store.PutGossipSeen(live, now)

	c.EvictExpired()

	if fired != 0 {
		t.Fatalf("liveness hook fired %d times for a LIVE node — each call is an "+
			"attestation that the peer is dead", fired)
	}
}

// TestEvictExpired_OnLivenessEvict_UnsetIsSafe: the hook is optional. An
// unset callback must not panic — every existing consumer leaves it nil.
func TestEvictExpired_OnLivenessEvict_UnsetIsSafe(t *testing.T) {
	c := NewDirectoryCache()
	c.SetLocalNodeID("vl1_selfnode")
	c.store.PutMember("t1", lad.MemberRecord{
		TenantID:  "t1",
		NodeID:    "vl1_ghost",
		CreatedAt: time.Now().Add(-2 * GossipLivenessTimeout),
	})
	c.EvictExpired() // must not panic
}

// TestEvictExpired_ReapsAlreadyOrphanedRole covers the fleet's ACTUAL state.
//
// The cascade fires the instant a member is liveness-evicted, so it cannot
// reach a role whose member is already gone — and by the time roles were given
// any expiry path at all, every node that had ever died had left exactly that.
// Live fleet: 127 roles, 8 members, ~11 real machines. Those 127 were
// unreachable by any code path in the process.
func TestEvictExpired_ReapsAlreadyOrphanedRole(t *testing.T) {
	const (
		tenant = "t1"
		orphan = "vl1_orphannode"
		self   = "vl1_selfnode"
	)

	c := NewDirectoryCache()
	c.SetLocalNodeID(self)

	// A role with NO member record and no refresh inside the liveness window:
	// its source node died long ago.
	c.store.PutRole(tenant, lad.RoleRecord{
		TenantID: tenant,
		NodeID:   orphan,
		Roles:    []string{"relay"},
		Updated:  time.Now().Add(-2 * GossipLivenessTimeout),
	})

	c.EvictExpired()

	if _, ok := c.store.GetRole(tenant, orphan); ok {
		t.Fatal("pre-existing orphaned role survived — no code path can ever remove it, " +
			"so it stays in mesh-topology machines[] as a healthy grade-A ghost forever")
	}
}

// TestEvictExpired_KeepsFreshOrphanRole guards the bootstrap race. The reach
// bridge pairs a role onto an inbound swarm fleet.peer record; a role can
// legitimately land before its member. Reaping on "no member" alone would evict
// a node that is still arriving, so a freshly-updated role must survive.
func TestEvictExpired_KeepsFreshOrphanRole(t *testing.T) {
	const (
		tenant   = "t1"
		arriving = "vl1_arriving"
		self     = "vl1_selfnode"
	)

	c := NewDirectoryCache()
	c.SetLocalNodeID(self)

	c.store.PutRole(tenant, lad.RoleRecord{
		TenantID: tenant,
		NodeID:   arriving,
		Roles:    []string{"relay"},
		Updated:  time.Now(), // just arrived; member still in flight
	})

	c.EvictExpired()

	if _, ok := c.store.GetRole(tenant, arriving); !ok {
		t.Fatal("freshly-published role evicted before its member arrived — " +
			"strips a live node of its advertised capabilities, worse than the orphan")
	}
}

// TestEvictExpired_KeepsSelfRole: a node must never reap its own role, for the
// same reason it never evicts its own member — it would erase its own
// capabilities from its own directory while perfectly healthy.
func TestEvictExpired_KeepsSelfRole(t *testing.T) {
	const (
		tenant = "t1"
		self   = "vl1_selfnode"
	)

	c := NewDirectoryCache()
	c.SetLocalNodeID(self)

	// Stale AND memberless — reaped for any other node.
	c.store.PutRole(tenant, lad.RoleRecord{
		TenantID: tenant,
		NodeID:   self,
		Roles:    []string{"anchor"},
		Updated:  time.Now().Add(-2 * GossipLivenessTimeout),
	})

	c.EvictExpired()

	if _, ok := c.store.GetRole(tenant, self); !ok {
		t.Fatal("node reaped its OWN role record")
	}
}

// TestEvictExpired_KeepsRolesForLiveNode guards the inverse: the cascade must
// key off the member sweep, never run on its own. A node that is alive keeps
// its roles — over-eviction would silently strip a live node of its
// capabilities and is strictly worse than the ghost it fixes.
func TestEvictExpired_KeepsRolesForLiveNode(t *testing.T) {
	const (
		tenant = "t1"
		live   = "vl1_livenode"
		self   = "vl1_selfnode"
	)

	c := NewDirectoryCache()
	c.SetLocalNodeID(self)

	now := time.Now()
	c.store.PutMember(tenant, lad.MemberRecord{
		TenantID:  tenant,
		NodeID:    live,
		CreatedAt: now,
	})
	c.store.PutRole(tenant, lad.RoleRecord{
		TenantID: tenant,
		NodeID:   live,
		Roles:    []string{"relay"},
		Updated:  now,
	})
	c.store.PutGossipSeen(live, now)

	c.EvictExpired()

	if _, ok := c.store.GetMember(tenant, live); !ok {
		t.Fatal("live member was evicted")
	}
	if _, ok := c.store.GetRole(tenant, live); !ok {
		t.Fatal("live node's role was evicted — cascade must fire only with the member sweep")
	}
}
