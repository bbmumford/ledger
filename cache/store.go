/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package cache

import (
	"time"

	lad "github.com/bbmumford/ledger"
)

// CacheStore abstracts the storage backend for DirectoryCache.
// Implementations must be safe for concurrent use only if they will be used
// without the DirectoryCache mutex. The default MemoryCacheStore does NOT
// use its own locking — DirectoryCache's c.mu is authoritative.
type CacheStore interface {
	// Members
	GetMember(tenant, nodeID string) (lad.MemberRecord, bool)
	PutMember(tenant string, member lad.MemberRecord)
	DeleteMember(tenant, nodeID string)
	ListMembers(tenant string) []lad.MemberRecord
	AllMembers() map[string][]lad.MemberRecord // tenant -> members
	CountMembers(tenant string) int

	// Roles
	GetRole(tenant, nodeID string) (lad.RoleRecord, bool)
	PutRole(tenant string, role lad.RoleRecord)
	DeleteRole(tenant, nodeID string)
	ListRoles(tenant string) []lad.RoleRecord
	AllRoles() map[string][]lad.RoleRecord

	// Reach
	GetReach(tenant, nodeID string) (lad.ReachRecord, bool)
	PutReach(tenant string, reach lad.ReachRecord)
	DeleteReach(tenant, nodeID string)
	ListReach(tenant string) []lad.ReachRecord
	AllReach() map[string][]lad.ReachRecord
	CountReach(tenant string) int

	// Latency
	GetLatency(key string) (lad.LatencyRecord, bool)
	PutLatency(key string, lat lad.LatencyRecord)
	DeleteLatency(key string)
	AllLatency() map[string]lad.LatencyRecord
	CountLatency() int

	// Tombstones
	GetTombstone(key string) (lad.Record, bool)
	PutTombstone(key string, rec lad.Record)
	DeleteTombstone(key string)
	AllTombstones() map[string]lad.Record
	CountTombstones() int

	// Gossip liveness
	GetGossipSeen(nodeID string) (time.Time, bool)
	PutGossipSeen(nodeID string, t time.Time)
	DeleteGossipSeen(nodeID string)
	AllGossipSeen() map[string]time.Time
}
