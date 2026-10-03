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
// use its own locking — DirectoryCache's c.inMemMu is authoritative.
type CacheStore interface {
	// Members
	GetMember(partition, nodeID string) (lad.MemberRecord, bool)
	PutMember(partition string, member lad.MemberRecord)
	DeleteMember(partition, nodeID string)
	ListMembers(partition string) []lad.MemberRecord
	AllMembers() map[string][]lad.MemberRecord // partition -> members
	CountMembers(partition string) int

	// Roles
	GetRole(partition, nodeID string) (lad.RoleRecord, bool)
	PutRole(partition string, role lad.RoleRecord)
	DeleteRole(partition, nodeID string)
	ListRoles(partition string) []lad.RoleRecord
	AllRoles() map[string][]lad.RoleRecord

	// Reach
	GetReach(partition, nodeID string) (lad.ReachRecord, bool)
	PutReach(partition string, reach lad.ReachRecord)
	DeleteReach(partition, nodeID string)
	ListReach(partition string) []lad.ReachRecord
	AllReach() map[string][]lad.ReachRecord
	CountReach(partition string) int

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
