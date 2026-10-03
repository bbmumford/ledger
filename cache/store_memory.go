/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package cache

import (
	"time"

	lad "github.com/bbmumford/ledger"
)

// MemoryCacheStore implements CacheStore using in-memory maps.
// It does NOT use its own locking — DirectoryCache's c.inMemMu is authoritative.
type MemoryCacheStore struct {
	// Exported for DirectoryCache backward-compatibility with tests that
	// access the maps directly via the DirectoryCache struct fields.
	Members      map[string]map[string]lad.MemberRecord
	Roles        map[string]map[string]lad.RoleRecord
	Reach        map[string]map[string]lad.ReachRecord
	Latency      map[string]lad.LatencyRecord
	LastGossipAt map[string]time.Time
	Tombstones   map[string]lad.Record
}

// NewMemoryCacheStore creates a MemoryCacheStore with initialized maps.
func NewMemoryCacheStore() *MemoryCacheStore {
	return &MemoryCacheStore{
		Members:      make(map[string]map[string]lad.MemberRecord),
		Roles:        make(map[string]map[string]lad.RoleRecord),
		Reach:        make(map[string]map[string]lad.ReachRecord),
		Latency:      make(map[string]lad.LatencyRecord),
		LastGossipAt: make(map[string]time.Time),
		Tombstones:   make(map[string]lad.Record),
	}
}

// --- Members ---

func (s *MemoryCacheStore) GetMember(partition, nodeID string) (lad.MemberRecord, bool) {
	tm, ok := s.Members[partition]
	if !ok {
		return lad.MemberRecord{}, false
	}
	m, ok := tm[nodeID]
	return m, ok
}

func (s *MemoryCacheStore) PutMember(partition string, member lad.MemberRecord) {
	if _, ok := s.Members[partition]; !ok {
		s.Members[partition] = make(map[string]lad.MemberRecord)
	}
	s.Members[partition][member.NodeID] = member
}

func (s *MemoryCacheStore) DeleteMember(partition, nodeID string) {
	if tm, ok := s.Members[partition]; ok {
		delete(tm, nodeID)
		if len(tm) == 0 {
			delete(s.Members, partition)
		}
	}
}

func (s *MemoryCacheStore) ListMembers(partition string) []lad.MemberRecord {
	tm := s.Members[partition]
	result := make([]lad.MemberRecord, 0, len(tm))
	for _, m := range tm {
		result = append(result, m)
	}
	return result
}

func (s *MemoryCacheStore) AllMembers() map[string][]lad.MemberRecord {
	out := make(map[string][]lad.MemberRecord, len(s.Members))
	for partition, tm := range s.Members {
		members := make([]lad.MemberRecord, 0, len(tm))
		for _, m := range tm {
			members = append(members, m)
		}
		out[partition] = members
	}
	return out
}

func (s *MemoryCacheStore) CountMembers(partition string) int {
	return len(s.Members[partition])
}

// --- Roles ---

func (s *MemoryCacheStore) GetRole(partition, nodeID string) (lad.RoleRecord, bool) {
	tm, ok := s.Roles[partition]
	if !ok {
		return lad.RoleRecord{}, false
	}
	r, ok := tm[nodeID]
	return r, ok
}

func (s *MemoryCacheStore) PutRole(partition string, role lad.RoleRecord) {
	if _, ok := s.Roles[partition]; !ok {
		s.Roles[partition] = make(map[string]lad.RoleRecord)
	}
	s.Roles[partition][role.NodeID] = role
}

func (s *MemoryCacheStore) DeleteRole(partition, nodeID string) {
	if tm, ok := s.Roles[partition]; ok {
		delete(tm, nodeID)
		if len(tm) == 0 {
			delete(s.Roles, partition)
		}
	}
}

func (s *MemoryCacheStore) ListRoles(partition string) []lad.RoleRecord {
	tm := s.Roles[partition]
	result := make([]lad.RoleRecord, 0, len(tm))
	for _, r := range tm {
		result = append(result, r)
	}
	return result
}

func (s *MemoryCacheStore) AllRoles() map[string][]lad.RoleRecord {
	out := make(map[string][]lad.RoleRecord, len(s.Roles))
	for partition, tm := range s.Roles {
		roles := make([]lad.RoleRecord, 0, len(tm))
		for _, r := range tm {
			roles = append(roles, r)
		}
		out[partition] = roles
	}
	return out
}

// --- Reach ---

func (s *MemoryCacheStore) GetReach(partition, nodeID string) (lad.ReachRecord, bool) {
	tm, ok := s.Reach[partition]
	if !ok {
		return lad.ReachRecord{}, false
	}
	r, ok := tm[nodeID]
	return r, ok
}

func (s *MemoryCacheStore) PutReach(partition string, reach lad.ReachRecord) {
	if _, ok := s.Reach[partition]; !ok {
		s.Reach[partition] = make(map[string]lad.ReachRecord)
	}
	s.Reach[partition][reach.NodeID] = reach
}

func (s *MemoryCacheStore) DeleteReach(partition, nodeID string) {
	if tm, ok := s.Reach[partition]; ok {
		delete(tm, nodeID)
		if len(tm) == 0 {
			delete(s.Reach, partition)
		}
	}
}

func (s *MemoryCacheStore) ListReach(partition string) []lad.ReachRecord {
	tm := s.Reach[partition]
	result := make([]lad.ReachRecord, 0, len(tm))
	for _, r := range tm {
		result = append(result, r)
	}
	return result
}

func (s *MemoryCacheStore) AllReach() map[string][]lad.ReachRecord {
	out := make(map[string][]lad.ReachRecord, len(s.Reach))
	for partition, tm := range s.Reach {
		records := make([]lad.ReachRecord, 0, len(tm))
		for _, r := range tm {
			records = append(records, r)
		}
		out[partition] = records
	}
	return out
}

func (s *MemoryCacheStore) CountReach(partition string) int {
	return len(s.Reach[partition])
}

// --- Latency ---

func (s *MemoryCacheStore) GetLatency(key string) (lad.LatencyRecord, bool) {
	lat, ok := s.Latency[key]
	return lat, ok
}

func (s *MemoryCacheStore) PutLatency(key string, lat lad.LatencyRecord) {
	s.Latency[key] = lat
}

func (s *MemoryCacheStore) DeleteLatency(key string) {
	delete(s.Latency, key)
}

func (s *MemoryCacheStore) AllLatency() map[string]lad.LatencyRecord {
	out := make(map[string]lad.LatencyRecord, len(s.Latency))
	for k, v := range s.Latency {
		out[k] = v
	}
	return out
}

func (s *MemoryCacheStore) CountLatency() int {
	return len(s.Latency)
}

// --- Tombstones ---

func (s *MemoryCacheStore) GetTombstone(key string) (lad.Record, bool) {
	rec, ok := s.Tombstones[key]
	return rec, ok
}

func (s *MemoryCacheStore) PutTombstone(key string, rec lad.Record) {
	s.Tombstones[key] = rec
}

func (s *MemoryCacheStore) DeleteTombstone(key string) {
	delete(s.Tombstones, key)
}

func (s *MemoryCacheStore) AllTombstones() map[string]lad.Record {
	out := make(map[string]lad.Record, len(s.Tombstones))
	for k, v := range s.Tombstones {
		out[k] = v
	}
	return out
}

func (s *MemoryCacheStore) CountTombstones() int {
	return len(s.Tombstones)
}

// --- Gossip liveness ---

func (s *MemoryCacheStore) GetGossipSeen(nodeID string) (time.Time, bool) {
	t, ok := s.LastGossipAt[nodeID]
	return t, ok
}

func (s *MemoryCacheStore) PutGossipSeen(nodeID string, t time.Time) {
	s.LastGossipAt[nodeID] = t
}

func (s *MemoryCacheStore) DeleteGossipSeen(nodeID string) {
	delete(s.LastGossipAt, nodeID)
}

func (s *MemoryCacheStore) AllGossipSeen() map[string]time.Time {
	out := make(map[string]time.Time, len(s.LastGossipAt))
	for k, v := range s.LastGossipAt {
		out[k] = v
	}
	return out
}
