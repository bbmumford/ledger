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
// It does NOT use its own locking — DirectoryCache's c.mu is authoritative.
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

func (s *MemoryCacheStore) GetMember(tenant, nodeID string) (lad.MemberRecord, bool) {
	tm, ok := s.Members[tenant]
	if !ok {
		return lad.MemberRecord{}, false
	}
	m, ok := tm[nodeID]
	return m, ok
}

func (s *MemoryCacheStore) PutMember(tenant string, member lad.MemberRecord) {
	if _, ok := s.Members[tenant]; !ok {
		s.Members[tenant] = make(map[string]lad.MemberRecord)
	}
	s.Members[tenant][member.NodeID] = member
}

func (s *MemoryCacheStore) DeleteMember(tenant, nodeID string) {
	if tm, ok := s.Members[tenant]; ok {
		delete(tm, nodeID)
		if len(tm) == 0 {
			delete(s.Members, tenant)
		}
	}
}

func (s *MemoryCacheStore) ListMembers(tenant string) []lad.MemberRecord {
	tm := s.Members[tenant]
	result := make([]lad.MemberRecord, 0, len(tm))
	for _, m := range tm {
		result = append(result, m)
	}
	return result
}

func (s *MemoryCacheStore) AllMembers() map[string][]lad.MemberRecord {
	out := make(map[string][]lad.MemberRecord, len(s.Members))
	for tenant, tm := range s.Members {
		members := make([]lad.MemberRecord, 0, len(tm))
		for _, m := range tm {
			members = append(members, m)
		}
		out[tenant] = members
	}
	return out
}

func (s *MemoryCacheStore) CountMembers(tenant string) int {
	return len(s.Members[tenant])
}

// --- Roles ---

func (s *MemoryCacheStore) GetRole(tenant, nodeID string) (lad.RoleRecord, bool) {
	tm, ok := s.Roles[tenant]
	if !ok {
		return lad.RoleRecord{}, false
	}
	r, ok := tm[nodeID]
	return r, ok
}

func (s *MemoryCacheStore) PutRole(tenant string, role lad.RoleRecord) {
	if _, ok := s.Roles[tenant]; !ok {
		s.Roles[tenant] = make(map[string]lad.RoleRecord)
	}
	s.Roles[tenant][role.NodeID] = role
}

func (s *MemoryCacheStore) DeleteRole(tenant, nodeID string) {
	if tm, ok := s.Roles[tenant]; ok {
		delete(tm, nodeID)
		if len(tm) == 0 {
			delete(s.Roles, tenant)
		}
	}
}

func (s *MemoryCacheStore) ListRoles(tenant string) []lad.RoleRecord {
	tm := s.Roles[tenant]
	result := make([]lad.RoleRecord, 0, len(tm))
	for _, r := range tm {
		result = append(result, r)
	}
	return result
}

func (s *MemoryCacheStore) AllRoles() map[string][]lad.RoleRecord {
	out := make(map[string][]lad.RoleRecord, len(s.Roles))
	for tenant, tm := range s.Roles {
		roles := make([]lad.RoleRecord, 0, len(tm))
		for _, r := range tm {
			roles = append(roles, r)
		}
		out[tenant] = roles
	}
	return out
}

// --- Reach ---

func (s *MemoryCacheStore) GetReach(tenant, nodeID string) (lad.ReachRecord, bool) {
	tm, ok := s.Reach[tenant]
	if !ok {
		return lad.ReachRecord{}, false
	}
	r, ok := tm[nodeID]
	return r, ok
}

func (s *MemoryCacheStore) PutReach(tenant string, reach lad.ReachRecord) {
	if _, ok := s.Reach[tenant]; !ok {
		s.Reach[tenant] = make(map[string]lad.ReachRecord)
	}
	s.Reach[tenant][reach.NodeID] = reach
}

func (s *MemoryCacheStore) DeleteReach(tenant, nodeID string) {
	if tm, ok := s.Reach[tenant]; ok {
		delete(tm, nodeID)
		if len(tm) == 0 {
			delete(s.Reach, tenant)
		}
	}
}

func (s *MemoryCacheStore) ListReach(tenant string) []lad.ReachRecord {
	tm := s.Reach[tenant]
	result := make([]lad.ReachRecord, 0, len(tm))
	for _, r := range tm {
		result = append(result, r)
	}
	return result
}

func (s *MemoryCacheStore) AllReach() map[string][]lad.ReachRecord {
	out := make(map[string][]lad.ReachRecord, len(s.Reach))
	for tenant, tm := range s.Reach {
		records := make([]lad.ReachRecord, 0, len(tm))
		for _, r := range tm {
			records = append(records, r)
		}
		out[tenant] = records
	}
	return out
}

func (s *MemoryCacheStore) CountReach(tenant string) int {
	return len(s.Reach[tenant])
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
