/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	lad "github.com/bbmumford/ledger"
)

// ReachQuery narrows down reachability lookups.
type ReachQuery struct {
	NodeID       string  // Filter by specific node ID
	Role         string  // Filter by role
	Region       string  // Filter by exact region (e.g., "iad")
	PreferRegion string  // Prefer nodes in this region (rank higher)
	MinHealth    float64 // Minimum availability score (0.0-1.0)
	MaxLoad      float64 // Maximum load factor (0.0-1.0)
	Limit        int     // Maximum results
}

// RoleQuery filters role views.
type RoleQuery struct {
	NodeID  string
	Role    string
	Handler string // Filter by handler name in RoleRecord.Handlers
}

// DefaultEvictionInterval is the default period between TTL eviction sweeps.
const DefaultEvictionInterval = 30 * time.Second

// GossipLivenessTimeout is how long a node can be silent in gossip before
// its member and reach records are evicted. Latency records use their own TTL.
const GossipLivenessTimeout = 16 * time.Minute

// maxLatencyRecords caps the total number of latency records in the cache.
// When exceeded, the oldest records (by MeasuredAt) are evicted first.
// 250 supports 11 nodes × 10 peers × 2 transports = 220 records with headroom.
// Revisit when QUIC transport is enabled (3 transports = 330 records).
const maxLatencyRecords = 250

// maxRecordsPerTenant caps the number of member and reach records per tenant map.
// When exceeded, the oldest records are evicted first.
const maxRecordsPerTenant = 200

// tombstoneTTL is how long tombstone records are retained for gossip propagation.
const tombstoneTTL = 30 * time.Minute

// CacheStatsReport holds observability metrics about the DirectoryCache.
type CacheStatsReport struct {
	MemberCount         int
	RoleCount           int
	ReachCount          int
	LatencyCount        int
	TombstoneCount      int
	TotalEstimatedBytes int
}

// DirectoryCache maintains a materialised view of ledger data for quick lookups.
type DirectoryCache struct {
	mu    sync.RWMutex
	store CacheStore

	// localNodeID identifies the record owner that this cache runs on.
	// When set (via SetLocalNodeID), EvictExpired skips removal of
	// records whose NodeID matches — so a live node can never evict
	// its OWN member/role/reach entries and emit a self-referential
	// tombstone that later suppresses its own re-announcement. Empty
	// string disables the exemption (backward compatible for tests
	// and consumers that don't set it). Set-once at startup, then
	// read lock-free under c.mu's existing read paths in eviction.
	localNodeID string

	// Legacy map fields — kept for backward compatibility with tests that access
	// them directly. When using the default MemoryCacheStore, these point to the
	// store's internal maps. For custom CacheStore implementations these are nil.
	members      map[string]map[string]lad.MemberRecord
	roles        map[string]map[string]lad.RoleRecord
	reach        map[string]map[string]lad.ReachRecord
	latency      map[string]lad.LatencyRecord
	lastGossipAt map[string]time.Time
	tombstones   map[string]lad.Record

	lamportClock uint64 // Phase G3: local Lamport timestamp, incremented on every operation

	// Eviction lifecycle
	evictStop chan struct{}
	evictDone chan struct{}

	// OnNewRecord is called (non-blocking) when Apply() stores a genuinely new record
	// (not an update to existing). Used by rumor-mongering to trigger immediate push.
	// Called AFTER the cache lock is released to prevent deadlock.
	OnNewRecord func(rec lad.Record)

	// OnMembershipChange is called when member records are added or removed.
	// Used by the hypercube overlay to trigger rebuild.
	OnMembershipChange func()

	// Agnostic redesign: per-topic merge and key functions.
	// When nil, defaults are used (OverwriteMerge, NodeIDKey).
	mergeFuncs map[lad.Topic]lad.MergeFunc
	keyFuncs   map[lad.Topic]lad.KeyFunc
	topicCfgs  map[lad.Topic]lad.TopicConfig
	metrics    lad.MetricsRecorder
	subscribers map[lad.Topic][]func(lad.Record)

	// ACL, indexing, compaction, and conflict logging.
	conflictLog *lad.ConflictLog
	aclFuncs    map[lad.Topic]lad.ACLFunc
	indexFuncs  map[lad.Topic]map[string]lad.IndexFunc  // topic -> indexName -> func
	indexes     map[lad.Topic]map[string]map[string][]lad.Record // topic -> indexName -> key -> records
	compaction  map[lad.Topic]lad.CompactionPolicy

	// reachDeltaApplier reconstructs a full ReachRecord body from the
	// previous full-snapshot body + a delta body. Consumers (the reach
	// package) register one via SetReachDeltaApplier; without it,
	// reach-layer deltas are skipped and the cache waits for the next
	// full snapshot to refresh. See ReachDeltaApplier docs.
	reachDeltaApplier ReachDeltaApplier
	// lastReachBody keeps the most recent full-snapshot body per
	// (tenant, nodeID) so the delta applier has a base to rebuild from.
	// Updated on every full-snapshot apply and on every successful delta
	// reconstruction. Parallel to store.Reach, not a replacement.
	lastReachBody map[string]map[string][]byte

	// identityViews is the per-(NodeID, Topic) projection used by the
	// reconciliation driver as the canonical IBLT input. Each entry
	// reflects the most recent record's content hash + HLC + last
	// update time, so two nodes computing IBLTs over their respective
	// projections see identical cells when their caches are in sync —
	// even if the topic-keyed merge merged a Reach delta into a base
	// body that was originally a different record's content.
	//
	// Cache-only — never serialised to the wire. Refreshed on every
	// successful Apply, including merges. Tombstones remove the entry.
	//
	// Map shape: identityViews[nodeID][topic] → IdentityView.
	identityViews map[string]map[string]lad.IdentityView
}

// ReachDeltaApplier reconstructs a full ReachRecord body from the previous
// full-snapshot body and a reach-layer delta body. The returned body MUST
// be a valid full ReachRecord JSON (SchemaVersion without the delta flag),
// with the delta's ops applied to the base's address set, the delta's
// UpdatedAt/HLC, and the base's Metadata/Region/Signature preserved as
// appropriate. Implementations typically live in the reach package so they
// can manipulate the full reach.Address shape; the cache treats bodies as
// opaque.
//
// When nil (the default), the cache skips delta bodies entirely and lets
// the next full-snapshot publish refresh the cache state.
type ReachDeltaApplier func(baseBody, deltaBody []byte) (newBody []byte, err error)

// SetReachDeltaApplier installs (or clears) the delta applier. Safe to call
// at any time; the next delta apply uses the new applier.
func (c *DirectoryCache) SetReachDeltaApplier(a ReachDeltaApplier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reachDeltaApplier = a
}

// RegisterMerge sets the merge function for a topic.
func (c *DirectoryCache) RegisterMerge(topic lad.Topic, fn lad.MergeFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mergeFuncs == nil {
		c.mergeFuncs = make(map[lad.Topic]lad.MergeFunc)
	}
	c.mergeFuncs[topic] = fn
}

// RegisterKey sets the key derivation function for a topic.
func (c *DirectoryCache) RegisterKey(topic lad.Topic, fn lad.KeyFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keyFuncs == nil {
		c.keyFuncs = make(map[lad.Topic]lad.KeyFunc)
	}
	c.keyFuncs[topic] = fn
}

// RegisterTopic registers a full topic configuration.
func (c *DirectoryCache) RegisterTopic(topic lad.Topic, cfg lad.TopicConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.topicCfgs == nil {
		c.topicCfgs = make(map[lad.Topic]lad.TopicConfig)
	}
	c.topicCfgs[topic] = cfg
	if cfg.Merge != nil {
		if c.mergeFuncs == nil {
			c.mergeFuncs = make(map[lad.Topic]lad.MergeFunc)
		}
		c.mergeFuncs[topic] = cfg.Merge
	}
	if cfg.Key != nil {
		if c.keyFuncs == nil {
			c.keyFuncs = make(map[lad.Topic]lad.KeyFunc)
		}
		c.keyFuncs[topic] = cfg.Key
	}
}

// SetMetrics sets the metrics recorder for observability.
func (c *DirectoryCache) SetMetrics(m lad.MetricsRecorder) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = m
}

// SetLocalNodeID identifies which NodeID is "self" so eviction never
// removes the local node's own records. Empty string disables the
// exemption. Intended for set-once use at startup; safe to call at
// any time but eviction that has already run with a different value
// can't be undone.
//
// Without this guard, a node whose self-announcement has lapsed past
// GossipLivenessTimeout can evict its OWN member/role entries and emit
// a tombstone that propagates, suppressing later re-announcements and
// turning the node invisible to the rest of the mesh (the exact
// symptom behind 94-tombstones-vs-2-members).
func (c *DirectoryCache) SetLocalNodeID(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.localNodeID = nodeID
}

// LocalNodeID returns the currently-registered local node identifier
// (empty if unset).
func (c *DirectoryCache) LocalNodeID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.localNodeID
}

// Subscribe registers a callback for record changes on a topic.
func (c *DirectoryCache) Subscribe(topic lad.Topic, handler func(lad.Record)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subscribers == nil {
		c.subscribers = make(map[lad.Topic][]func(lad.Record))
	}
	c.subscribers[topic] = append(c.subscribers[topic], handler)
}

// mergeFunc returns the merge function for a topic, defaulting to OverwriteMerge.
func (c *DirectoryCache) mergeFunc(topic lad.Topic) lad.MergeFunc {
	if c.mergeFuncs != nil {
		if fn, ok := c.mergeFuncs[topic]; ok {
			return fn
		}
	}
	return lad.OverwriteMerge
}

// keyFunc returns the key derivation function for a topic, defaulting to NodeIDKey.
func (c *DirectoryCache) keyFunc(topic lad.Topic) lad.KeyFunc {
	if c.keyFuncs != nil {
		if fn, ok := c.keyFuncs[topic]; ok {
			return fn
		}
	}
	return lad.NodeIDKey
}

// RegisterACL sets the ACL check function for a topic.
// The function is called in Apply() before merge; return an error to reject the record.
func (c *DirectoryCache) RegisterACL(topic lad.Topic, fn lad.ACLFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aclFuncs == nil {
		c.aclFuncs = make(map[lad.Topic]lad.ACLFunc)
	}
	c.aclFuncs[topic] = fn
}

// RegisterIndex adds a secondary index for a topic. The IndexFunc is called on
// every Apply to derive zero or more index keys that map back to the record.
func (c *DirectoryCache) RegisterIndex(topic lad.Topic, name string, fn lad.IndexFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.indexFuncs == nil {
		c.indexFuncs = make(map[lad.Topic]map[string]lad.IndexFunc)
	}
	if c.indexFuncs[topic] == nil {
		c.indexFuncs[topic] = make(map[string]lad.IndexFunc)
	}
	c.indexFuncs[topic][name] = fn
	// Initialize the index storage.
	if c.indexes == nil {
		c.indexes = make(map[lad.Topic]map[string]map[string][]lad.Record)
	}
	if c.indexes[topic] == nil {
		c.indexes[topic] = make(map[string]map[string][]lad.Record)
	}
	if c.indexes[topic][name] == nil {
		c.indexes[topic][name] = make(map[string][]lad.Record)
	}
}

// QueryByIndex returns all records matching a secondary index key.
func (c *DirectoryCache) QueryByIndex(topic lad.Topic, indexName string, key string) ([]lad.Record, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.indexes == nil {
		return nil, errors.New("ledger: no indexes registered")
	}
	topicIdx, ok := c.indexes[topic]
	if !ok {
		return nil, fmt.Errorf("ledger: no indexes for topic %q", topic)
	}
	idx, ok := topicIdx[indexName]
	if !ok {
		return nil, fmt.Errorf("ledger: index %q not found for topic %q", indexName, topic)
	}
	records := idx[key]
	// Return a copy to avoid caller mutation.
	out := make([]lad.Record, len(records))
	copy(out, records)
	return out, nil
}

// RegisterCompaction sets the compaction policy for a topic.
func (c *DirectoryCache) RegisterCompaction(topic lad.Topic, policy lad.CompactionPolicy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.compaction == nil {
		c.compaction = make(map[lad.Topic]lad.CompactionPolicy)
	}
	c.compaction[topic] = policy
}

// Compact removes tombstones older than MaxTombstoneAge for the given topic.
// Returns the number of records removed.
func (c *DirectoryCache) Compact(topic lad.Topic) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	policy, ok := c.compaction[topic]
	if !ok {
		return 0, nil
	}
	if policy.MaxTombstoneAge <= 0 {
		return 0, nil
	}

	cutoff := time.Now().Add(-policy.MaxTombstoneAge)
	removed := 0

	// Iterate tombstones and remove those for this topic that are older than cutoff.
	for key, ts := range c.store.AllTombstones() {
		if ts.Topic != topic {
			continue
		}
		if ts.DeletedAt.Before(cutoff) {
			c.store.DeleteTombstone(key)
			removed++
		}
	}

	return removed, nil
}

// CompactAll runs compaction across all topics with registered policies.
// Returns the total number of records removed.
func (c *DirectoryCache) CompactAll() (int, error) {
	c.mu.RLock()
	topics := make([]lad.Topic, 0, len(c.compaction))
	for t := range c.compaction {
		topics = append(topics, t)
	}
	c.mu.RUnlock()

	total := 0
	for _, t := range topics {
		n, err := c.Compact(t)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// SetConflictLogging enables or disables conflict logging with the given capacity.
// When enabled, merge conflicts in Apply() are recorded for later inspection.
func (c *DirectoryCache) SetConflictLogging(enabled bool, maxEntries int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if enabled {
		if c.conflictLog == nil {
			c.conflictLog = lad.NewConflictLog(maxEntries)
		} else {
			c.conflictLog.SetEnabled(true)
		}
	} else {
		if c.conflictLog != nil {
			c.conflictLog.SetEnabled(false)
		}
	}
}

// ConflictEntries returns recent conflict records for a topic.
func (c *DirectoryCache) ConflictEntries(topic lad.Topic, limit int) []lad.ConflictRecord {
	c.mu.RLock()
	cl := c.conflictLog
	c.mu.RUnlock()
	if cl == nil {
		return nil
	}
	return cl.Entries(topic, limit)
}

// BloomFilter returns the bloom filter for a topic, if available.
// Currently returns nil — reserved for future probabilistic membership testing.
func (c *DirectoryCache) BloomFilter(topic lad.Topic) interface{} {
	return nil
}

// HasRecord returns true if a record exists for the given topic and key.
// Key semantics depend on the topic (NodeID for members/roles/reach, composite for latency).
func (c *DirectoryCache) HasRecord(topic lad.Topic, key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch topic {
	case lad.TopicMember:
		// key is treated as "tenant:nodeID"
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			return false
		}
		_, ok := c.store.GetMember(parts[0], parts[1])
		return ok
	case lad.TopicRole:
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			return false
		}
		_, ok := c.store.GetRole(parts[0], parts[1])
		return ok
	case lad.TopicReach:
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			return false
		}
		_, ok := c.store.GetReach(parts[0], parts[1])
		return ok
	case lad.TopicLatency:
		_, ok := c.store.GetLatency(key)
		return ok
	default:
		return false
	}
}

// NewDirectoryCache constructs an empty cache. An optional CacheStore can be
// provided to override the default in-memory storage backend.
func NewDirectoryCache(stores ...CacheStore) *DirectoryCache {
	var store CacheStore
	if len(stores) > 0 && stores[0] != nil {
		store = stores[0]
	} else {
		store = NewMemoryCacheStore()
	}

	dc := &DirectoryCache{
		store:         store,
		identityViews: make(map[string]map[string]lad.IdentityView),
	}

	// Wire up legacy map fields for backward compatibility when using MemoryCacheStore.
	if ms, ok := store.(*MemoryCacheStore); ok {
		dc.members = ms.Members
		dc.roles = ms.Roles
		dc.reach = ms.Reach
		dc.latency = ms.Latency
		dc.lastGossipAt = ms.LastGossipAt
		dc.tombstones = ms.Tombstones
	}

	return dc
}

// Store returns the underlying CacheStore for advanced usage.
func (c *DirectoryCache) Store() CacheStore {
	return c.store
}

// RecordGossipSeen marks a nodeID as alive (seen in gossip exchange).
func (c *DirectoryCache) RecordGossipSeen(nodeID string) {
	if nodeID == "" {
		return
	}
	c.mu.Lock()
	c.store.PutGossipSeen(nodeID, time.Now())
	c.mu.Unlock()
}

// LastGossipSeen returns when a nodeID was last seen in gossip (zero if never).
func (c *DirectoryCache) LastGossipSeen(nodeID string) time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, _ := c.store.GetGossipSeen(nodeID)
	return t
}

// GossipLiveness returns all gossip liveness entries.
func (c *DirectoryCache) GossipLiveness() map[string]time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store.AllGossipSeen()
}

// CacheStats returns observability metrics about the current cache state.
func (c *DirectoryCache) CacheStats() CacheStatsReport {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var stats CacheStatsReport
	allMembers := c.store.AllMembers()
	for _, members := range allMembers {
		stats.MemberCount += len(members)
	}
	allRoles := c.store.AllRoles()
	for _, roles := range allRoles {
		stats.RoleCount += len(roles)
	}
	allReach := c.store.AllReach()
	for _, records := range allReach {
		stats.ReachCount += len(records)
	}
	stats.LatencyCount = c.store.CountLatency()
	stats.TombstoneCount = c.store.CountTombstones()

	// Rough size estimate: ~512 bytes per member/role/reach, ~128 bytes per latency, ~64 bytes per tombstone
	stats.TotalEstimatedBytes = (stats.MemberCount+stats.RoleCount+stats.ReachCount)*512 +
		stats.LatencyCount*128 +
		stats.TombstoneCount*64

	return stats
}

// StartEviction launches a background goroutine that periodically removes
// expired reach records from the cache. Call StopEviction to terminate it.
func (c *DirectoryCache) StartEviction(interval time.Duration) {
	if interval <= 0 {
		interval = DefaultEvictionInterval
	}
	c.evictStop = make(chan struct{})
	c.evictDone = make(chan struct{})
	go c.evictionLoop(interval)
}

// StopEviction signals the eviction goroutine to stop and waits for it to finish.
func (c *DirectoryCache) StopEviction() {
	if c.evictStop == nil {
		return
	}
	close(c.evictStop)
	<-c.evictDone
}

func (c *DirectoryCache) evictionLoop(interval time.Duration) {
	defer close(c.evictDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.evictStop:
			return
		case <-ticker.C:
			c.EvictExpired()
		}
	}
}

// tombstoneKey builds a composite key for tombstone dedup: "topic:tenant:nodeID".
func tombstoneKey(topic lad.Topic, tenantID, nodeID string) string {
	return string(topic) + ":" + tenantID + ":" + nodeID
}

// emitTombstone stores a tombstone record for gossip propagation.
// reason controls propagation behavior:
//   - "explicit" / "cap" — propagated via gossip to all peers (prevents re-propagation)
//   - "liveness" — local-only, NOT propagated (prevents cascade eviction across mesh)
//
// Must be called with c.mu held.
func (c *DirectoryCache) emitTombstone(topic lad.Topic, tenantID, nodeID, reason string) {
	now := time.Now()
	key := tombstoneKey(topic, tenantID, nodeID)
	rec := lad.Record{
		Topic:           topic,
		TenantID:        tenantID,
		NodeID:          nodeID,
		Tombstone:       true,
		DeletedAt:       now,
		Timestamp:       now,
		TombstoneReason: reason,
	}
	if reason == "liveness" {
		// Liveness tombstones are local-only — stored to block gossip from
		// re-inserting the record, but NOT propagated to other nodes (prevents
		// cascade eviction when a node temporarily loses connectivity).
		rec.TombstoneReason = "liveness-local"
	}
	c.store.PutTombstone(key, rec)
}

// EvictNode removes all records for a specific node and emits propagating
// tombstones. Used during graceful shutdown to announce departure.
func (c *DirectoryCache) EvictNode(nodeID string, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find and remove from all tenant maps
	for tenant := range c.store.AllMembers() {
		if _, ok := c.store.GetMember(tenant, nodeID); ok {
			c.store.DeleteMember(tenant, nodeID)
			c.emitTombstone(lad.TopicMember, tenant, nodeID, reason)
		}
		if _, ok := c.store.GetReach(tenant, nodeID); ok {
			c.store.DeleteReach(tenant, nodeID)
			c.emitTombstone(lad.TopicReach, tenant, nodeID, reason)
		}
		if _, ok := c.store.GetRole(tenant, nodeID); ok {
			c.store.DeleteRole(tenant, nodeID)
			c.emitTombstone(lad.TopicRole, tenant, nodeID, reason)
		}
	}
	c.store.DeleteGossipSeen(nodeID)
}

// EvictExpired removes stale records from the cache.
// Member and reach records are evicted by gossip liveness timeout.
// Latency records are evicted by TTL. Tombstones are evicted after tombstoneTTL.
// Caps are enforced on latency (maxLatencyRecords) and per-tenant maps (maxRecordsPerTenant).
func (c *DirectoryCache) EvictExpired() int {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	removedMembers := 0
	removedReach := 0
	removedLatency := 0
	gossipCutoff := now.Add(-GossipLivenessTimeout)

	// --- Evict member records by gossip liveness ---
	//
	// Self-exemption: a node must never evict its OWN member record.
	// Doing so creates a self-referential "liveness" tombstone that
	// propagates via gossip and suppresses the node's own future
	// re-announcements (tombstone wins on causal compare). Before this
	// guard, any node that went 16 minutes without re-publishing
	// (e.g. quiet service hours) would self-tombstone and vanish from
	// its peers' member directories even while its peer connections
	// stayed healthy — the mechanism behind the 94-tombstones-vs-2-
	// members symptom on help.orbtr.io after fleet-wide deploys.
	allMembers := c.store.AllMembers()
	for tenant, members := range allMembers {
		for _, rec := range members {
			if c.localNodeID != "" && rec.NodeID == c.localNodeID {
				continue
			}
			lastSeen, _ := c.store.GetGossipSeen(rec.NodeID)
			if lastSeen.IsZero() {
				lastSeen = rec.CreatedAt
			}
			if lastSeen.Before(gossipCutoff) {
				c.emitTombstone(lad.TopicMember, tenant, rec.NodeID, "liveness")
				c.store.DeleteMember(tenant, rec.NodeID)
				c.store.DeleteGossipSeen(rec.NodeID)
				removedMembers++
				// Proactively evict latency records involving this dead node
				// instead of waiting for their 10-minute TTL to expire.
				removedLatency += c.evictLatencyForNode(rec.NodeID)
			}
		}
	}

	// --- Enforce per-tenant cap on member records ---
	// Self is also exempt from cap eviction for the same reason:
	// a capacity-tombstone on self suppresses future self-announces.
	allMembers = c.store.AllMembers()
	for tenant, members := range allMembers {
		if len(members) > maxRecordsPerTenant {
			type memberEntry struct {
				nodeID    string
				createdAt time.Time
			}
			entries := make([]memberEntry, 0, len(members))
			for _, m := range members {
				if c.localNodeID != "" && m.NodeID == c.localNodeID {
					continue
				}
				entries = append(entries, memberEntry{m.NodeID, m.CreatedAt})
			}
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].createdAt.Before(entries[j].createdAt)
			})
			// Recompute excess against the (possibly smaller) non-self set.
			nonSelf := len(entries)
			overCap := len(members) - maxRecordsPerTenant
			excess := overCap
			if excess > nonSelf {
				excess = nonSelf
			}
			for i := 0; i < excess; i++ {
				c.emitTombstone(lad.TopicMember, tenant, entries[i].nodeID, "cap")
				c.store.DeleteMember(tenant, entries[i].nodeID)
				removedMembers++
			}
		}
	}

	// --- Evict reach records by ExpiresAt or gossip liveness ---
	// Same self-exemption as members: never tombstone the local node's
	// own reach record, or a stale local entry vanishes from its peers
	// and can't be revived by future self-announcements.
	allReach := c.store.AllReach()
	for tenant, records := range allReach {
		for _, rec := range records {
			if c.localNodeID != "" && rec.NodeID == c.localNodeID {
				continue
			}
			if !rec.ExpiresAt.IsZero() {
				if rec.ExpiresAt.Before(now) {
					c.emitTombstone(lad.TopicReach, tenant, rec.NodeID, "liveness")
					c.store.DeleteReach(tenant, rec.NodeID)
					removedReach++
				}
				continue
			}
			lastSeen, _ := c.store.GetGossipSeen(rec.NodeID)
			if lastSeen.IsZero() {
				lastSeen = rec.UpdatedAt
			}
			if lastSeen.Before(gossipCutoff) {
				c.emitTombstone(lad.TopicReach, tenant, rec.NodeID, "liveness")
				c.store.DeleteReach(tenant, rec.NodeID)
				removedReach++
			}
		}
	}

	// --- Enforce per-tenant cap on reach records ---
	allReach = c.store.AllReach()
	for tenant, records := range allReach {
		if len(records) > maxRecordsPerTenant {
			type reachEntry struct {
				nodeID    string
				updatedAt time.Time
			}
			entries := make([]reachEntry, 0, len(records))
			for _, r := range records {
				if c.localNodeID != "" && r.NodeID == c.localNodeID {
					continue
				}
				entries = append(entries, reachEntry{r.NodeID, r.UpdatedAt})
			}
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].updatedAt.Before(entries[j].updatedAt)
			})
			nonSelf := len(entries)
			overCap := len(records) - maxRecordsPerTenant
			excess := overCap
			if excess > nonSelf {
				excess = nonSelf
			}
			for i := 0; i < excess; i++ {
				c.emitTombstone(lad.TopicReach, tenant, entries[i].nodeID, "cap")
				c.store.DeleteReach(tenant, entries[i].nodeID)
				removedReach++
			}
		}
	}

	// --- Evict latency records by TTL ---
	allLatency := c.store.AllLatency()
	for key, lat := range allLatency {
		if !lat.ExpiresAt.IsZero() && lat.ExpiresAt.Before(now) {
			c.emitTombstone(lad.TopicLatency, "", key, "liveness")
			c.store.DeleteLatency(key)
			removedLatency++
		}
	}

	// --- Enforce latency record cap ---
	if c.store.CountLatency() > maxLatencyRecords {
		allLatency = c.store.AllLatency()
		type latEntry struct {
			key        string
			measuredAt time.Time
		}
		entries := make([]latEntry, 0, len(allLatency))
		for key, lat := range allLatency {
			entries = append(entries, latEntry{key, lat.MeasuredAt})
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].measuredAt.Before(entries[j].measuredAt)
		})
		excess := len(allLatency) - maxLatencyRecords
		for i := 0; i < excess; i++ {
			c.emitTombstone(lad.TopicLatency, "", entries[i].key, "cap")
			c.store.DeleteLatency(entries[i].key)
			removedLatency++
		}
	}

	// --- Evict expired tombstones ---
	tombstoneCutoff := now.Add(-tombstoneTTL)
	allTombstones := c.store.AllTombstones()
	for key, ts := range allTombstones {
		if ts.DeletedAt.Before(tombstoneCutoff) {
			c.store.DeleteTombstone(key)
		}
	}

	// --- Phase 1: Observability logging (only when evictions > 0) ---
	removed := removedMembers + removedReach + removedLatency
	if removed > 0 {
		totalMembers := 0
		for _, members := range c.store.AllMembers() {
			totalMembers += len(members)
		}
		totalReach := 0
		for _, records := range c.store.AllReach() {
			totalReach += len(records)
		}
		dbgCache.Printf("Evicted: %d members, %d reach, %d latency (cache: %d members, %d reach, %d latency)",
			removedMembers, removedReach, removedLatency,
			totalMembers, totalReach, c.store.CountLatency())
	}

	return removed
}

// Apply ingests a ledger record into the cache.
func (c *DirectoryCache) Apply(rec lad.Record) error {
	// Phase G3: Update Lamport clock — max(local, received) + 1
	c.mu.Lock()
	if rec.LamportClock > c.lamportClock {
		c.lamportClock = rec.LamportClock
	}
	c.lamportClock++
	c.mu.Unlock()

	// Phase 3: Handle tombstone records — delete matching cache entries
	if rec.Tombstone {
		c.applyTombstone(rec)
		// Drop the projection entry too — peers reconciling on the
		// IBLT need to see the tombstone as a key removal, not as
		// "still present with old content."
		c.updateIdentityView(rec)
		// Fire membership change callback for member tombstones (hypercube rebuild)
		if rec.Topic == lad.TopicMember && c.OnMembershipChange != nil {
			c.OnMembershipChange()
		}
		return nil
	}

	// Phase 3: Check for newer tombstone that would block this record (prevent zombie resurrection).
	// Records with Seq > 0 and Timestamp after (or equal to) tombstone are accepted — this handles
	// rapid restart scenarios where a node shuts down and comes back within the same second.
	// Local-only tombstones ("liveness-local") never block — they're just cache cleanup markers.
	tKey := tombstoneKey(rec.Topic, rec.TenantID, rec.NodeID)
	c.mu.RLock()
	if ts, ok := c.store.GetTombstone(tKey); ok {
		if ts.TombstoneReason == "liveness-local" {
			// Local-only tombstone — always allow new records (node may have restarted)
			c.mu.RUnlock()
		} else if rec.Seq > 0 && !rec.Timestamp.Before(ts.DeletedAt) {
			// Record has a sequence number and is at least as new as the tombstone.
			// This is a live node re-registering after restart — allow it and clear tombstone.
			c.mu.RUnlock()
			c.mu.Lock()
			c.store.DeleteTombstone(tKey)
			c.mu.Unlock()
		} else if ts.DeletedAt.After(rec.Timestamp) {
			c.mu.RUnlock()
			return nil // tombstone is strictly newer — reject stale record
		} else {
			c.mu.RUnlock()
		}
	} else {
		c.mu.RUnlock()
	}

	// ACL check — reject record before merge if topic has an ACL function.
	if c.aclFuncs != nil {
		if aclFn, ok := c.aclFuncs[rec.Topic]; ok {
			if err := aclFn(string(rec.Topic), rec.AuthorPubKey, rec); err != nil {
				return err
			}
		}
	}

	var isNew bool
	var isMemberChange bool
	var wasMerged bool

	switch rec.Topic {
	case lad.TopicMember:
		member, err := lad.UnmarshalMember(rec.Body)
		if err != nil {
			return err
		}
		c.mu.RLock()
		_, exists := c.store.GetMember(member.TenantID, member.NodeID)
		c.mu.RUnlock()
		isNew = !exists
		isMemberChange = isNew
		c.storeMember(member)
	case lad.TopicRole:
		role, err := lad.UnmarshalRole(rec.Body)
		if err != nil {
			return err
		}
		c.mu.RLock()
		_, exists := c.store.GetRole(role.TenantID, role.NodeID)
		c.mu.RUnlock()
		isNew = !exists
		c.storeRole(role)
	case lad.TopicReach:
		peek, err := lad.UnmarshalReach(rec.Body)
		if err != nil {
			return err
		}
		if peek.IsReachDelta() {
			// Reach-layer delta: compact address-change ops. The delta body
			// carries no signed identity payload (Metadata/Region/...), so
			// it can't be stored directly. If a ReachDeltaApplier is
			// registered AND we hold a base full snapshot, rebuild a full
			// record from (base + ops) and store that. Otherwise skip and
			// wait for the next full snapshot to refresh.
			c.mu.RLock()
			applier := c.reachDeltaApplier
			var baseBody []byte
			if tenantMap, ok := c.lastReachBody[peek.TenantID]; ok {
				baseBody = tenantMap[peek.NodeID]
			}
			_, exists := c.store.GetReach(peek.TenantID, peek.NodeID)
			c.mu.RUnlock()
			if applier == nil || baseBody == nil {
				return nil
			}
			newBody, err := applier(baseBody, rec.Body)
			if err != nil {
				return nil // skip on reconstruction error; base remains authoritative
			}
			rebuilt, err := lad.UnmarshalReach(newBody)
			if err != nil {
				return nil
			}
			if rebuilt.IsReachDelta() {
				return nil // applier returned another delta — malformed
			}
			rebuilt.UpdatedAt = rec.Timestamp
			rebuilt.Seq = rec.Seq
			isNew = !exists
			c.storeReach(rebuilt)
			c.setLastReachBody(rebuilt.TenantID, rebuilt.NodeID, newBody)
			break
		}
		// Full snapshot path.
		peek.UpdatedAt = rec.Timestamp
		peek.Seq = rec.Seq
		c.mu.RLock()
		_, exists := c.store.GetReach(peek.TenantID, peek.NodeID)
		c.mu.RUnlock()
		isNew = !exists
		c.storeReach(peek)
		c.setLastReachBody(peek.TenantID, peek.NodeID, rec.Body)
	case lad.TopicLatency:
		lat, err := lad.UnmarshalLatency(rec.Body)
		if err != nil {
			return err
		}
		key := lat.FromNode + "::" + lat.ToNode + "::" + lat.Transport
		c.mu.RLock()
		_, exists := c.store.GetLatency(key)
		c.mu.RUnlock()
		isNew = !exists
		c.storeLatency(lat)
	default:
		// ignore other topics for directory view.
	}

	wasMerged = !isNew

	// Conflict logging — when a merge resolved two different records, log it.
	if c.conflictLog != nil && wasMerged {
		c.conflictLog.Record(lad.ConflictRecord{
			Topic:     rec.Topic,
			Key:       c.keyFunc(rec.Topic)(rec),
			Winner:    rec,
			Loser:     rec, // best-effort: typed store doesn't retain pre-merge snapshot
			Timestamp: time.Now(),
		})
	}

	// Update secondary indexes for this record.
	if c.indexFuncs != nil {
		if topicFuncs, ok := c.indexFuncs[rec.Topic]; ok {
			c.mu.Lock()
			for idxName, idxFn := range topicFuncs {
				keys := idxFn(rec)
				for _, k := range keys {
					if c.indexes[rec.Topic] == nil {
						c.indexes[rec.Topic] = make(map[string]map[string][]lad.Record)
					}
					if c.indexes[rec.Topic][idxName] == nil {
						c.indexes[rec.Topic][idxName] = make(map[string][]lad.Record)
					}
					c.indexes[rec.Topic][idxName][k] = append(c.indexes[rec.Topic][idxName][k], rec)
				}
			}
			c.mu.Unlock()
		}
	}

	// Update the per-(NodeID, Topic) projection so the reconciliation
	// driver's IBLT input stays in lockstep with what the cache
	// considers authoritative. Skipped when the record was rejected
	// upstream (we'd have returned already) or topic is unknown.
	c.updateIdentityView(rec)

	// Fire callbacks AFTER lock is released (storeX methods release their locks)
	if isNew && c.OnNewRecord != nil {
		c.OnNewRecord(rec)
	}
	if isMemberChange && c.OnMembershipChange != nil {
		c.OnMembershipChange()
	}

	// Notify topic subscribers (used by SubscribeWithReplay for live changes).
	c.mu.RLock()
	subs := c.subscribers[rec.Topic]
	c.mu.RUnlock()
	for _, fn := range subs {
		fn(rec)
	}

	// Metrics — record that Apply completed.
	if c.metrics != nil {
		c.metrics.RecordApplied(string(rec.Topic), wasMerged)
	}

	return nil
}

// applyTombstone processes a tombstone record: removes matching data from cache
// and stores the tombstone for re-propagation to peers.
func (c *DirectoryCache) applyTombstone(rec lad.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tKey := tombstoneKey(rec.Topic, rec.TenantID, rec.NodeID)

	// Only store if this tombstone is newer than any existing one for the same key
	if existing, ok := c.store.GetTombstone(tKey); ok {
		if rec.DeletedAt.Before(existing.DeletedAt) {
			return
		}
	}
	c.store.PutTombstone(tKey, rec)

	// Delete matching record from live cache maps
	switch rec.Topic {
	case lad.TopicMember:
		c.store.DeleteMember(rec.TenantID, rec.NodeID)
	case lad.TopicRole:
		c.store.DeleteRole(rec.TenantID, rec.NodeID)
	case lad.TopicReach:
		c.store.DeleteReach(rec.TenantID, rec.NodeID)
	case lad.TopicLatency:
		// For latency tombstones, NodeID holds the latency composite key
		c.store.DeleteLatency(rec.NodeID)
	}
}

// NextLamportClock increments and returns the local Lamport clock.
// Used by publishers to stamp outgoing records before ledger append.
func (c *DirectoryCache) NextLamportClock() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lamportClock++
	return c.lamportClock
}

// CurrentLamportClock returns the current Lamport clock value without incrementing.
func (c *DirectoryCache) CurrentLamportClock() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lamportClock
}

func (c *DirectoryCache) storeMember(member lad.MemberRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Newer-wins with generic attr merging: preserve all non-empty attrs from the richer record.
	if existing, ok := c.store.GetMember(member.TenantID, member.NodeID); ok {
		if member.CreatedAt.Before(existing.CreatedAt) {
			// Incoming is older — merge richer attrs INTO existing
			merged := false
			for key, val := range member.Attrs {
				if existing.Attrs[key] == "" && val != "" {
					if existing.Attrs == nil {
						existing.Attrs = make(map[string]string)
					}
					existing.Attrs[key] = val
					merged = true
				}
			}
			if merged {
				c.store.PutMember(member.TenantID, existing)
			}
			return
		}
		// Incoming is newer — preserve existing attrs that incoming is missing
		if existing.Attrs != nil {
			if member.Attrs == nil {
				member.Attrs = make(map[string]string)
			}
			for key, val := range existing.Attrs {
				if member.Attrs[key] == "" && val != "" {
					member.Attrs[key] = val
				}
			}
		}
	}
	c.store.PutMember(member.TenantID, member)
}

func (c *DirectoryCache) storeRole(role lad.RoleRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store.PutRole(role.TenantID, role)
}

// GetLastReachBody returns a copy of the most recent full-snapshot body
// for (tenant, nodeID), or nil if none is cached. Exposed so the reach
// package's FreshnessClient can compute the cached address-set digest
// and decide whether to ignore or act on an incoming announce.
//
// The returned slice is a private copy; callers may mutate it freely.
func (c *DirectoryCache) GetLastReachBody(tenant, nodeID string) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tm, ok := c.lastReachBody[tenant]
	if !ok {
		return nil
	}
	body, ok := tm[nodeID]
	if !ok {
		return nil
	}
	dup := make([]byte, len(body))
	copy(dup, body)
	return dup
}

// setLastReachBody stores the raw body of the most recent full-snapshot
// ReachRecord for (tenant, nodeID) so delta applies have a base to rebuild
// from. Caller must not hold c.mu.
func (c *DirectoryCache) setLastReachBody(tenant, nodeID string, body []byte) {
	if len(body) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastReachBody == nil {
		c.lastReachBody = make(map[string]map[string][]byte)
	}
	tm, ok := c.lastReachBody[tenant]
	if !ok {
		tm = make(map[string][]byte)
		c.lastReachBody[tenant] = tm
	}
	// Copy so we don't retain a caller-controlled buffer.
	dup := make([]byte, len(body))
	copy(dup, body)
	tm[nodeID] = dup
}

func (c *DirectoryCache) storeReach(reach lad.ReachRecord) {
	if !reach.ExpiresAt.IsZero() && reach.ExpiresAt.Before(time.Now()) {
		return // already expired
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// If a record already exists for this NodeID, only replace it if the
	// incoming record is newer (higher Seq or later UpdatedAt). This
	// prevents stale gossip from overwriting fresher state.
	if existing, ok := c.store.GetReach(reach.TenantID, reach.NodeID); ok {
		if reach.Seq < existing.Seq {
			return
		}
		if reach.Seq == existing.Seq && reach.UpdatedAt.Before(existing.UpdatedAt) {
			return
		}
	}
	c.store.PutReach(reach.TenantID, reach)
}

// Members returns the registered members for a tenant.
//
// The cache unifies two sources into a single view:
//   - Persisted MemberRecord entries (explicit Append of TopicMember)
//   - Derived views synthesised from ReachRecord.Metadata
//
// A ReachRecord whose Metadata map is non-empty contributes a MemberRecord
// view keyed by NodeID: Attrs mirror Metadata verbatim, CreatedAt is the
// reach UpdatedAt, and ExpiresAt is the reach ExpiresAt. This ties the
// synthetic Member's lifetime to the signed Reach record, so TTL eviction
// cannot cause the Member view to decay independently of the reachability
// data that produced it.
//
// Persisted Members shadow derived ones — consumers that explicitly append
// a MemberRecord (e.g. agents storing peer-info) keep full control. Nodes
// that only publish Reach get a Member view for free, with no separate
// publish path and no TTL drift.
func (c *DirectoryCache) Members(ctx context.Context, tenant string) ([]lad.MemberRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	persisted := c.store.ListMembers(tenant)
	seen := make(map[string]struct{}, len(persisted))
	for _, m := range persisted {
		seen[m.NodeID] = struct{}{}
	}
	result := persisted
	for _, reach := range c.store.ListReach(tenant) {
		if _, ok := seen[reach.NodeID]; ok {
			continue
		}
		if len(reach.Metadata) == 0 {
			continue
		}
		attrs := make(map[string]string, len(reach.Metadata))
		for k, v := range reach.Metadata {
			attrs[k] = v
		}
		result = append(result, lad.MemberRecord{
			TenantID:  reach.TenantID,
			NodeID:    reach.NodeID,
			CreatedAt: reach.UpdatedAt,
			ExpiresAt: reach.ExpiresAt,
			Attrs:     attrs,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result, nil
}

// Roles returns role assignments that match the query.
func (c *DirectoryCache) Roles(ctx context.Context, tenant string, query RoleQuery) ([]lad.RoleRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entries := c.store.ListRoles(tenant)
	result := make([]lad.RoleRecord, 0)
	for _, role := range entries {
		if query.NodeID != "" && role.NodeID != query.NodeID {
			continue
		}
		if query.Role != "" && !roleContains(role, query.Role) {
			continue
		}
		if query.Handler != "" && !handlerContains(role, query.Handler) {
			continue
		}
		result = append(result, role)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result, nil
}

// Reach returns reachability advertisements honouring the provided query.
func (c *DirectoryCache) Reach(ctx context.Context, tenant string, query ReachQuery) ([]lad.ReachRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entries := c.store.ListReach(tenant)
	if len(entries) == 0 {
		return nil, nil
	}
	var candidates []lad.ReachRecord
	for _, reach := range entries {
		// Filter by NodeID
		if query.NodeID != "" && reach.NodeID != query.NodeID {
			continue
		}
		// Filter by Role
		if query.Role != "" {
			role, _ := c.store.GetRole(tenant, reach.NodeID)
			if !roleContains(role, query.Role) {
				continue
			}
		}
		// Filter by exact Region
		if query.Region != "" && !strings.EqualFold(reach.Region, query.Region) {
			continue
		}
		// Filter by minimum health (availability)
		if query.MinHealth > 0 && reach.Availability < query.MinHealth {
			continue
		}
		// Filter by maximum load
		if query.MaxLoad > 0 && reach.LoadFactor > query.MaxLoad {
			continue
		}
		candidates = append(candidates, reach)
	}

	// Sort by composite score: region preference, health, load, latency
	sort.Slice(candidates, func(i, j int) bool {
		return reachScore(candidates[i], query.PreferRegion) > reachScore(candidates[j], query.PreferRegion)
	})

	if query.Limit > 0 && len(candidates) > query.Limit {
		candidates = candidates[:query.Limit]
	}
	return candidates, nil
}

// RelayRole is the standard role name for nodes that can relay traffic.
const RelayRole = "relay"

// DiscoverRelays finds available relay nodes for NAT traversal.
// Returns relay nodes sorted by preference: region match, availability, load.
// This is a convenience wrapper around Reach() with Role="relay".
func (c *DirectoryCache) DiscoverRelays(ctx context.Context, tenant, preferRegion string, limit int) ([]lad.ReachRecord, error) {
	if limit <= 0 {
		limit = 5 // Default: return top 5 relays
	}
	return c.Reach(ctx, tenant, ReachQuery{
		Role:         RelayRole,
		PreferRegion: preferRegion,
		MinHealth:    0.5, // Require at least 50% availability for relays
		MaxLoad:      0.9, // Exclude overloaded relays
		Limit:        limit,
	})
}

// reachScore computes a composite ranking score for a ReachRecord.
// Higher score = better candidate.
// Factors: region match (bonus), availability, inverse load, inverse latency
func reachScore(r lad.ReachRecord, preferRegion string) float64 {
	score := 0.0

	// Region preference bonus (+100 for exact match)
	if preferRegion != "" && strings.EqualFold(r.Region, preferRegion) {
		score += 100.0
	}

	// Health score: availability 0.0-1.0 → 0-50 points
	score += r.Availability * 50.0

	// Load score: inverse load 0.0-1.0 → 0-30 points (lower load = higher score)
	score += (1.0 - r.LoadFactor) * 30.0

	// Latency score: inverse latency → 0-20 points (lower latency = higher score)
	// Cap at 1000ms for calculation
	latency := float64(r.LatencyMillis)
	if latency <= 0 {
		latency = 1 // Avoid division by zero
	}
	if latency > 1000 {
		latency = 1000
	}
	score += (1.0 - latency/1000.0) * 20.0

	return score
}

func roleContains(record lad.RoleRecord, role string) bool {
	if role == "" {
		return true
	}
	r := strings.ToLower(role)
	for _, value := range record.Roles {
		if strings.ToLower(value) == r {
			return true
		}
	}
	return false
}

func handlerContains(record lad.RoleRecord, handler string) bool {
	h := strings.ToLower(handler)
	for _, hm := range record.Handlers {
		if strings.ToLower(hm.Name) == h {
			return true
		}
	}
	return false
}

// QueryHandlerMetadata searches for a handler across all nodes with the specified role
// Returns handler metadata including auth requirements and scopes
func (c *DirectoryCache) QueryHandlerMetadata(ctx context.Context, tenant, handlerName string) (*lad.HandlerMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Parse role from handler name (e.g., "auth.login" → "auth")
	parts := strings.Split(handlerName, ".")
	if len(parts) < 2 {
		return nil, nil // Invalid handler name format
	}
	role := parts[0]

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Search role records for nodes with this role
	entries := c.store.ListRoles(tenant)
	for _, roleRecord := range entries {
		if !roleContains(roleRecord, role) {
			continue
		}

		// Search handlers in this node's role record
		for _, handler := range roleRecord.Handlers {
			if handler.Name == handlerName {
				// Found it! Return a copy
				return &lad.HandlerMetadata{
					Name:           handler.Name,
					RequiresAuth:   handler.RequiresAuth,
					RequiredScopes: append([]string{}, handler.RequiredScopes...),
					TimeoutMs:      handler.TimeoutMs,
				}, nil
			}
		}
	}

	// Handler not found in LAD
	return nil, nil
}

// evictLatencyForNode removes all latency records involving the given node
// (as either source or destination). Called when a member is liveness-evicted
// to proactively free memory instead of waiting for the 10-minute TTL.
// Caller must NOT hold c.mu (this method is called from EvictExpired which holds it).
func (c *DirectoryCache) evictLatencyForNode(nodeID string) int {
	removed := 0
	allLatency := c.store.AllLatency()
	for key, lat := range allLatency {
		if lat.FromNode == nodeID || lat.ToNode == nodeID {
			c.store.DeleteLatency(key)
			removed++
		}
	}
	return removed
}

func (c *DirectoryCache) storeLatency(lat lad.LatencyRecord) {
	if !lat.ExpiresAt.IsZero() && lat.ExpiresAt.Before(time.Now()) {
		return // already expired
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Composite key: each transport gets its own latency record per node pair.
	// This prevents TLS and WebSocket from overwriting each other.
	key := lat.FromNode + "::" + lat.ToNode + "::" + lat.Transport
	// Only replace if newer
	if existing, ok := c.store.GetLatency(key); ok {
		if lat.MeasuredAt.Before(existing.MeasuredAt) {
			return
		}
	}
	c.store.PutLatency(key, lat)
}

// Latency returns all non-expired latency records.
// If fromNode is non-empty, only returns measurements from that node.
func (c *DirectoryCache) Latency(fromNode string) []lad.LatencyRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	allLatency := c.store.AllLatency()
	var result []lad.LatencyRecord
	for _, lat := range allLatency {
		if !lat.ExpiresAt.IsZero() && lat.ExpiresAt.Before(now) {
			continue
		}
		if fromNode != "" && lat.FromNode != fromNode && lat.ToNode != fromNode {
			continue
		}
		result = append(result, lat)
	}
	return result
}

// LatencyBetween returns the best (lowest) measured RTT between two nodes.
// Checks all transport variants for this node pair.
func (c *DirectoryCache) LatencyBetween(fromNode, toNode string) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	prefix := fromNode + "::" + toNode + "::"
	now := time.Now()
	var best time.Duration
	allLatency := c.store.AllLatency()
	for key, lat := range allLatency {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			if !lat.ExpiresAt.IsZero() && lat.ExpiresAt.Before(now) {
				continue
			}
			rtt := time.Duration(lat.RTTMs) * time.Millisecond
			if best == 0 || rtt < best {
				best = rtt
			}
		}
	}
	return best
}

// DumpSince returns records with Timestamp strictly after since.
// Used by delta gossip sync to send only changed records.
func (c *DirectoryCache) DumpSince(since time.Time) []lad.Record {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var records []lad.Record
	now := time.Now()

	// Members
	for _, members := range c.store.AllMembers() {
		for _, m := range members {
			if !m.CreatedAt.After(since) {
				continue
			}
			body, _ := json.Marshal(m)
			records = append(records, lad.Record{
				Topic:     lad.TopicMember,
				TenantID:  m.TenantID,
				NodeID:    m.NodeID,
				Body:      body,
				Timestamp: m.CreatedAt,
			})
		}
	}

	// Roles
	for _, roles := range c.store.AllRoles() {
		for _, r := range roles {
			if !r.Updated.After(since) {
				continue
			}
			body, _ := json.Marshal(r)
			records = append(records, lad.Record{
				Topic:     lad.TopicRole,
				TenantID:  r.TenantID,
				NodeID:    r.NodeID,
				Body:      body,
				Timestamp: r.Updated,
			})
		}
	}

	// Reach
	for _, reachRecords := range c.store.AllReach() {
		for _, r := range reachRecords {
			if !r.UpdatedAt.After(since) {
				continue
			}
			body, _ := json.Marshal(r)
			records = append(records, lad.Record{
				Topic:     lad.TopicReach,
				TenantID:  r.TenantID,
				NodeID:    r.NodeID,
				Seq:       r.Seq,
				Body:      body,
				Timestamp: r.UpdatedAt,
			})
		}
	}

	// Latency — skip expired
	// Always include all non-expired latency records in delta sync.
	// Unlike members/reach (append-only), latency records are updated in-place
	// (newer-wins by MeasuredAt). The watermark-based skip would miss updates
	// where MeasuredAt < watermark, causing stale records on remote nodes.
	// Latency records are small (~200 bytes) and capped at 500, so including
	// all non-expired records in every sync is cheap.
	for _, lat := range c.store.AllLatency() {
		if !lat.ExpiresAt.IsZero() && lat.ExpiresAt.Before(now) {
			continue
		}
		body, _ := json.Marshal(lat)
		records = append(records, lad.Record{
			Topic:     lad.TopicLatency,
			NodeID:    lat.FromNode,
			Body:      body,
			Timestamp: lat.MeasuredAt,
		})
	}

	// Tombstones — include live tombstones created after since.
	// Skip local-only tombstones (liveness eviction) — they block local
	// re-insertion but should not propagate to other nodes.
	tombstoneCutoff := now.Add(-tombstoneTTL)
	for _, ts := range c.store.AllTombstones() {
		if ts.DeletedAt.Before(tombstoneCutoff) {
			continue
		}
		if !ts.Timestamp.After(since) {
			continue
		}
		if ts.TombstoneReason == "liveness-local" {
			continue // local-only — don't propagate via gossip
		}
		records = append(records, ts)
	}

	return records
}

// ChangesSince returns the number of records with Timestamp after the given time.
// Used by adaptive gossip interval (Phase E1) to measure cache change rate.
func (c *DirectoryCache) ChangesSince(since time.Time) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	for _, members := range c.store.AllMembers() {
		for _, m := range members {
			if m.CreatedAt.After(since) {
				count++
			}
		}
	}
	for _, roles := range c.store.AllRoles() {
		for _, r := range roles {
			if r.Updated.After(since) {
				count++
			}
		}
	}
	for _, reachRecords := range c.store.AllReach() {
		for _, r := range reachRecords {
			if r.UpdatedAt.After(since) {
				count++
			}
		}
	}
	for _, lat := range c.store.AllLatency() {
		if lat.MeasuredAt.After(since) {
			count++
		}
	}
	return count
}

// Dump returns all records in the cache as a flat list.
func (c *DirectoryCache) Dump() []lad.Record {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Pre-allocate based on estimated counts to avoid repeated slice growth.
	// Use cheap count methods where available; estimate the rest conservatively.
	estCount := c.store.CountLatency() + c.store.CountTombstones() + 50 // +50 for members/roles/reach
	records := make([]lad.Record, 0, estCount)

	// Members
	for _, members := range c.store.AllMembers() {
		for _, m := range members {
			body, _ := json.Marshal(m)
			records = append(records, lad.Record{
				Topic:     lad.TopicMember,
				TenantID:  m.TenantID,
				NodeID:    m.NodeID,
				Body:      body,
				Timestamp: m.CreatedAt,
			})
		}
	}

	// Roles
	for _, roles := range c.store.AllRoles() {
		for _, r := range roles {
			body, _ := json.Marshal(r)
			records = append(records, lad.Record{
				Topic:     lad.TopicRole,
				TenantID:  r.TenantID,
				NodeID:    r.NodeID,
				Body:      body,
				Timestamp: r.Updated,
			})
		}
	}

	// Reach — skip expired
	now := time.Now()
	for _, reachRecords := range c.store.AllReach() {
		for _, r := range reachRecords {
			if !r.ExpiresAt.IsZero() && r.ExpiresAt.Before(now) {
				continue
			}
			body, _ := json.Marshal(r)
			records = append(records, lad.Record{
				Topic:     lad.TopicReach,
				TenantID:  r.TenantID,
				NodeID:    r.NodeID,
				Seq:       r.Seq,
				Body:      body,
				Timestamp: r.UpdatedAt,
			})
		}
	}

	// Latency — skip expired
	for _, lat := range c.store.AllLatency() {
		if !lat.ExpiresAt.IsZero() && lat.ExpiresAt.Before(now) {
			continue
		}
		body, _ := json.Marshal(lat)
		records = append(records, lad.Record{
			Topic:     lad.TopicLatency,
			NodeID:    lat.FromNode,
			Body:      body,
			Timestamp: lat.MeasuredAt,
		})
	}

	// Tombstones — include live tombstones for gossip propagation
	tombstoneCutoff := now.Add(-tombstoneTTL)
	for _, ts := range c.store.AllTombstones() {
		if ts.DeletedAt.Before(tombstoneCutoff) {
			continue
		}
		records = append(records, ts)
	}

	return records
}

// Fingerprint computes a fast, order-independent hash of all cache keys.
// Used for cache reconciliation: two caches with identical keys produce the
// same fingerprint. Different fingerprints indicate divergence.
func (c *DirectoryCache) Fingerprint() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var fp uint64

	for tenant, members := range c.store.AllMembers() {
		for _, m := range members {
			fp ^= hashKey(fmt.Sprintf("member:%s:%s", tenant, m.NodeID))
		}
	}
	for tenant, roles := range c.store.AllRoles() {
		for _, r := range roles {
			fp ^= hashKey(fmt.Sprintf("role:%s:%s", tenant, r.NodeID))
		}
	}
	for tenant, reachRecords := range c.store.AllReach() {
		for _, r := range reachRecords {
			fp ^= hashKey(fmt.Sprintf("reach:%s:%s", tenant, r.NodeID))
		}
	}
	for key := range c.store.AllLatency() {
		fp ^= hashKey("latency:" + key)
	}

	return fp
}

// hashKey returns the FNV-1a 64-bit hash of a string.
func hashKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// ---------------------------------------------------------------------------
// TTL Expiration: PruneExpired
// ---------------------------------------------------------------------------

// PruneExpired scans all records for non-zero ExpiresAt that are past expiry
// and marks them as tombstones. Returns the number of records removed.
// Implements ledger.ExpirySource.
func (c *DirectoryCache) PruneExpired() (int, error) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0

	// Members with ExpiresAt
	for tenant, members := range c.store.AllMembers() {
		for _, m := range members {
			if !m.ExpiresAt.IsZero() && now.After(m.ExpiresAt) {
				c.emitTombstone(lad.TopicMember, tenant, m.NodeID, "ttl")
				c.store.DeleteMember(tenant, m.NodeID)
				if c.metrics != nil {
					c.metrics.RecordExpired(string(lad.TopicMember))
				}
				removed++
			}
		}
	}

	// Reach with ExpiresAt
	for tenant, reachRecords := range c.store.AllReach() {
		for _, r := range reachRecords {
			if !r.ExpiresAt.IsZero() && now.After(r.ExpiresAt) {
				c.emitTombstone(lad.TopicReach, tenant, r.NodeID, "ttl")
				c.store.DeleteReach(tenant, r.NodeID)
				if c.metrics != nil {
					c.metrics.RecordExpired(string(lad.TopicReach))
				}
				removed++
			}
		}
	}

	// Latency with ExpiresAt
	for key, lat := range c.store.AllLatency() {
		if !lat.ExpiresAt.IsZero() && now.After(lat.ExpiresAt) {
			c.store.DeleteLatency(key)
			if c.metrics != nil {
				c.metrics.RecordExpired(string(lad.TopicLatency))
			}
			removed++
		}
	}

	return removed, nil
}

// ---------------------------------------------------------------------------
// Change Feed: ChangesSince / SubscribeWithReplay
// ---------------------------------------------------------------------------

// ChangesSinceHLC returns all records for a topic with timestamp (as UnixNano) > since.
// Records are returned sorted by timestamp ascending for deterministic replay.
func (c *DirectoryCache) ChangesSinceHLC(topic lad.Topic, since uint64) ([]lad.Record, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var records []lad.Record

	switch topic {
	case lad.TopicMember:
		for _, members := range c.store.AllMembers() {
			for _, m := range members {
				body, _ := json.Marshal(m)
				rec := lad.Record{
					Topic:        lad.TopicMember,
					TenantID:     m.TenantID,
					NodeID:       m.NodeID,
					Body:         body,
					Timestamp:    m.CreatedAt,
					LamportClock: c.lamportClock, // best-effort: exact per-record LC not stored on typed records
				}
				// Use timestamp-derived ordering when per-record LC is not available
				if uint64(m.CreatedAt.UnixNano()) > since {
					records = append(records, rec)
				}
			}
		}
	case lad.TopicRole:
		for _, roles := range c.store.AllRoles() {
			for _, r := range roles {
				body, _ := json.Marshal(r)
				rec := lad.Record{
					Topic:     lad.TopicRole,
					TenantID:  r.TenantID,
					NodeID:    r.NodeID,
					Body:      body,
					Timestamp: r.Updated,
				}
				if uint64(r.Updated.UnixNano()) > since {
					records = append(records, rec)
				}
			}
		}
	case lad.TopicReach:
		for _, reachRecords := range c.store.AllReach() {
			for _, r := range reachRecords {
				body, _ := json.Marshal(r)
				rec := lad.Record{
					Topic:     lad.TopicReach,
					TenantID:  r.TenantID,
					NodeID:    r.NodeID,
					Seq:       r.Seq,
					Body:      body,
					Timestamp: r.UpdatedAt,
				}
				if uint64(r.UpdatedAt.UnixNano()) > since {
					records = append(records, rec)
				}
			}
		}
	case lad.TopicLatency:
		for _, lat := range c.store.AllLatency() {
			body, _ := json.Marshal(lat)
			rec := lad.Record{
				Topic:     lad.TopicLatency,
				NodeID:    lat.FromNode,
				Body:      body,
				Timestamp: lat.MeasuredAt,
			}
			if uint64(lat.MeasuredAt.UnixNano()) > since {
				records = append(records, rec)
			}
		}
	}

	// Sort by timestamp ascending for deterministic replay order.
	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp.Before(records[j].Timestamp)
	})

	return records, nil
}

// SubscribeWithReplay delivers historical records first (via ChangesSince),
// then registers a live subscriber for ongoing changes on the topic.
// The handler is called synchronously for historical records and asynchronously
// for live records (from Apply callbacks).
func (c *DirectoryCache) SubscribeWithReplay(topic lad.Topic, since uint64, handler func(lad.Record)) {
	// Deliver historical records first.
	records, _ := c.ChangesSinceHLC(topic, since)
	for _, rec := range records {
		handler(rec)
	}

	// Register live subscriber.
	c.Subscribe(topic, handler)
}

// updateIdentityView refreshes the per-(NodeID, Topic) projection
// entry for a record that just merged successfully. Tombstones drop
// the entry. Records with empty NodeID (e.g. quorum singletons) are
// skipped — the projection is identity-keyed and only meaningful for
// per-node records.
//
// The projection's content hash is the canonical input to the G4
// reconciliation IBLT — by deriving the hash from the merged record
// (not the wire form), two nodes that merged a delta into the same
// base body see the same hash, even though the wire bytes differed.
func (c *DirectoryCache) updateIdentityView(rec lad.Record) {
	if rec.NodeID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.identityViews == nil {
		c.identityViews = make(map[string]map[string]lad.IdentityView)
	}
	topic := string(rec.Topic)
	if rec.Tombstone {
		if topicMap, ok := c.identityViews[rec.NodeID]; ok {
			delete(topicMap, topic)
			if len(topicMap) == 0 {
				delete(c.identityViews, rec.NodeID)
			}
		}
		return
	}
	if _, ok := c.identityViews[rec.NodeID]; !ok {
		c.identityViews[rec.NodeID] = make(map[string]lad.IdentityView)
	}
	c.identityViews[rec.NodeID][topic] = lad.IdentityView{
		NodeID:      rec.NodeID,
		Topic:       topic,
		ContentHash: rec.ContentHash(),
		HLC:         rec.HLCTimestamp,
		UpdatedAt:   time.Now().UnixNano(),
	}
}

// IdentityViews returns a snapshot of every (NodeID, Topic) entry in
// the projection. Used by reconciliation drivers as the canonical
// IBLT input. Order is unstable; callers that need a stable order
// (the IBLT driver, for instance) should sort by content hash.
func (c *DirectoryCache) IdentityViews() []lad.IdentityView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.identityViews) == 0 {
		return nil
	}
	out := make([]lad.IdentityView, 0, len(c.identityViews)*2)
	for _, topicMap := range c.identityViews {
		for _, v := range topicMap {
			out = append(out, v)
		}
	}
	return out
}

// IdentityView returns the projection entry for a specific
// (NodeID, Topic) pair, or zero-value + false if absent.
func (c *DirectoryCache) IdentityView(nodeID, topic string) (lad.IdentityView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if topicMap, ok := c.identityViews[nodeID]; ok {
		v, found := topicMap[topic]
		return v, found
	}
	return lad.IdentityView{}, false
}
