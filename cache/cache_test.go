/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package cache

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

func TestNewDirectoryCache(t *testing.T) {
	cache := NewDirectoryCache()
	if cache == nil {
		t.Fatal("NewDirectoryCache returned nil")
	}
	if cache.members == nil {
		t.Error("members map should be initialized")
	}
	if cache.roles == nil {
		t.Error("roles map should be initialized")
	}
	if cache.reach == nil {
		t.Error("reach map should be initialized")
	}
}

func TestDirectoryCacheApplyMember(t *testing.T) {
	cache := NewDirectoryCache()

	member := lad.MemberRecord{
		TenantID:  "tenant-1",
		NodeID:    "node-1",
		PubKey:    []byte("test-key"),
		CreatedAt: time.Now(),
	}
	body, err := json.Marshal(member)
	if err != nil {
		t.Fatalf("Failed to marshal member: %v", err)
	}

	rec := lad.Record{
		Topic:     lad.TopicMember,
		Body:      body,
		Timestamp: time.Now(),
	}

	err = cache.Apply(rec)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Verify member is stored
	ctx := context.Background()
	members, err := cache.Members(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("Members failed: %v", err)
	}
	if len(members) != 1 {
		t.Errorf("Expected 1 member, got %d", len(members))
	}
	if members[0].NodeID != "node-1" {
		t.Errorf("NodeID mismatch: got %s, want node-1", members[0].NodeID)
	}
}

func TestDirectoryCacheApplyRole(t *testing.T) {
	cache := NewDirectoryCache()

	role := lad.RoleRecord{
		TenantID: "tenant-1",
		NodeID:   "node-1",
		Roles:    []string{"auth", "identity"},
		Updated:  time.Now(),
	}
	body, err := json.Marshal(role)
	if err != nil {
		t.Fatalf("Failed to marshal role: %v", err)
	}

	rec := lad.Record{
		Topic:     lad.TopicRole,
		Body:      body,
		Timestamp: time.Now(),
	}

	err = cache.Apply(rec)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Verify role is stored
	ctx := context.Background()
	roles, err := cache.Roles(ctx, "tenant-1", RoleQuery{})
	if err != nil {
		t.Fatalf("Roles failed: %v", err)
	}
	if len(roles) != 1 {
		t.Errorf("Expected 1 role, got %d", len(roles))
	}
	if len(roles[0].Roles) != 2 {
		t.Errorf("Expected 2 roles, got %d", len(roles[0].Roles))
	}
}

func TestDirectoryCacheApplyReach(t *testing.T) {
	cache := NewDirectoryCache()

	reach := lad.ReachRecord{
		TenantID: "tenant-1",
		NodeID:   "node-1",
		Addresses: []lad.ReachAddress{
			{Host: "192.168.1.1", Port: 8000, Proto: "tcp", Scope: "lan"},
			{Host: "10.0.0.1", Port: 8000, Proto: "tcp", Scope: "lan"},
		},
		Region:       "iad",
		Availability: 0.95,
		LoadFactor:   0.3,
		ExpiresAt:    time.Now().Add(1 * time.Hour),
	}
	body, err := json.Marshal(reach)
	if err != nil {
		t.Fatalf("Failed to marshal reach: %v", err)
	}

	rec := lad.Record{
		Topic:     lad.TopicReach,
		Body:      body,
		Timestamp: time.Now(),
		Seq:       123,
	}

	err = cache.Apply(rec)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Verify reach is stored
	ctx := context.Background()
	reaches, err := cache.Reach(ctx, "tenant-1", ReachQuery{})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(reaches) != 1 {
		t.Errorf("Expected 1 reach, got %d", len(reaches))
	}
	if reaches[0].Region != "iad" {
		t.Errorf("Region mismatch: got %s, want iad", reaches[0].Region)
	}
	if reaches[0].Seq != 123 {
		t.Errorf("Seq mismatch: got %d, want 123", reaches[0].Seq)
	}
}

func TestDirectoryCacheMembersFiltering(t *testing.T) {
	cache := NewDirectoryCache()

	// Add members to different tenants
	tenants := []string{"tenant-1", "tenant-2"}
	for _, tenant := range tenants {
		for i := 0; i < 3; i++ {
			member := lad.MemberRecord{
				TenantID: tenant,
				NodeID:   tenant + "-node-" + string(rune('A'+i)),
				PubKey:   []byte("key"),
			}
			body, _ := json.Marshal(member)
			cache.Apply(lad.Record{Topic: lad.TopicMember, Body: body})
		}
	}

	ctx := context.Background()

	// Query tenant-1
	members, err := cache.Members(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("Members failed: %v", err)
	}
	if len(members) != 3 {
		t.Errorf("Expected 3 members for tenant-1, got %d", len(members))
	}

	// Query tenant-2
	members, err = cache.Members(ctx, "tenant-2")
	if err != nil {
		t.Fatalf("Members failed: %v", err)
	}
	if len(members) != 3 {
		t.Errorf("Expected 3 members for tenant-2, got %d", len(members))
	}

	// Query non-existent tenant
	members, err = cache.Members(ctx, "tenant-3")
	if err != nil {
		t.Fatalf("Members failed: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("Expected 0 members for tenant-3, got %d", len(members))
	}
}

func TestDirectoryCacheRolesFiltering(t *testing.T) {
	cache := NewDirectoryCache()

	// Add roles
	roles := []lad.RoleRecord{
		{TenantID: "tenant-1", NodeID: "node-1", Roles: []string{"auth", "identity"}, Updated: time.Now()},
		{TenantID: "tenant-1", NodeID: "node-2", Roles: []string{"notify"}, Updated: time.Now()},
		{TenantID: "tenant-1", NodeID: "node-3", Roles: []string{"auth", "maintenance"}, Updated: time.Now()},
	}
	for _, role := range roles {
		body, _ := json.Marshal(role)
		cache.Apply(lad.Record{Topic: lad.TopicRole, Body: body})
	}

	ctx := context.Background()

	// Query by NodeID
	result, err := cache.Roles(ctx, "tenant-1", RoleQuery{NodeID: "node-1"})
	if err != nil {
		t.Fatalf("Roles failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Expected 1 role for node-1, got %d", len(result))
	}

	// Query by Role
	result, err = cache.Roles(ctx, "tenant-1", RoleQuery{Role: "auth"})
	if err != nil {
		t.Fatalf("Roles failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 roles with 'auth', got %d", len(result))
	}
}

func TestDirectoryCacheReachFiltering(t *testing.T) {
	cache := NewDirectoryCache()
	expireTime := time.Now().Add(1 * time.Hour)

	// Add reach records
	reaches := []lad.ReachRecord{
		{TenantID: "tenant-1", NodeID: "node-1", Region: "iad", Availability: 0.95, LoadFactor: 0.3, ExpiresAt: expireTime},
		{TenantID: "tenant-1", NodeID: "node-2", Region: "lhr", Availability: 0.80, LoadFactor: 0.6, ExpiresAt: expireTime},
		{TenantID: "tenant-1", NodeID: "node-3", Region: "iad", Availability: 0.50, LoadFactor: 0.9, ExpiresAt: expireTime},
	}
	for _, reach := range reaches {
		body, _ := json.Marshal(reach)
		cache.Apply(lad.Record{Topic: lad.TopicReach, Body: body})
	}

	ctx := context.Background()

	// Query by NodeID
	result, err := cache.Reach(ctx, "tenant-1", ReachQuery{NodeID: "node-1"})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Expected 1 reach for node-1, got %d", len(result))
	}

	// Query by Region
	result, err = cache.Reach(ctx, "tenant-1", ReachQuery{Region: "iad"})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 reaches in iad, got %d", len(result))
	}

	// Query with MinHealth (Availability)
	result, err = cache.Reach(ctx, "tenant-1", ReachQuery{MinHealth: 0.7})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 reaches with health >= 0.7, got %d", len(result))
	}

	// Query with MaxLoad
	result, err = cache.Reach(ctx, "tenant-1", ReachQuery{MaxLoad: 0.5})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Expected 1 reach with load <= 0.5, got %d", len(result))
	}

	// Query with Limit
	result, err = cache.Reach(ctx, "tenant-1", ReachQuery{Limit: 2})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 reaches with limit=2, got %d", len(result))
	}
}

func TestDirectoryCacheContextCancellation(t *testing.T) {
	cache := NewDirectoryCache()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// All operations should fail with canceled context
	_, err := cache.Members(ctx, "tenant-1")
	if err == nil {
		t.Error("Members should fail with canceled context")
	}

	_, err = cache.Roles(ctx, "tenant-1", RoleQuery{})
	if err == nil {
		t.Error("Roles should fail with canceled context")
	}

	_, err = cache.Reach(ctx, "tenant-1", ReachQuery{})
	if err == nil {
		t.Error("Reach should fail with canceled context")
	}
}

func TestDirectoryCacheConcurrency(t *testing.T) {
	cache := NewDirectoryCache()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			member := lad.MemberRecord{
				TenantID: "tenant-1",
				NodeID:   string(rune('A' + id%26)),
			}
			body, _ := json.Marshal(member)
			cache.Apply(lad.Record{Topic: lad.TopicMember, Body: body})
		}(i)
	}

	// Concurrent reads
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.Members(ctx, "tenant-1")
			cache.Roles(ctx, "tenant-1", RoleQuery{})
			cache.Reach(ctx, "tenant-1", ReachQuery{})
		}()
	}

	wg.Wait()
}

func TestDirectoryCacheApplyUnknownTopic(t *testing.T) {
	cache := NewDirectoryCache()

	rec := lad.Record{
		Topic:     "unknown-topic",
		Body:      []byte("{}"),
		Timestamp: time.Now(),
	}

	// Should not error on unknown topic
	err := cache.Apply(rec)
	if err != nil {
		t.Errorf("Apply should not error on unknown topic: %v", err)
	}
}

func TestDirectoryCacheExpiredReach(t *testing.T) {
	cache := NewDirectoryCache()

	// Add an expired reach record
	reach := lad.ReachRecord{
		TenantID:     "tenant-1",
		NodeID:       "node-1",
		Region:       "iad",
		Availability: 0.95,
		ExpiresAt:    time.Now().Add(-1 * time.Hour), // Already expired
	}
	body, _ := json.Marshal(reach)
	cache.Apply(lad.Record{Topic: lad.TopicReach, Body: body})

	ctx := context.Background()
	result, err := cache.Reach(ctx, "tenant-1", ReachQuery{})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Expected 0 reaches (expired), got %d", len(result))
	}
}

func TestDirectoryCacheReachRegionPreference(t *testing.T) {
	cache := NewDirectoryCache()
	expireTime := time.Now().Add(1 * time.Hour)

	// Add reach records in different regions with same availability
	reaches := []lad.ReachRecord{
		{TenantID: "tenant-1", NodeID: "node-1", Region: "lhr", Availability: 0.95, LoadFactor: 0.3, ExpiresAt: expireTime},
		{TenantID: "tenant-1", NodeID: "node-2", Region: "iad", Availability: 0.95, LoadFactor: 0.3, ExpiresAt: expireTime},
		{TenantID: "tenant-1", NodeID: "node-3", Region: "syd", Availability: 0.95, LoadFactor: 0.3, ExpiresAt: expireTime},
	}
	for _, reach := range reaches {
		body, _ := json.Marshal(reach)
		cache.Apply(lad.Record{Topic: lad.TopicReach, Body: body})
	}

	ctx := context.Background()

	// Query with region preference
	result, err := cache.Reach(ctx, "tenant-1", ReachQuery{PreferRegion: "iad"})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("Expected 3 reaches, got %d", len(result))
	}
	// With PreferRegion, iad should be ranked first
	if result[0].Region != "iad" {
		t.Errorf("Expected iad to be first with region preference, got %s", result[0].Region)
	}
}

func TestEvictExpired(t *testing.T) {
	cache := NewDirectoryCache()

	// Manually insert reach records: one expired, one valid.
	// We bypass storeReach (which now rejects expired) by writing directly.
	cache.mu.Lock()
	cache.reach["tenant-1"] = map[string]lad.ReachRecord{
		"node-old": {
			TenantID:  "tenant-1",
			NodeID:    "node-old",
			Region:    "iad",
			ExpiresAt: time.Now().Add(-10 * time.Minute), // expired
		},
		"node-live": {
			TenantID:  "tenant-1",
			NodeID:    "node-live",
			Region:    "iad",
			ExpiresAt: time.Now().Add(1 * time.Hour), // still valid
		},
	}
	cache.mu.Unlock()

	removed := cache.EvictExpired()
	if removed != 1 {
		t.Errorf("Expected 1 eviction, got %d", removed)
	}

	ctx := context.Background()
	result, err := cache.Reach(ctx, "tenant-1", ReachQuery{})
	if err != nil {
		t.Fatalf("Reach failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Expected 1 remaining reach record, got %d", len(result))
	}
	if result[0].NodeID != "node-live" {
		t.Errorf("Expected node-live to survive eviction, got %s", result[0].NodeID)
	}
}

func TestEvictExpiredCleansEmptyTenantMaps(t *testing.T) {
	cache := NewDirectoryCache()

	// Insert a single expired record for a tenant.
	cache.mu.Lock()
	cache.reach["tenant-gone"] = map[string]lad.ReachRecord{
		"node-1": {
			TenantID:  "tenant-gone",
			NodeID:    "node-1",
			ExpiresAt: time.Now().Add(-1 * time.Minute),
		},
	}
	cache.mu.Unlock()

	cache.EvictExpired()

	cache.mu.RLock()
	_, exists := cache.reach["tenant-gone"]
	cache.mu.RUnlock()
	if exists {
		t.Error("Expected empty tenant map to be removed after eviction")
	}
}

func TestStoreReachRejectsExpired(t *testing.T) {
	cache := NewDirectoryCache()

	reach := lad.ReachRecord{
		TenantID:  "tenant-1",
		NodeID:    "node-1",
		ExpiresAt: time.Now().Add(-5 * time.Minute),
	}
	body, _ := json.Marshal(reach)
	cache.Apply(lad.Record{Topic: lad.TopicReach, Body: body})

	cache.mu.RLock()
	count := len(cache.reach["tenant-1"])
	cache.mu.RUnlock()
	if count != 0 {
		t.Errorf("Expected expired record to be rejected, but %d records stored", count)
	}
}

func TestStoreReachKeepsNewerSeq(t *testing.T) {
	cache := NewDirectoryCache()
	expiry := time.Now().Add(1 * time.Hour)

	// Apply record with seq=10
	r1 := lad.ReachRecord{
		TenantID:  "tenant-1",
		NodeID:    "node-1",
		Region:    "iad",
		ExpiresAt: expiry,
	}
	body, _ := json.Marshal(r1)
	cache.Apply(lad.Record{Topic: lad.TopicReach, Body: body, Seq: 10, Timestamp: time.Now()})

	// Apply older record with seq=5 — should be rejected
	r2 := lad.ReachRecord{
		TenantID:  "tenant-1",
		NodeID:    "node-1",
		Region:    "lhr",
		ExpiresAt: expiry,
	}
	body, _ = json.Marshal(r2)
	cache.Apply(lad.Record{Topic: lad.TopicReach, Body: body, Seq: 5, Timestamp: time.Now()})

	ctx := context.Background()
	result, _ := cache.Reach(ctx, "tenant-1", ReachQuery{NodeID: "node-1"})
	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}
	if result[0].Region != "iad" {
		t.Errorf("Expected region 'iad' (newer seq), got %s", result[0].Region)
	}
}

func TestDumpSkipsExpiredReach(t *testing.T) {
	cache := NewDirectoryCache()

	// Manually insert: one expired, one valid.
	cache.mu.Lock()
	cache.reach["tenant-1"] = map[string]lad.ReachRecord{
		"node-expired": {
			TenantID:  "tenant-1",
			NodeID:    "node-expired",
			ExpiresAt: time.Now().Add(-1 * time.Hour),
		},
		"node-live": {
			TenantID:  "tenant-1",
			NodeID:    "node-live",
			ExpiresAt: time.Now().Add(1 * time.Hour),
		},
	}
	cache.mu.Unlock()

	records := cache.Dump()
	reachCount := 0
	for _, rec := range records {
		if rec.Topic == lad.TopicReach {
			reachCount++
			if rec.NodeID == "node-expired" {
				t.Error("Dump should not include expired reach records")
			}
		}
	}
	if reachCount != 1 {
		t.Errorf("Expected 1 reach record in Dump, got %d", reachCount)
	}
}

func TestStartStopEviction(t *testing.T) {
	cache := NewDirectoryCache()

	// Insert an expired record directly.
	cache.mu.Lock()
	cache.reach["tenant-1"] = map[string]lad.ReachRecord{
		"node-1": {
			TenantID:  "tenant-1",
			NodeID:    "node-1",
			ExpiresAt: time.Now().Add(-1 * time.Second),
		},
	}
	cache.mu.Unlock()

	// Start eviction with a very short interval.
	cache.StartEviction(50 * time.Millisecond)

	// Wait long enough for at least one sweep.
	time.Sleep(200 * time.Millisecond)

	cache.StopEviction()

	cache.mu.RLock()
	count := len(cache.reach["tenant-1"])
	cache.mu.RUnlock()
	if count != 0 {
		t.Errorf("Expected expired record to be evicted by background sweep, but %d remain", count)
	}
}

func BenchmarkDirectoryCacheApply(b *testing.B) {
	cache := NewDirectoryCache()
	member := lad.MemberRecord{
		TenantID: "tenant-1",
		NodeID:   "node-1",
	}
	body, _ := json.Marshal(member)
	rec := lad.Record{Topic: lad.TopicMember, Body: body}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Apply(rec)
	}
}

func BenchmarkDirectoryCacheMembers(b *testing.B) {
	cache := NewDirectoryCache()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		member := lad.MemberRecord{
			TenantID: "tenant-1",
			NodeID:   string(rune(i)),
		}
		body, _ := json.Marshal(member)
		cache.Apply(lad.Record{Topic: lad.TopicMember, Body: body})
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Members(ctx, "tenant-1")
	}
}

func BenchmarkDirectoryCacheReach(b *testing.B) {
	cache := NewDirectoryCache()
	expireTime := time.Now().Add(1 * time.Hour)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		reach := lad.ReachRecord{
			TenantID:     "tenant-1",
			NodeID:       string(rune(i)),
			Region:       "iad",
			Availability: 0.95,
			LoadFactor:   0.3,
			ExpiresAt:    expireTime,
		}
		body, _ := json.Marshal(reach)
		cache.Apply(lad.Record{Topic: lad.TopicReach, Body: body})
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Reach(ctx, "tenant-1", ReachQuery{MinHealth: 0.5, MaxLoad: 0.8, Limit: 10})
	}
}
