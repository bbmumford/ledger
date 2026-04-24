// Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. Licensed under MIT.

package cache

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// TestReachRecord_MetadataRoundTrip verifies Metadata survives JSON round-trip
// through the cache unmarshal path. This is the full Reach → Body → Apply →
// cache store path that the gossip subsystem uses.
func TestReachRecord_MetadataRoundTrip(t *testing.T) {
	// Simulate what reach.Publisher.appendToLedger produces: a ReachRecord
	// with Metadata populated, marshaled to JSON. The JSON goes into
	// lad.Record.Body. Body is unmarshaled by lad.UnmarshalReach into
	// lad.ReachRecord and stored in the cache.
	type reachWithMeta struct {
		NodeID   string            `json:"node_id"`
		TenantID string            `json:"tenant"`
		Region   string            `json:"region"`
		Metadata map[string]string `json:"meta,omitempty"`
	}
	sent := reachWithMeta{
		NodeID:   "vl1_test",
		TenantID: "",
		Region:   "iad",
		Metadata: map[string]string{
			"service_name": "devices.orbtr.io",
			"region":       "iad",
			"roles":        "anchor,platform.tenant",
		},
	}
	body, err := json.Marshal(sent)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := lad.UnmarshalReach(body)
	if err != nil {
		t.Fatalf("UnmarshalReach: %v", err)
	}
	if got.Metadata["service_name"] != "devices.orbtr.io" {
		t.Fatalf("expected service_name=devices.orbtr.io, got %q (meta=%v)", got.Metadata["service_name"], got.Metadata)
	}
	if got.Metadata["roles"] != "anchor,platform.tenant" {
		t.Fatalf("expected roles, got %q", got.Metadata["roles"])
	}
	if got.Region != "iad" {
		t.Fatalf("expected region=iad, got %q", got.Region)
	}
}

// TestCache_DeltaApplierRebuildsFullRecord verifies that when a
// ReachDeltaApplier is registered, the cache rebuilds the full
// ReachRecord from (stored base body + incoming delta body). Metadata
// must survive; addresses reflect the delta ops.
func TestCache_DeltaApplierRebuildsFullRecord(t *testing.T) {
	c := NewDirectoryCache()

	// Applier: ignores the delta body contents and just returns a synthesized
	// full record with the base's NodeID + Metadata + a new Address.
	c.SetReachDeltaApplier(func(baseBody, deltaBody []byte) ([]byte, error) {
		var base lad.ReachRecord
		if err := json.Unmarshal(baseBody, &base); err != nil {
			return nil, err
		}
		// Pretend the delta added one address.
		base.Addresses = append(base.Addresses, lad.ReachAddress{
			Host: "1.2.3.4", Port: 8080, Proto: "udp", Scope: "public",
		})
		base.SchemaVersion = 1 // full, not delta
		return json.Marshal(&base)
	})

	// Seed full snapshot.
	full := lad.ReachRecord{
		TenantID:  "",
		NodeID:    "vl1_alice",
		Region:    "iad",
		UpdatedAt: time.Now(),
		Addresses: []lad.ReachAddress{{Host: "5.6.7.8", Port: 9000, Proto: "udp", Scope: "public"}},
		Metadata: map[string]string{
			"service_name": "alice.example.com",
			"region":       "iad",
		},
	}
	fullBody, _ := json.Marshal(full)
	if err := c.Apply(lad.Record{
		Topic:     lad.TopicReach,
		NodeID:    "vl1_alice",
		Body:      fullBody,
		Seq:       1,
		Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("apply full: %v", err)
	}

	// Send a delta (body shape irrelevant — applier rewrites).
	deltaBody := []byte(`{"node_id":"vl1_alice","tenant":"","v":32769,"ops":[]}`)
	if err := c.Apply(lad.Record{
		Topic:     lad.TopicReach,
		NodeID:    "vl1_alice",
		Body:      deltaBody,
		Seq:       2,
		Timestamp: time.Now().Add(time.Second),
	}); err != nil {
		t.Fatalf("apply delta: %v", err)
	}

	reaches, _ := c.Reach(context.Background(), "", ReachQuery{})
	if len(reaches) != 1 {
		t.Fatalf("expected 1 reach, got %d", len(reaches))
	}
	got := reaches[0]
	if got.Metadata["service_name"] != "alice.example.com" {
		t.Fatalf("metadata lost: %+v", got.Metadata)
	}
	if len(got.Addresses) != 2 {
		t.Fatalf("expected 2 addresses after delta, got %d", len(got.Addresses))
	}
	foundNewAddr := false
	for _, a := range got.Addresses {
		if a.Host == "1.2.3.4" && a.Port == 8080 {
			foundNewAddr = true
		}
	}
	if !foundNewAddr {
		t.Fatalf("delta applier's new address not present: %+v", got.Addresses)
	}
	// Seq should be from the delta envelope.
	if got.Seq != 2 {
		t.Fatalf("expected Seq=2 from delta envelope, got %d", got.Seq)
	}
}

// TestCache_DeltaWithoutApplierSkips verifies the fallback behavior: no
// applier + delta → skip, full record untouched.
func TestCache_SkipsReachDelta(t *testing.T) {
	c := NewDirectoryCache()

	// Seed with a full snapshot (has Metadata)
	full := lad.ReachRecord{
		TenantID:  "",
		NodeID:    "vl1_alice",
		Region:    "iad",
		UpdatedAt: time.Now(),
		Metadata: map[string]string{
			"service_name": "alice.example.com",
			"region":       "iad",
		},
	}
	fullBody, _ := json.Marshal(full)
	if err := c.Apply(lad.Record{
		Topic:    lad.TopicReach,
		NodeID:   "vl1_alice",
		Body:     fullBody,
		Seq:      1,
		Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("apply full: %v", err)
	}

	// Confirm seeded
	reaches, _ := c.Reach(context.Background(), "", ReachQuery{})
	if len(reaches) != 1 || reaches[0].Metadata["service_name"] != "alice.example.com" {
		t.Fatalf("seed failed: %+v", reaches)
	}

	// Send a delta — should be SKIPPED
	deltaBody := []byte(`{"node_id":"vl1_alice","tenant":"","v":32769,"hlc":{"wall":0,"logical":0},"ops":[]}`)
	if err := c.Apply(lad.Record{
		Topic:    lad.TopicReach,
		NodeID:   "vl1_alice",
		Body:     deltaBody,
		Seq:      2,
		Timestamp: time.Now().Add(time.Second),
	}); err != nil {
		t.Fatalf("apply delta: %v", err)
	}

	reaches, _ = c.Reach(context.Background(), "", ReachQuery{})
	if len(reaches) != 1 {
		t.Fatalf("expected 1 reach after delta, got %d", len(reaches))
	}
	if reaches[0].Metadata["service_name"] != "alice.example.com" {
		t.Fatalf("delta clobbered metadata: %+v", reaches[0].Metadata)
	}
	if reaches[0].Region != "iad" {
		t.Fatalf("delta clobbered region: %q", reaches[0].Region)
	}
}
