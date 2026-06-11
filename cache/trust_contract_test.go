/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package cache

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// memberRecord builds a non-tombstone TopicMember record for (tenant,node)
// stamped with a fresh sequence number and timestamp so applyCore's
// tombstone-blocking path admits it.
func memberRecord(t *testing.T, tenant, nodeID string, pub []byte) lad.Record {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{
		TenantID:  tenant,
		NodeID:    nodeID,
		PubKey:    pub,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal member: %v", err)
	}
	return lad.Record{
		Topic:     lad.TopicMember,
		TenantID:  tenant,
		NodeID:    nodeID,
		Body:      body,
		Seq:       1,
		Timestamp: time.Now().UTC(),
	}
}

// TestTrustContract_ACLFiresOnApply_SkippedOnApplyLocal pins the two-door
// semantics from doc.go: external ingress (Apply) runs the per-topic ACL
// and is rejected by it; the trusted in-process bridge door (ApplyLocal)
// bypasses the ACL and merges. A refactor that routes ApplyLocal through
// the ACL — or routes Apply around it — breaks this test.
func TestTrustContract_ACLFiresOnApply_SkippedOnApplyLocal(t *testing.T) {
	const tenant, node = "t-acme", "vl1_node_a"
	pub, _, _ := ed25519.GenerateKey(nil)

	// --- Apply: ACL denies → record must NOT be present ---
	denyCache := NewDirectoryCache()
	denyCache.RegisterACL(lad.TopicMember, func(string, []byte, lad.Record) error {
		return lad.ErrACLDenied
	})
	rec := memberRecord(t, tenant, node, pub)

	if err := denyCache.Apply(rec); !errors.Is(err, lad.ErrACLDenied) {
		t.Fatalf("Apply with denying ACL: got err %v, want ErrACLDenied", err)
	}
	if _, ok := denyCache.Store().GetMember(tenant, node); ok {
		t.Fatal("Apply was rejected by the ACL but the record reached the store — the ACL gate did not fire before merge")
	}

	// --- ApplyLocal: same denying ACL registered → record MUST be present ---
	localCache := NewDirectoryCache()
	localCache.RegisterACL(lad.TopicMember, func(string, []byte, lad.Record) error {
		return lad.ErrACLDenied // identical ACL; ApplyLocal must not consult it
	})
	rec2 := memberRecord(t, tenant, node, pub)

	if err := localCache.ApplyLocal(rec2); err != nil {
		t.Fatalf("ApplyLocal must bypass the ACL: got err %v, want nil", err)
	}
	if _, ok := localCache.Store().GetMember(tenant, node); !ok {
		t.Fatal("ApplyLocal returned nil but the record is absent from the store — the trusted-bridge door failed to merge")
	}
}

// TestTrustContract_SignedTopicACL_RejectsUnverifiableIngress proves the
// gate semantics with the REAL signed-topic ACL (lad.VerifyRecord): every
// record that fails signature verification is rejected at Apply and never
// reaches the store, while a correctly-signed record passes. This is the
// enforceable half of the contract; the cross-module ApplyLocal allowlist
// (lad_reach_bridge.go, swarm_integration.go) is review-enforced against
// doc.go — a Go test in this module cannot scan the separate HSTLES
// Library module, so it is deliberately not attempted here.
func TestTrustContract_SignedTopicACL_RejectsUnverifiableIngress(t *testing.T) {
	const tenant = "t-acme"

	// The signed-topic ACL the Library registers in production: admit iff
	// the record's Ed25519 signature reproduces over signatureContent.
	signedTopicACL := func(_ string, _ []byte, rec lad.Record) error {
		if !lad.VerifyRecord(rec) {
			return lad.ErrSignatureInvalid
		}
		return nil
	}

	pub, priv, _ := ed25519.GenerateKey(nil)
	_, wrongPriv, _ := ed25519.GenerateKey(nil)

	// Positive control: a correctly-signed record passes and is stored.
	t.Run("correctly_signed_passes", func(t *testing.T) {
		c := NewDirectoryCache()
		c.RegisterACL(lad.TopicMember, signedTopicACL)
		rec := memberRecord(t, tenant, "vl1_signed_ok", pub)
		lad.SignRecord(&rec, priv)
		if err := c.Apply(rec); err != nil {
			t.Fatalf("correctly-signed record rejected: %v", err)
		}
		if _, ok := c.Store().GetMember(tenant, "vl1_signed_ok"); !ok {
			t.Fatal("correctly-signed record not stored")
		}
	})

	// Negative cases: each must be rejected at Apply and never stored.
	cases := []struct {
		name    string
		node    string
		corrupt func(rec *lad.Record)
	}{
		{
			name:    "unsigned",
			node:    "vl1_unsigned",
			corrupt: func(*lad.Record) {}, // no signature applied at all
		},
		{
			name: "wrong_key_signature",
			node: "vl1_wrongkey",
			corrupt: func(rec *lad.Record) {
				// Sign with one key but claim a different author.
				lad.SignRecord(rec, wrongPriv)
				rec.AuthorPubKey = pub // signature no longer matches the claimed author
			},
		},
		{
			name: "tampered_after_signing",
			node: "vl1_tampered",
			corrupt: func(rec *lad.Record) {
				lad.SignRecord(rec, priv)
				// Mutate a signed field after signing → VerifyRecord fails.
				rec.HLCTimestamp++
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewDirectoryCache()
			c.RegisterACL(lad.TopicMember, signedTopicACL)
			rec := memberRecord(t, tenant, tc.node, pub)
			tc.corrupt(&rec)

			if err := c.Apply(rec); err == nil {
				t.Fatalf("Apply admitted an unverifiable %s record — external ingress reached merge without passing the ACL", tc.name)
			}
			if _, ok := c.Store().GetMember(tenant, tc.node); ok {
				t.Fatalf("unverifiable %s record reached the store despite ACL rejection", tc.name)
			}
		})
	}
}
