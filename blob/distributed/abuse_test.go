/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeRefs lets tests toggle reference-ness per rootCID.
type fakeRefs struct {
	mu    sync.Mutex
	alive map[string]bool
}

func (f *fakeRefs) IsReferenced(ctx context.Context, rootCID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.alive[rootCID], nil
}

func (f *fakeRefs) set(cid string, alive bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.alive == nil {
		f.alive = map[string]bool{}
	}
	f.alive[cid] = alive
}

func makeSignedManifest(t *testing.T, size int) (*Manifest, []byte, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	data := make([]byte, size)
	rand.Read(data)
	m, _, err := Split(data, 0)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	sig, err := SignManifest(m, priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return m, sig, pub, priv
}

func TestValidateStorablePerTypeLimits(t *testing.T) {
	// Avatar cap is 2 MB; a 3 MB avatar must be rejected.
	m, sig, pub, _ := makeSignedManifest(t, 3*1024*1024)
	refs := &fakeRefs{}
	refs.set(m.RootCID, true)
	ag := NewAbuseGuard(AbuseGuardConfig{
		References: refs,
		Limits:     DefaultPerTypeLimits(),
	})
	err := ag.ValidateStorable(context.Background(), m, sig, pub, RecordTypeAvatar)
	if !errors.Is(err, ErrPerTypeExceeded) {
		t.Fatalf("expected ErrPerTypeExceeded, got %v", err)
	}

	// 1 MB avatar accepted.
	m2, sig2, pub2, _ := makeSignedManifest(t, 1*1024*1024)
	refs.set(m2.RootCID, true)
	if err := ag.ValidateStorable(context.Background(), m2, sig2, pub2, RecordTypeAvatar); err != nil {
		t.Fatalf("unexpected reject: %v", err)
	}
}

func TestValidateStorableReferenceRequired(t *testing.T) {
	m, sig, pub, _ := makeSignedManifest(t, 200*1024)
	refs := &fakeRefs{}
	// No reference set -> orphan.
	ag := NewAbuseGuard(AbuseGuardConfig{References: refs, Limits: DefaultPerTypeLimits()})
	err := ag.ValidateStorable(context.Background(), m, sig, pub, RecordTypeListing)
	if !errors.Is(err, ErrNoReference) {
		t.Fatalf("expected ErrNoReference, got %v", err)
	}
}

func TestValidateStorablePerUploaderQuota(t *testing.T) {
	m, sig, pub, _ := makeSignedManifest(t, 1*1024*1024)
	refs := &fakeRefs{}
	refs.set(m.RootCID, true)
	ag := NewAbuseGuard(AbuseGuardConfig{
		References: refs,
		Limits:     DefaultPerTypeLimits(),
		PerPeerCap: 1 * 1024 * 1024, // exactly one manifest's worth
	})
	// First store: OK.
	if err := ag.ValidateStorable(context.Background(), m, sig, pub, RecordTypeListing); err != nil {
		t.Fatalf("first: %v", err)
	}
	ag.ChargeUploader(pub, m.TotalBytes)
	// Second store by same uploader of another file: should exceed.
	m2, sig2, _, _ := makeSignedManifest(t, 1*1024*1024)
	refs.set(m2.RootCID, true)
	// Sign m2 with pub's key? We need the same uploader. Re-sign with
	// previous priv: reuse makeSignedManifest isn't enough because each
	// call creates a new key. Reconstruct with the original key.
	_ = m2
	_ = sig2
	// Easier: re-sign m itself as a second "upload" and call with same pub.
	if err := ag.ValidateStorable(context.Background(), m, sig, pub, RecordTypeListing); !errors.Is(err, ErrPerPeerExceeded) {
		t.Fatalf("expected ErrPerPeerExceeded, got %v", err)
	}
}

func TestValidateStorableBadSignature(t *testing.T) {
	m, sig, pub, _ := makeSignedManifest(t, 128)
	sig[0] ^= 0xFF
	refs := &fakeRefs{}
	refs.set(m.RootCID, true)
	ag := NewAbuseGuard(AbuseGuardConfig{References: refs, Limits: DefaultPerTypeLimits()})
	err := ag.ValidateStorable(context.Background(), m, sig, pub, RecordTypeListing)
	if !errors.Is(err, ErrBadUploaderSig) {
		t.Fatalf("expected ErrBadUploaderSig, got %v", err)
	}
}

func TestGarbageCollectOrphanAfterGrace(t *testing.T) {
	refs := &fakeRefs{}
	ag := NewAbuseGuard(AbuseGuardConfig{
		References:  refs,
		Limits:      DefaultPerTypeLimits(),
		OrphanGrace: 50 * time.Millisecond,
	})
	cid := "abc123"
	refs.set(cid, false) // orphaned from birth

	// First sweep: registers first-seen, returns nothing yet.
	got, err := ag.GarbageCollect(context.Background(), []string{cid})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("first sweep: got %v, want empty", got)
	}

	// Before grace: still nothing.
	time.Sleep(10 * time.Millisecond)
	got, _ = ag.GarbageCollect(context.Background(), []string{cid})
	if len(got) != 0 {
		t.Fatalf("pre-grace: got %v, want empty", got)
	}

	// After grace: evict.
	time.Sleep(60 * time.Millisecond)
	got, _ = ag.GarbageCollect(context.Background(), []string{cid})
	if len(got) != 1 || got[0] != cid {
		t.Fatalf("post-grace: got %v, want [%s]", got, cid)
	}

	// If reference reappears, first-seen resets.
	refs.set(cid, true)
	got, _ = ag.GarbageCollect(context.Background(), []string{cid})
	if len(got) != 0 {
		t.Fatalf("re-referenced: got %v, want empty", got)
	}
}

func TestAbuseGuardCheck_FirstFetchAllowed(t *testing.T) {
	ag := NewAbuseGuard(AbuseGuardConfig{
		Limits:                     DefaultPerTypeLimits(),
		MaxReplicationBytesPerPeer: 10 * 1024 * 1024,
		MaxReplicationBytesTotal:   100 * 1024 * 1024,
	})
	if err := ag.Check(context.Background(), "cid-1", "peer-A", 4096, RecordTypeReplication); err != nil {
		t.Fatalf("first fetch unexpectedly rejected: %v", err)
	}
	if got := ag.ReplicationUsage("peer-A"); got != 4096 {
		t.Errorf("peer-A usage: got %d want 4096", got)
	}
	if got := ag.ReplicationTotal(); got != 4096 {
		t.Errorf("total usage: got %d want 4096", got)
	}
}

func TestAbuseGuardCheck_PerPeerQuotaEnforced(t *testing.T) {
	ag := NewAbuseGuard(AbuseGuardConfig{
		Limits:                     DefaultPerTypeLimits(),
		MaxReplicationBytesPerPeer: 10 * 1024,
		MaxReplicationBytesTotal:   1024 * 1024,
	})
	// Peer A fills its quota.
	if err := ag.Check(context.Background(), "cid-A1", "peer-A", 8*1024, RecordTypeReplication); err != nil {
		t.Fatalf("peer-A first: %v", err)
	}
	// Next accept would exceed.
	err := ag.Check(context.Background(), "cid-A2", "peer-A", 4*1024, RecordTypeReplication)
	if !errors.Is(err, ErrPeerQuotaExceeded) {
		t.Fatalf("expected ErrPeerQuotaExceeded, got %v", err)
	}
	// Peer B is unaffected by peer A's usage.
	if err := ag.Check(context.Background(), "cid-B1", "peer-B", 8*1024, RecordTypeReplication); err != nil {
		t.Fatalf("peer-B unaffected by peer-A: %v", err)
	}
}

func TestAbuseGuardCheck_TotalReplicationQuotaEnforced(t *testing.T) {
	ag := NewAbuseGuard(AbuseGuardConfig{
		Limits:                     DefaultPerTypeLimits(),
		MaxReplicationBytesPerPeer: 100 * 1024, // generous per-peer
		MaxReplicationBytesTotal:   10 * 1024,  // tight aggregate
	})
	if err := ag.Check(context.Background(), "cid-1", "peer-A", 6*1024, RecordTypeReplication); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second fetch from a different peer pushes aggregate past the cap.
	err := ag.Check(context.Background(), "cid-2", "peer-B", 6*1024, RecordTypeReplication)
	if !errors.Is(err, ErrReplicationQuotaExceeded) {
		t.Fatalf("expected ErrReplicationQuotaExceeded, got %v", err)
	}
}

func TestAbuseGuardCheck_AlreadyHeldReturnsSentinel(t *testing.T) {
	ag := NewAbuseGuard(AbuseGuardConfig{
		Limits:                     DefaultPerTypeLimits(),
		MaxReplicationBytesPerPeer: 10 * 1024 * 1024,
		MaxReplicationBytesTotal:   100 * 1024 * 1024,
	})
	if err := ag.Check(context.Background(), "cid-dup", "peer-A", 2048, RecordTypeReplication); err != nil {
		t.Fatalf("first: %v", err)
	}
	err := ag.Check(context.Background(), "cid-dup", "peer-B", 2048, RecordTypeReplication)
	if !errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("expected ErrAlreadyHeld, got %v", err)
	}
	// The duplicate must not have charged peer-B.
	if got := ag.ReplicationUsage("peer-B"); got != 0 {
		t.Errorf("peer-B usage: got %d want 0 (dup must not charge)", got)
	}
}

func TestAbuseGuardCheck_RecordTypeAllowList(t *testing.T) {
	ag := NewAbuseGuard(AbuseGuardConfig{
		Limits:                        DefaultPerTypeLimits(),
		MaxReplicationBytesPerPeer:    10 * 1024 * 1024,
		MaxReplicationBytesTotal:      100 * 1024 * 1024,
		ReplicationAllowedRecordTypes: []RecordType{RecordTypeReplication, RecordTypeListing},
	})
	// Listing is allowed.
	if err := ag.Check(context.Background(), "cid-ok", "peer-A", 512, RecordTypeListing); err != nil {
		t.Fatalf("listing allowed but rejected: %v", err)
	}
	// Message is NOT on the allow-list → reject.
	err := ag.Check(context.Background(), "cid-bad", "peer-A", 512, RecordTypeMessage)
	if !errors.Is(err, ErrRecordTypeNotAllowed) {
		t.Fatalf("expected ErrRecordTypeNotAllowed, got %v", err)
	}
}

func TestAbuseGuardCheck_ReleaseRefundsPeer(t *testing.T) {
	ag := NewAbuseGuard(AbuseGuardConfig{
		Limits:                     DefaultPerTypeLimits(),
		MaxReplicationBytesPerPeer: 8 * 1024,
		MaxReplicationBytesTotal:   1024 * 1024,
	})
	if err := ag.Check(context.Background(), "cid-1", "peer-A", 6*1024, RecordTypeReplication); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Would exceed if not refunded.
	if err := ag.Check(context.Background(), "cid-2", "peer-A", 4*1024, RecordTypeReplication); !errors.Is(err, ErrPeerQuotaExceeded) {
		t.Fatalf("expected ErrPeerQuotaExceeded pre-release, got %v", err)
	}
	ag.ReleaseReplication("cid-1", "peer-A")
	if got := ag.ReplicationUsage("peer-A"); got != 0 {
		t.Errorf("after release: got %d want 0", got)
	}
	// Now the second fetch fits.
	if err := ag.Check(context.Background(), "cid-2", "peer-A", 4*1024, RecordTypeReplication); err != nil {
		t.Fatalf("post-release: %v", err)
	}
}

func TestChargeAndRefundUploader(t *testing.T) {
	ag := NewAbuseGuard(AbuseGuardConfig{Limits: DefaultPerTypeLimits()})
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	ag.ChargeUploader(pub, 1024)
	ag.ChargeUploader(pub, 512)
	if got := ag.UploaderUsage(pub); got != 1536 {
		t.Errorf("usage: got %d want 1536", got)
	}
	ag.RefundUploader(pub, 1024)
	if got := ag.UploaderUsage(pub); got != 512 {
		t.Errorf("after refund: got %d want 512", got)
	}
	// Refund more than owed -> clamp to zero.
	ag.RefundUploader(pub, 10_000)
	if got := ag.UploaderUsage(pub); got != 0 {
		t.Errorf("clamp: got %d want 0", got)
	}
}
