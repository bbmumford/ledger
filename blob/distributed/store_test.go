/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T, inMemory bool) *Store {
	t.Helper()
	seed := make([]byte, 32)
	rand.Read(seed)
	key, err := DeriveStorageKey(seed)
	if err != nil {
		t.Fatalf("DeriveStorageKey: %v", err)
	}
	cfg := Config{
		StorageKey: key,
		InMemory:   inMemory,
	}
	if !inMemory {
		cfg.Root = t.TempDir()
	}
	s, err := NewDistributedBlobStore(cfg)
	if err != nil {
		t.Fatalf("NewDistributedBlobStore: %v", err)
	}
	return s
}

func TestBlobStoreRoundtripInMemory(t *testing.T) {
	s := newTestStore(t, true)
	testBlobStoreConformance(t, s)
}

func TestBlobStoreRoundtripOnDisk(t *testing.T) {
	s := newTestStore(t, false)
	testBlobStoreConformance(t, s)
}

func testBlobStoreConformance(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	data := []byte("hello distributed world")
	cid, err := s.Put(ctx, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if cid == "" {
		t.Fatal("empty cid")
	}

	// Has true
	ok, err := s.Has(ctx, cid)
	if err != nil || !ok {
		t.Fatalf("Has: ok=%v err=%v", ok, err)
	}

	// Get roundtrip
	got, err := s.Get(ctx, cid)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("get mismatch: %q vs %q", got, data)
	}

	// Size
	count, bytesTotal, err := s.Size(ctx)
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if count != 1 || bytesTotal != int64(len(data)) {
		t.Errorf("Size: got count=%d bytes=%d, want 1/%d", count, bytesTotal, len(data))
	}

	// Delete + Has false
	if err := s.Delete(ctx, cid); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ok, _ = s.Has(ctx, cid)
	if ok {
		t.Fatal("Has after delete: expected false")
	}
	if _, err := s.Get(ctx, cid); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("Get after delete: got %v want ErrBlobNotFound", err)
	}

	// Delete of missing cid is a no-op.
	if err := s.Delete(ctx, cid); err != nil {
		t.Errorf("Delete missing: %v", err)
	}

	// Put idempotent.
	cid2, err := s.Put(ctx, data)
	if err != nil {
		t.Fatalf("Put #2: %v", err)
	}
	if cid2 != cid {
		t.Errorf("cid drift: %s vs %s", cid2, cid)
	}
	cid3, err := s.Put(ctx, data)
	if err != nil {
		t.Fatalf("Put #3: %v", err)
	}
	if cid3 != cid {
		t.Errorf("cid drift 2: %s vs %s", cid3, cid)
	}
	count, _, _ = s.Size(ctx)
	if count != 1 {
		t.Errorf("idempotent put: got count=%d want 1", count)
	}
}

func TestStoreEncryptsOnDisk(t *testing.T) {
	s := newTestStore(t, false)
	ctx := context.Background()
	plaintext := []byte("super-secret-bytes-that-should-not-appear-on-disk")
	cid, err := s.Put(ctx, plaintext)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(s.root, cid+".blob"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.Contains(raw, plaintext) {
		t.Fatal("plaintext appeared on disk")
	}
}

func TestStoreQuotaRejection(t *testing.T) {
	seed := make([]byte, 32)
	rand.Read(seed)
	key, _ := DeriveStorageKey(seed)
	fs := &fakeStatter{total: 100 * gb, free: 30 * gb} // at-ceiling -> budget 0
	qm := NewManager(DefaultManagerConfig(), "", fs)
	s, err := NewDistributedBlobStore(Config{
		StorageKey: key,
		InMemory:   true,
		Quota:      qm,
	})
	if err != nil {
		t.Fatalf("NewDistributedBlobStore: %v", err)
	}
	_, err = s.Put(context.Background(), []byte("hello"))
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
}

func TestStoreRehydrate(t *testing.T) {
	// Write on one store, re-open on a second pointed at the same dir.
	seed := make([]byte, 32)
	rand.Read(seed)
	key, _ := DeriveStorageKey(seed)
	dir := t.TempDir()

	s1, err := NewDistributedBlobStore(Config{Root: dir, StorageKey: key})
	if err != nil {
		t.Fatalf("NewDistributedBlobStore: %v", err)
	}
	cid, err := s1.Put(context.Background(), []byte("persistent bytes"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	s2, err := NewDistributedBlobStore(Config{Root: dir, StorageKey: key})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	ok, err := s2.Has(context.Background(), cid)
	if err != nil || !ok {
		t.Fatalf("rehydrated Has: ok=%v err=%v", ok, err)
	}
	got, err := s2.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("rehydrated Get: %v", err)
	}
	if !bytes.Equal(got, []byte("persistent bytes")) {
		t.Fatalf("rehydrated plaintext mismatch: %q", got)
	}
}
