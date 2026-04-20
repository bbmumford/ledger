/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
)

var ErrBlobNotFound = errors.New("ledger: blob not found")

// BlobStore manages content-addressed binary objects alongside records.
// This is the historical (pre-Session-B) interface: no context, returns
// (count, bytes, err) from Size. Retained verbatim so existing callers
// (notably minerva/persistence/tiered.go and the HSTLES library) compile
// unchanged. New code should prefer ContextBlobStore.
type BlobStore interface {
	Put(data []byte) (cid string, err error)
	Get(cid string) ([]byte, error)
	Has(cid string) (bool, error)
	Delete(cid string) error
	Size() (count int, bytes int64, err error)
}

// ContextBlobStore is the Session-B-harmonised blob-store interface shared
// with Minerva's messaging subsystem. It accepts a context on every
// method so long-running operations (IndexedDB writes, S3 PUTs, Turso
// range scans) can be cancelled; Size returns a single int64 byte total
// because callers that also want the record count can wrap Has with a
// counter. Adapt legacy BlobStore instances via BlobStoreAdapter.
type ContextBlobStore interface {
	Put(ctx context.Context, data []byte) (cid string, err error)
	Get(ctx context.Context, cid string) ([]byte, error)
	Has(ctx context.Context, cid string) (bool, error)
	Delete(ctx context.Context, cid string) error
	Size(ctx context.Context) (int64, error)
}

// BlobStoreAdapter wraps a legacy BlobStore as a ContextBlobStore. The
// adapter ignores the context (the underlying store has no cancellation
// hook); callers that need cancellation should implement
// ContextBlobStore directly. Size returns only the byte total — the
// record count from the legacy API is discarded.
type BlobStoreAdapter struct {
	Inner BlobStore
}

// NewBlobStoreAdapter returns a ContextBlobStore backed by inner.
func NewBlobStoreAdapter(inner BlobStore) *BlobStoreAdapter {
	return &BlobStoreAdapter{Inner: inner}
}

// Put delegates to the wrapped BlobStore (ctx is unused).
func (a *BlobStoreAdapter) Put(_ context.Context, data []byte) (string, error) {
	return a.Inner.Put(data)
}

// Get delegates to the wrapped BlobStore (ctx is unused).
func (a *BlobStoreAdapter) Get(_ context.Context, cid string) ([]byte, error) {
	return a.Inner.Get(cid)
}

// Has delegates to the wrapped BlobStore (ctx is unused).
func (a *BlobStoreAdapter) Has(_ context.Context, cid string) (bool, error) {
	return a.Inner.Has(cid)
}

// Delete delegates to the wrapped BlobStore (ctx is unused).
func (a *BlobStoreAdapter) Delete(_ context.Context, cid string) error {
	return a.Inner.Delete(cid)
}

// Size returns the byte total from the legacy BlobStore, discarding the
// record count so the return shape matches ContextBlobStore.
func (a *BlobStoreAdapter) Size(_ context.Context) (int64, error) {
	_, bytes, err := a.Inner.Size()
	return bytes, err
}

// ComputeCID returns the SHA-256 content hash of data.
func ComputeCID(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// MemoryBlobStore is an in-memory BlobStore for testing.
type MemoryBlobStore struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

// NewMemoryBlobStore creates a new in-memory blob store.
func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{
		blobs: make(map[string][]byte),
	}
}

func (s *MemoryBlobStore) Put(data []byte) (string, error) {
	cid := ComputeCID(data)
	cp := make([]byte, len(data))
	copy(cp, data)

	s.mu.Lock()
	s.blobs[cid] = cp
	s.mu.Unlock()
	return cid, nil
}

func (s *MemoryBlobStore) Get(cid string) ([]byte, error) {
	s.mu.RLock()
	data, ok := s.blobs[cid]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrBlobNotFound
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (s *MemoryBlobStore) Has(cid string) (bool, error) {
	s.mu.RLock()
	_, ok := s.blobs[cid]
	s.mu.RUnlock()
	return ok, nil
}

func (s *MemoryBlobStore) Delete(cid string) error {
	s.mu.Lock()
	delete(s.blobs, cid)
	s.mu.Unlock()
	return nil
}

func (s *MemoryBlobStore) Size() (int, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total int64
	for _, data := range s.blobs {
		total += int64(len(data))
	}
	return len(s.blobs), total, nil
}
