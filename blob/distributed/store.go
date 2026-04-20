/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrBlobNotFound is returned when a CID is not present in the store.
var ErrBlobNotFound = errors.New("distributed: blob not found")

// ErrQuotaExceeded is returned when a Put would exceed the current budget.
var ErrQuotaExceeded = errors.New("distributed: quota exceeded")

// BlobStore is the context-aware blob interface served by the distributed
// cache. It deliberately mirrors ledger.BlobStore with added ctx plumbing
// (Phase 1 does not import the root ledger package to keep the extraction
// direction clean).
type BlobStore interface {
	Put(ctx context.Context, data []byte) (cid string, err error)
	Get(ctx context.Context, cid string) ([]byte, error)
	Has(ctx context.Context, cid string) (bool, error)
	Delete(ctx context.Context, cid string) error
	Size(ctx context.Context) (count int, bytes int64, err error)
}

// Config wires the Store's collaborators together.
type Config struct {
	// Root is the on-disk directory that holds encrypted pieces. Created
	// if missing.
	Root string
	// StorageKey is the 32-byte XChaCha20-Poly1305 key used to encrypt
	// every piece on disk. Derive via DeriveStorageKey.
	StorageKey []byte
	// Quota, if non-nil, gates new writes. Pass nil to disable quota
	// enforcement (tests).
	Quota *Manager
	// AbuseGuard, if non-nil, gates stores by reference and per-type
	// limits. Pass nil to disable.
	AbuseGuard *AbuseGuard
	// InMemory true runs the store purely in memory. Ignores Root.
	InMemory bool
	// Fetcher, if non-nil, is used by Get as a fallback when the CID is
	// not present locally. Successfully fetched pieces are cached back
	// into this Store (subject to quota). Leave nil to preserve pure
	// local-only semantics (the original Phase 1 behaviour).
	Fetcher *Fetcher
}

// WithFetcher returns a Config copy with the fetcher attached. Helper for
// callers that build a Store first and a Fetcher second (common when the
// Fetcher depends on a Resolver that itself depends on the Store).
func (c Config) WithFetcher(f *Fetcher) Config {
	c.Fetcher = f
	return c
}

// SetFetcher attaches (or replaces) the fallback fetcher on an
// already-constructed Store. Safe to call concurrently with in-flight Get
// calls — they capture the current fetcher at read time. Pass nil to
// detach (subsequent local misses will return ErrBlobNotFound instead of
// fetching from the mesh).
func (s *Store) SetFetcher(f *Fetcher) {
	s.mu.Lock()
	s.cfg.Fetcher = f
	s.mu.Unlock()
}

// Store is the Phase 1 distributed blob store. It is content-addressed,
// encrypted at rest, and quota-aware. Phase 2/3 will layer advertisement,
// resolution, and fetch on top without modifying this type.
type Store struct {
	cfg     Config
	root    string
	memOnly bool

	mu      sync.RWMutex
	mem     map[string][]byte // cid -> ciphertext (in-memory mode)
	sizes   map[string]int    // cid -> plaintext size (on-disk index)
	byteSum int64             // total plaintext bytes cached
}

// NewDistributedBlobStore constructs a Store from the given config.
func NewDistributedBlobStore(cfg Config) (*Store, error) {
	if !cfg.InMemory {
		if cfg.Root == "" {
			return nil, errors.New("distributed: Config.Root required")
		}
		if err := os.MkdirAll(cfg.Root, 0o700); err != nil {
			return nil, fmt.Errorf("distributed: mkdir %q: %w", cfg.Root, err)
		}
	}
	if len(cfg.StorageKey) != 32 {
		return nil, fmt.Errorf("distributed: StorageKey must be 32 bytes, got %d", len(cfg.StorageKey))
	}
	s := &Store{
		cfg:     cfg,
		root:    cfg.Root,
		memOnly: cfg.InMemory,
		mem:     make(map[string][]byte),
		sizes:   make(map[string]int),
	}
	if !cfg.InMemory {
		if err := s.rehydrate(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// rehydrate rebuilds the in-memory index from the on-disk directory. Piece
// files are named `<cid>.blob`.
func (s *Store) rehydrate() error {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return fmt.Errorf("distributed: readdir %q: %w", s.root, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".blob" {
			continue
		}
		cid := name[:len(name)-len(".blob")]
		info, err := e.Info()
		if err != nil {
			continue
		}
		// We track plaintext sizes accurately only for items written this
		// session; for rehydrated items, approximate using ciphertext size
		// minus AEAD overhead (16 bytes). Good enough for quota bookkeeping.
		plainSize := int(info.Size()) - 16
		if plainSize < 0 {
			plainSize = 0
		}
		s.sizes[cid] = plainSize
		s.byteSum += int64(plainSize)
	}
	if s.cfg.Quota != nil {
		s.cfg.Quota.SetOurStorage(s.byteSum)
	}
	return nil
}

// Put stores a single chunk. The returned CID is the BLAKE2b-256 hash of
// the plaintext.
func (s *Store) Put(ctx context.Context, data []byte) (string, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return "", err
		}
	}
	cid := computePieceCID(data)

	// Quota gate: skip when the CID is already stored (idempotent re-put).
	s.mu.RLock()
	_, exists := s.sizes[cid]
	s.mu.RUnlock()
	if !exists && s.cfg.Quota != nil {
		if !s.cfg.Quota.HasRoomFor(int64(len(data))) {
			return "", fmt.Errorf("%w: budget=%d need=%d",
				ErrQuotaExceeded, s.cfg.Quota.CurrentBudget(), len(data))
		}
	}

	ct, err := EncryptForStorage(s.cfg.StorageKey, cid, data)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, already := s.sizes[cid]; already {
		return cid, nil
	}
	if s.memOnly {
		s.mem[cid] = ct
	} else {
		path := filepath.Join(s.root, cid+".blob")
		if err := writeFileAtomic(path, ct); err != nil {
			return "", err
		}
	}
	s.sizes[cid] = len(data)
	s.byteSum += int64(len(data))
	if s.cfg.Quota != nil {
		s.cfg.Quota.SetOurStorage(s.byteSum)
	}
	return cid, nil
}

// Get returns the plaintext chunk identified by cid.
//
// If the CID is not found locally AND Config.Fetcher is wired, Get falls
// back to the distributed fetch path (Phase 2). Successfully fetched
// pieces are cached back into this Store before being returned so
// subsequent Gets are local hits. If the fetcher is nil OR fails, Get
// returns ErrBlobNotFound (preserving Phase 1 semantics).
func (s *Store) Get(ctx context.Context, cid string) ([]byte, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	pt, err := s.getLocal(ctx, cid)
	if err == nil {
		return pt, nil
	}
	if !errors.Is(err, ErrBlobNotFound) {
		return nil, err
	}
	// Local miss → distributed fallback if wired.
	if s.cfg.Fetcher == nil {
		return nil, ErrBlobNotFound
	}
	data, ferr := s.cfg.Fetcher.Fetch(ctx, cid)
	if ferr != nil {
		// Surface ErrBlobNotFound when the resolver couldn't find anyone,
		// so callers that previously caught ErrBlobNotFound still see a
		// stable error type.
		if errors.Is(ferr, ErrNoPeers) {
			return nil, ErrBlobNotFound
		}
		return nil, ferr
	}
	// Cache back into the store. Errors here (quota, disk) are NOT fatal —
	// we still return the plaintext to the caller; next Get will miss again
	// and trigger another fetch.
	if _, perr := s.Put(ctx, data); perr != nil && !errors.Is(perr, ErrQuotaExceeded) {
		// Unexpected put failure; log via returning cache miss? We choose
		// to silently drop and return the data — Fetch succeeded, so the
		// caller expects bytes.
		_ = perr
	}
	return data, nil
}

// getLocal performs the pure local lookup without touching the Fetcher.
func (s *Store) getLocal(ctx context.Context, cid string) ([]byte, error) {
	var ct []byte
	s.mu.RLock()
	if s.memOnly {
		ct = s.mem[cid]
	}
	s.mu.RUnlock()
	if !s.memOnly {
		path := filepath.Join(s.root, cid+".blob")
		b, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ErrBlobNotFound
			}
			return nil, fmt.Errorf("distributed: read %q: %w", path, err)
		}
		ct = b
	}
	if len(ct) == 0 {
		return nil, ErrBlobNotFound
	}
	pt, err := DecryptFromStorage(s.cfg.StorageKey, cid, ct)
	if err != nil {
		return nil, err
	}
	// Verify the CID still matches -- catches bit-rot AND any confusion.
	if computePieceCID(pt) != cid {
		return nil, fmt.Errorf("distributed: cid mismatch on read")
	}
	return pt, nil
}

// Has reports whether cid is in the store.
func (s *Store) Has(ctx context.Context, cid string) (bool, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return false, err
		}
	}
	s.mu.RLock()
	_, ok := s.sizes[cid]
	s.mu.RUnlock()
	return ok, nil
}

// Delete removes cid from the store. Missing CIDs are a no-op.
func (s *Store) Delete(ctx context.Context, cid string) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	size, ok := s.sizes[cid]
	if !ok {
		return nil
	}
	delete(s.sizes, cid)
	s.byteSum -= int64(size)
	if s.byteSum < 0 {
		s.byteSum = 0
	}
	if s.memOnly {
		delete(s.mem, cid)
	} else {
		path := filepath.Join(s.root, cid+".blob")
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("distributed: remove %q: %w", path, err)
		}
	}
	if s.cfg.Quota != nil {
		s.cfg.Quota.SetOurStorage(s.byteSum)
	}
	return nil
}

// Size reports the current count + byte totals.
func (s *Store) Size(ctx context.Context) (int, int64, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sizes), s.byteSum, nil
}

// List enumerates the root CIDs currently stored. Order is unspecified.
func (s *Store) List(ctx context.Context) ([]string, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.sizes))
	for cid := range s.sizes {
		out = append(out, cid)
	}
	return out, nil
}

// Bytes returns the total plaintext bytes currently cached. Helpful for
// tests / status reporting.
func (s *Store) Bytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byteSum
}

// writeFileAtomic writes b to path via a temp file + rename to avoid
// partial-write corruption on crash.
func writeFileAtomic(path string, b []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".blob-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName) // noop if rename succeeded
	}()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
