/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/bbmumford/whisper"
)

// CacheStateStore wraps a DirectoryCache to implement whisper.StateStore.
// This allows the Whisper gossip engine to read/write cache state for
// delta sync without knowing about Ledger record structure.
type CacheStateStore struct {
	cache interface {
		Fingerprint() uint64
		DumpSince(since uint64) ([]Record, uint64)
		Apply(rec Record) error
	}
	marshal   func(Record) ([]byte, error)
	unmarshal func([]byte) (Record, error)
}

// NewCacheStateStore creates a StateStore adapter.
// marshal/unmarshal convert between Record and opaque bytes for gossip transport.
func NewCacheStateStore(
	cache interface {
		Fingerprint() uint64
		DumpSince(since uint64) ([]Record, uint64)
		Apply(rec Record) error
	},
	marshal func(Record) ([]byte, error),
	unmarshal func([]byte) (Record, error),
) *CacheStateStore {
	return &CacheStateStore{
		cache:     cache,
		marshal:   marshal,
		unmarshal: unmarshal,
	}
}

// Fingerprint returns the cache fingerprint for delta-sync optimisation.
func (s *CacheStateStore) Fingerprint(topic string) uint64 {
	return s.cache.Fingerprint()
}

// DeltaSince returns records changed since the given watermark as opaque bytes.
func (s *CacheStateStore) DeltaSince(topic string, since uint64) ([][]byte, uint64, error) {
	records, watermark := s.cache.DumpSince(since)
	result := make([][]byte, 0, len(records))
	for _, rec := range records {
		if string(rec.Topic) != topic && topic != "" {
			continue
		}
		data, err := s.marshal(rec)
		if err != nil {
			continue
		}
		result = append(result, data)
	}
	return result, watermark, nil
}

// Apply stores an incoming record from gossip.
func (s *CacheStateStore) Apply(topic string, data []byte) error {
	rec, err := s.unmarshal(data)
	if err != nil {
		return err
	}
	return s.cache.Apply(rec)
}

// Verify interface compliance.
var _ whisper.StateStore = (*CacheStateStore)(nil)

// RecordFingerprint computes an FNV-1a fingerprint of a record for dedup.
func RecordFingerprint(rec Record) uint64 {
	h := fnv.New64a()
	h.Write([]byte(rec.Topic))
	h.Write([]byte(rec.NodeID))
	h.Write([]byte(rec.TenantID))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(rec.Timestamp.UnixNano()))
	h.Write(buf[:])
	return h.Sum64()
}
