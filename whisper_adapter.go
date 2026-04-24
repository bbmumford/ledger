/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"encoding/binary"
	"hash/fnv"
	"time"

	"github.com/bbmumford/whisper"
)

// CacheStateStore wraps a DirectoryCache-like store to implement
// whisper.StateStore so the native G1 exchange handler can drive
// delta-sync and full-sync transfers without knowing about Record
// structure. Records travel as opaque bytes; the supplied marshal /
// unmarshal funcs bridge Record↔wire format at the adapter boundary.
type CacheStateStore struct {
	cache interface {
		Fingerprint() uint64
		Dump() []Record
		DumpSince(since time.Time) []Record
		Apply(rec Record) error
	}
	marshal   func(Record) ([]byte, error)
	unmarshal func([]byte) (Record, error)
}

// NewCacheStateStore returns a StateStore adapter suitable for
// whisper.WithG1Store. marshal and unmarshal are called once per
// record transferred; callers typically use the same codec that
// their G1Codec uses on the envelope boundary.
func NewCacheStateStore(
	cache interface {
		Fingerprint() uint64
		Dump() []Record
		DumpSince(since time.Time) []Record
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

// Fingerprint returns the cache's order-independent fingerprint for
// G2 digest probes.
func (s *CacheStateStore) Fingerprint() uint64 {
	return s.cache.Fingerprint()
}

// Snapshot returns every live record as opaque bytes. Records that
// fail to marshal are dropped so a single malformed entry doesn't
// starve the rest of the payload.
func (s *CacheStateStore) Snapshot() [][]byte {
	records := s.cache.Dump()
	return s.marshalBatch(records)
}

// Delta returns records mutated since the given time. A zero time
// falls back to Snapshot so whisper's handler never has to special-
// case the bootstrap path.
func (s *CacheStateStore) Delta(since time.Time) [][]byte {
	if since.IsZero() {
		return s.Snapshot()
	}
	records := s.cache.DumpSince(since)
	return s.marshalBatch(records)
}

// Apply ingests a single inbound record from gossip or rumor push.
func (s *CacheStateStore) Apply(data []byte) error {
	rec, err := s.unmarshal(data)
	if err != nil {
		return err
	}
	return s.cache.Apply(rec)
}

// marshalBatch serialises a slice of records, skipping any that
// fail to marshal so one bad record doesn't block the batch.
func (s *CacheStateStore) marshalBatch(records []Record) [][]byte {
	out := make([][]byte, 0, len(records))
	for _, rec := range records {
		data, err := s.marshal(rec)
		if err != nil {
			continue
		}
		out = append(out, data)
	}
	return out
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
