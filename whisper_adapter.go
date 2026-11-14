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
//
// Fingerprint, BucketDigest and RecordsInBuckets summarise and serve the
// cache's records through a whisper.RecordSet built from Dump on each call,
// keyed and summarised by Record.ContentHash. The content hash leaves out the
// signature and the receive time, so two caches holding the same records
// agree even though DirectoryCache.Dump re-signs every record with the local
// node's key, and a record replaced under the same node and topic changes
// the summary. The set is rebuilt rather than kept because records reach the
// cache by paths this adapter never sees (DirectoryCache.ApplyLocal, the
// expiry sweep, tombstoning).
type CacheStateStore struct {
	cache interface {
		Dump() []Record
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
		Dump() []Record
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

// recordSet is the cache's current records as a whisper.RecordSet, each
// under its content hash. A record that fails to marshal is left out so a
// single malformed entry does not starve the rest; it is then also left out
// of the summary, so Fingerprint describes exactly what Snapshot serves.
func (s *CacheStateStore) recordSet() *whisper.RecordSet {
	set := whisper.NewRecordSet()
	for _, rec := range s.cache.Dump() {
		data, err := s.marshal(rec)
		if err != nil {
			continue
		}
		h := rec.ContentHash()
		set.PutContent(string(h[:]), data, h[:])
	}
	return set
}

// Fingerprint summarises the content of every record the cache holds; 0
// when it holds none.
func (s *CacheStateStore) Fingerprint() uint64 {
	return s.recordSet().Fingerprint()
}

// Snapshot returns every live record as opaque bytes.
func (s *CacheStateStore) Snapshot() [][]byte {
	return s.recordSet().Snapshot()
}

// Delta returns the snapshot. DirectoryCache.DumpSince keeps a record only
// when a time the record carries (a member's CreatedAt, a role's Updated)
// is after since, while whisper passes the local time of the last exchange
// with the peer, so a record created before that exchange but received
// after it would never be sent.
func (s *CacheStateStore) Delta(_ time.Time) [][]byte {
	return s.Snapshot()
}

// BucketDigest partitions the records Fingerprint summarises into buckets,
// so a mismatch narrows to the buckets that differ.
func (s *CacheStateStore) BucketDigest(buckets uint16) []uint64 {
	return s.recordSet().BucketDigest(buckets)
}

// RecordsInBuckets serves the records of the buckets a peer differs on.
func (s *CacheStateStore) RecordsInBuckets(buckets uint16, want []bool) [][]byte {
	return s.recordSet().RecordsInBuckets(buckets, want)
}

// Apply ingests a single inbound record from gossip or rumor push.
func (s *CacheStateStore) Apply(data []byte) error {
	rec, err := s.unmarshal(data)
	if err != nil {
		return err
	}
	return s.cache.Apply(rec)
}

// Verify interface compliance.
var (
	_ whisper.StateStore                = (*CacheStateStore)(nil)
	_ whisper.BucketFingerprintProvider = (*CacheStateStore)(nil)
	_ whisper.BucketRecordsProvider     = (*CacheStateStore)(nil)
)

// RecordFingerprint computes an FNV-1a fingerprint of a record for dedup.
func RecordFingerprint(rec Record) uint64 {
	h := fnv.New64a()
	h.Write([]byte(rec.Topic))
	h.Write([]byte(rec.NodeID))
	h.Write([]byte(rec.Partition))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(rec.Timestamp.UnixNano()))
	h.Write(buf[:])
	return h.Sum64()
}
