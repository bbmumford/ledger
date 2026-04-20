/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// RecordType classifies the kind of signed mesh record a blob is attached
// to. Used for per-type size limits.
type RecordType string

const (
	RecordTypeListing  RecordType = "listing"
	RecordTypeMessage  RecordType = "message"
	RecordTypeEvidence RecordType = "evidence"
	RecordTypeAvatar   RecordType = "avatar"
	RecordTypeUnknown  RecordType = "unknown"
)

// PerTypeLimits captures per-record-type byte caps.
type PerTypeLimits struct {
	Listing  int64
	Message  int64
	Evidence int64
	Avatar   int64
	// Unknown is the cap when no specific record-type matches. Leave 0
	// to reject records of unknown type entirely.
	Unknown int64
}

// DefaultPerTypeLimits returns the Phase 1 defaults:
// listings 50 MB, messages 100 MB, evidence 20 MB, avatar 2 MB.
func DefaultPerTypeLimits() PerTypeLimits {
	return PerTypeLimits{
		Listing:  50 * 1024 * 1024,  // 50 MB
		Message:  100 * 1024 * 1024, // 100 MB
		Evidence: 20 * 1024 * 1024,  // 20 MB
		Avatar:   2 * 1024 * 1024,   // 2 MB
	}
}

// Cap returns the byte cap for a given record type.
func (l PerTypeLimits) Cap(t RecordType) int64 {
	switch t {
	case RecordTypeListing:
		return l.Listing
	case RecordTypeMessage:
		return l.Message
	case RecordTypeEvidence:
		return l.Evidence
	case RecordTypeAvatar:
		return l.Avatar
	default:
		return l.Unknown
	}
}

// ReferenceSource is the consumer-provided hook that tells the blob store
// which manifests are still referenced by live mesh records. Mercury's
// StoreManager implements this in Phase 3.
type ReferenceSource interface {
	// IsReferenced reports whether at least one live, signed record
	// references the given rootCID.
	IsReferenced(ctx context.Context, rootCID string) (bool, error)
}

// Errors surfaced by ValidateStorable and GarbageCollect.
var (
	ErrNoReference     = errors.New("distributed: manifest references no live record")
	ErrPerPeerExceeded = errors.New("distributed: per-uploader quota exceeded")
	ErrPerTypeExceeded = errors.New("distributed: per-record-type size exceeded")
	ErrBadUploaderSig  = errors.New("distributed: bad uploader signature")

	// ErrAlreadyHeld is a benign sentinel returned by Check when the CID is
	// already accounted for under this guard. Callers (typically the blob
	// replicator) should treat this as a no-op success and avoid re-storing
	// the bytes.
	ErrAlreadyHeld = errors.New("distributed: cid already held")

	// ErrReplicationQuotaExceeded is returned by Check when the aggregate
	// replication cap (MaxReplicationTotalBytes) would be exceeded.
	ErrReplicationQuotaExceeded = errors.New("distributed: replication quota exceeded")

	// ErrPeerQuotaExceeded is returned by Check when the per-source-peer
	// replication cap (MaxReplicationBytesPerPeer) would be exceeded.
	ErrPeerQuotaExceeded = errors.New("distributed: per-peer replication quota exceeded")

	// ErrRecordTypeNotAllowed is returned by Check when the record type is
	// not on the configured allow-list.
	ErrRecordTypeNotAllowed = errors.New("distributed: record type not allowed")
)

// RecordTypeReplication is the default record-type label for opportunistic
// replication fetches — pieces discovered via have-ads that carry no
// application-level classification.
const RecordTypeReplication RecordType = "replication"

// DefaultReplicationAllowedRecordTypes is the initial set of record types
// an AbuseGuard accepts on the Check() path when no explicit allow-list is
// configured. Mirrors the Mercury DESIGN §19 surface: opportunistic
// replication plus the three record-kinds that currently publish blobs.
var DefaultReplicationAllowedRecordTypes = []RecordType{
	RecordTypeReplication,
	RecordTypeListing,
	"attachment",
	"dispute",
}

// DefaultOrphanGrace is the grace period before an unreferenced manifest
// becomes eligible for garbage collection.
const DefaultOrphanGrace = 24 * time.Hour

// AbuseGuard enforces anti-bloat rules and orchestrates garbage collection
// of orphaned blobs.
//
// It carries two logically independent ledgers:
//
//  1. The uploader ledger (perUploader) tracks bytes charged to
//     signed-manifest uploads that came through ValidateStorable. Entries
//     here are keyed by ed25519 public key.
//
//  2. The replication ledger (replBytesPerPeer / replBytesTotal /
//     replCIDs) tracks bytes accepted via Check() — opportunistic
//     replicator fetches that do NOT carry an uploader signature. Entries
//     here are keyed by the source-peer wire-ID string.
//
// The two ledgers are intentionally separate so that a malicious peer
// cannot fill its uploader quota by re-advertising pieces it merely
// happened to have cached, nor can a legitimate uploader be starved by
// replicator traffic.
type AbuseGuard struct {
	refs        ReferenceSource
	limits      PerTypeLimits
	perPeerCap  int64
	orphanGrace time.Duration

	// Replication-path config (opportunistic fetchers).
	replMaxBytesPerPeer int64
	replMaxBytesTotal   int64
	replTypeLimits      map[RecordType]int64
	replAllowedTypes    map[RecordType]struct{}

	mu          sync.Mutex
	perUploader map[string]int64     // hex(pubkey) -> bytes (uploader path)
	firstSeen   map[string]time.Time // rootCID -> first time we saw it orphaned

	// Replication-path ledger. Kept distinct from perUploader so upload
	// and opportunistic caching quotas never cross-contaminate.
	replBytesPerPeer map[string]int64 // peerID -> bytes accepted via Check
	replBytesTotal   int64            // aggregate bytes accepted via Check
	replCIDs         map[string]int64 // cid -> size, for dedup + refund
}

// AbuseGuardConfig is the constructor input for AbuseGuard.
type AbuseGuardConfig struct {
	References  ReferenceSource
	Limits      PerTypeLimits
	PerPeerCap  int64         // max aggregate bytes per uploader (0 = unlimited)
	OrphanGrace time.Duration // 0 -> DefaultOrphanGrace

	// --- Replication path (Check) ----------------------------------------

	// MaxReplicationBytesPerPeer caps the bytes any single source peer can
	// contribute to the opportunistic replication cache. 0 disables the
	// per-peer cap (not recommended in production).
	MaxReplicationBytesPerPeer int64

	// MaxReplicationBytesTotal caps the aggregate bytes stored via the
	// opportunistic replication path across ALL source peers. 0 disables
	// the aggregate cap.
	MaxReplicationBytesTotal int64

	// ReplicationRecordTypeLimits lets callers override the per-record-type
	// byte ceiling on the replication path. A type absent from the map
	// falls back to MaxReplicationBytesPerPeer.
	ReplicationRecordTypeLimits map[RecordType]int64

	// ReplicationAllowedRecordTypes restricts which record types the Check
	// admission gate will accept. Empty = use DefaultReplicationAllowedRecordTypes.
	ReplicationAllowedRecordTypes []RecordType
}

// NewAbuseGuard constructs an AbuseGuard. A nil ReferenceSource disables
// reference checks (useful for tests and self-hosted content).
func NewAbuseGuard(cfg AbuseGuardConfig) *AbuseGuard {
	if (cfg.Limits == PerTypeLimits{}) {
		cfg.Limits = DefaultPerTypeLimits()
	}
	grace := cfg.OrphanGrace
	if grace <= 0 {
		grace = DefaultOrphanGrace
	}
	allowed := cfg.ReplicationAllowedRecordTypes
	if len(allowed) == 0 {
		allowed = DefaultReplicationAllowedRecordTypes
	}
	allowSet := make(map[RecordType]struct{}, len(allowed))
	for _, t := range allowed {
		allowSet[t] = struct{}{}
	}
	typeLimits := make(map[RecordType]int64, len(cfg.ReplicationRecordTypeLimits))
	for k, v := range cfg.ReplicationRecordTypeLimits {
		typeLimits[k] = v
	}
	return &AbuseGuard{
		refs:                cfg.References,
		limits:              cfg.Limits,
		perPeerCap:          cfg.PerPeerCap,
		orphanGrace:         grace,
		replMaxBytesPerPeer: cfg.MaxReplicationBytesPerPeer,
		replMaxBytesTotal:   cfg.MaxReplicationBytesTotal,
		replTypeLimits:      typeLimits,
		replAllowedTypes:    allowSet,
		perUploader:         make(map[string]int64),
		firstSeen:           make(map[string]time.Time),
		replBytesPerPeer:    make(map[string]int64),
		replCIDs:            make(map[string]int64),
	}
}

// ValidateStorable enforces the anti-bloat gates documented in the roadmap:
//   - valid uploader signature over the manifest
//   - reference validation (the manifest must be referenced on-mesh)
//   - per-uploader quota
//   - per-record-type size limits
func (a *AbuseGuard) ValidateStorable(
	ctx context.Context,
	m *Manifest,
	uploaderSig []byte,
	uploaderPub ed25519.PublicKey,
	recordType RecordType,
) error {
	if m == nil {
		return errors.New("distributed: nil manifest")
	}
	if err := ValidateManifest(m, uploaderSig, uploaderPub); err != nil {
		if errors.Is(err, ErrInvalidManifest) {
			return fmt.Errorf("%w: %v", ErrBadUploaderSig, err)
		}
		return err
	}
	// Per-record-type size cap.
	cap := a.limits.Cap(recordType)
	if cap > 0 && m.TotalBytes > cap {
		return fmt.Errorf("%w: type=%s size=%d cap=%d",
			ErrPerTypeExceeded, recordType, m.TotalBytes, cap)
	} else if cap <= 0 && recordType == RecordTypeUnknown {
		return fmt.Errorf("%w: type=%s has no cap", ErrPerTypeExceeded, recordType)
	}
	// Reference validation.
	if a.refs != nil {
		ok, err := a.refs.IsReferenced(ctx, m.RootCID)
		if err != nil {
			return fmt.Errorf("distributed: reference check: %w", err)
		}
		if !ok {
			return fmt.Errorf("%w: rootCID=%s", ErrNoReference, m.RootCID)
		}
	}
	// Per-uploader quota.
	if a.perPeerCap > 0 {
		key := uploaderKey(uploaderPub)
		a.mu.Lock()
		cur := a.perUploader[key]
		a.mu.Unlock()
		if cur+m.TotalBytes > a.perPeerCap {
			return fmt.Errorf("%w: uploader=%s current=%d new=%d cap=%d",
				ErrPerPeerExceeded, key, cur, m.TotalBytes, a.perPeerCap)
		}
	}
	return nil
}

// ChargeUploader records uploaded bytes against an uploader's per-peer
// quota. Call AFTER a successful store.
func (a *AbuseGuard) ChargeUploader(uploader ed25519.PublicKey, bytes int64) {
	a.mu.Lock()
	a.perUploader[uploaderKey(uploader)] += bytes
	a.mu.Unlock()
}

// RefundUploader reverses ChargeUploader on delete / GC.
func (a *AbuseGuard) RefundUploader(uploader ed25519.PublicKey, bytes int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := uploaderKey(uploader)
	a.perUploader[key] -= bytes
	if a.perUploader[key] < 0 {
		a.perUploader[key] = 0
	}
}

// UploaderUsage returns the bytes currently charged to an uploader.
func (a *AbuseGuard) UploaderUsage(uploader ed25519.PublicKey) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.perUploader[uploaderKey(uploader)]
}

// GCCandidate is a candidate returned by ListOrphans.
type GCCandidate struct {
	RootCID   string
	FirstSeen time.Time
}

// GarbageCollect scans `manifests` and returns the root CIDs that are
// orphaned and have been observed as orphaned for at least OrphanGrace.
// The caller is responsible for actually deleting the associated bytes
// (the GC function itself has no side effects on storage).
//
// The AbuseGuard records first-seen timestamps internally -- call this
// once per sweep interval to accumulate "age" for each candidate.
func (a *AbuseGuard) GarbageCollect(ctx context.Context, rootCIDs []string) ([]string, error) {
	if a.refs == nil {
		// Without a reference source we can't classify anything as orphaned.
		return nil, nil
	}
	now := time.Now()
	var out []string
	a.mu.Lock()
	// Prune firstSeen entries that no longer appear in the input list so
	// the map doesn't grow unbounded. We'll rebuild what's still relevant
	// below.
	keep := make(map[string]time.Time, len(a.firstSeen))
	for _, cid := range rootCIDs {
		if t, ok := a.firstSeen[cid]; ok {
			keep[cid] = t
		}
	}
	a.firstSeen = keep
	a.mu.Unlock()

	for _, cid := range rootCIDs {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		ok, err := a.refs.IsReferenced(ctx, cid)
		if err != nil {
			return out, fmt.Errorf("distributed: gc reference check: %w", err)
		}
		if ok {
			a.mu.Lock()
			delete(a.firstSeen, cid)
			a.mu.Unlock()
			continue
		}
		// Orphan. Record first-seen or check grace.
		a.mu.Lock()
		first, seen := a.firstSeen[cid]
		if !seen {
			a.firstSeen[cid] = now
			a.mu.Unlock()
			continue
		}
		a.mu.Unlock()
		if now.Sub(first) >= a.orphanGrace {
			out = append(out, cid)
		}
	}
	return out, nil
}

// ForgetOrphan drops any bookkeeping for the given rootCID. Call after
// actually deleting from storage.
func (a *AbuseGuard) ForgetOrphan(rootCID string) {
	a.mu.Lock()
	delete(a.firstSeen, rootCID)
	a.mu.Unlock()
}

func uploaderKey(pub ed25519.PublicKey) string {
	return hex.EncodeToString(pub)
}

// Check is the lightweight admission gate used by opportunistic fetchers
// (replicators) that have a CID + bytes but no signed manifest. It applies
// a distinct "replication" quota separate from the uploader quota applied
// by ValidateStorable.
//
// recordType is one of: "replication" (default for opportunistic), or an
// application-supplied type if the caller has more context (e.g. a listing
// replica explicitly carries "listing").
//
// Returns:
//   - ErrAlreadyHeld when the CID is already charged to this guard
//     (caller should treat as a benign no-op and skip Put).
//   - ErrRecordTypeNotAllowed when recordType is not on the allow-list.
//   - ErrPerTypeExceeded when size exceeds the configured per-type cap.
//   - ErrPeerQuotaExceeded when the single source peer would exceed its cap.
//   - ErrReplicationQuotaExceeded when the aggregate cache would exceed its cap.
//   - nil on success — the caller SHOULD proceed to Put the bytes. The
//     guard has already recorded the charge.
//
// Check is designed for the hot path: it takes the internal lock exactly
// once and allocates nothing beyond the map upserts.
//
// The replication ledger is kept purely in memory for this pass; a
// restart resets the counters. Persistent counters are a follow-up.
func (a *AbuseGuard) Check(
	ctx context.Context,
	cid string,
	fromPeerID string,
	size int64,
	recordType RecordType,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cid == "" {
		return errors.New("distributed: Check requires a non-empty cid")
	}
	if size < 0 {
		return fmt.Errorf("distributed: Check size must be non-negative, got %d", size)
	}
	if recordType == "" {
		recordType = RecordTypeReplication
	}

	// Allow-list check happens BEFORE taking the lock — cheap rejection.
	if len(a.replAllowedTypes) > 0 {
		if _, ok := a.replAllowedTypes[recordType]; !ok {
			return fmt.Errorf("%w: type=%s", ErrRecordTypeNotAllowed, recordType)
		}
	}

	// Per-type size cap — use the replication-specific override if one
	// is configured, otherwise fall back to the per-peer cap as a
	// sensible default ceiling.
	if typeCap, ok := a.replTypeLimits[recordType]; ok && typeCap > 0 && size > typeCap {
		return fmt.Errorf("%w: type=%s size=%d cap=%d",
			ErrPerTypeExceeded, recordType, size, typeCap)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Dedup: if we already charged this CID, caller should treat as benign.
	if _, held := a.replCIDs[cid]; held {
		return ErrAlreadyHeld
	}

	// Aggregate cap.
	if a.replMaxBytesTotal > 0 && a.replBytesTotal+size > a.replMaxBytesTotal {
		return fmt.Errorf("%w: current=%d new=%d cap=%d",
			ErrReplicationQuotaExceeded, a.replBytesTotal, size, a.replMaxBytesTotal)
	}

	// Per-peer cap.
	cur := a.replBytesPerPeer[fromPeerID]
	if a.replMaxBytesPerPeer > 0 && cur+size > a.replMaxBytesPerPeer {
		return fmt.Errorf("%w: peer=%s current=%d new=%d cap=%d",
			ErrPeerQuotaExceeded, fromPeerID, cur, size, a.replMaxBytesPerPeer)
	}

	// Charge.
	a.replBytesPerPeer[fromPeerID] = cur + size
	a.replBytesTotal += size
	a.replCIDs[cid] = size
	return nil
}

// ReleaseReplication refunds a previously-accepted replication charge.
// Call after deleting a CID that was admitted via Check. Safe to call with
// an unknown CID (no-op).
func (a *AbuseGuard) ReleaseReplication(cid, fromPeerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	size, held := a.replCIDs[cid]
	if !held {
		return
	}
	delete(a.replCIDs, cid)
	a.replBytesTotal -= size
	if a.replBytesTotal < 0 {
		a.replBytesTotal = 0
	}
	cur := a.replBytesPerPeer[fromPeerID]
	cur -= size
	if cur <= 0 {
		delete(a.replBytesPerPeer, fromPeerID)
	} else {
		a.replBytesPerPeer[fromPeerID] = cur
	}
}

// ReplicationUsage returns the bytes currently charged to a source peer
// via the replication path. Intended for observability / tests.
func (a *AbuseGuard) ReplicationUsage(fromPeerID string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.replBytesPerPeer[fromPeerID]
}

// ReplicationTotal returns the aggregate replication bytes charged
// across all peers.
func (a *AbuseGuard) ReplicationTotal() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.replBytesTotal
}
