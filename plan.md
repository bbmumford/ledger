# Ledger — Extraction + Agnostic Redesign Plan

> Extract `mesh/core/directory/` from HSTLES Library into `github.com/bbmumford/ledger`, then redesign it to serve ALL consumers (HSTLES/ORBTR, Hospitium, Mercury) with pluggable merge functions, flexible keys, and HLC support.

---

## 1. What Gets Extracted

| Current Path | Ledger Path | Description |
|---|---|---|
| `directory/types.go` | `ledger/types.go` | Record, Topic, MemberRecord, RoleRecord, ReachRecord, LatencyRecord |
| `directory/doc.go` | `ledger/doc.go` | Package documentation |
| `directory/cache/directory.go` | `ledger/cache/directory.go` | DirectoryCache materialised view |
| `directory/cache/store.go` | `ledger/cache/store.go` | CacheStore interface |
| `directory/cache/store_memory.go` | `ledger/cache/memory.go` | In-memory CacheStore |
| `directory/ledger/ledger.go` | `ledger/storage/ledger.go` | Ledger interface + subscriber broadcast |
| `directory/ledger/memory_ledger.go` | `ledger/storage/memory.go` | In-memory Ledger |
| `directory/ledger/local_ledger.go` | `ledger/storage/local.go` | Local libSQL persistence + bloom filter |
| `directory/anchor/` | `ledger/anchor/` | Signed snapshot generation + verification |
| (new from Step 1 Task 11) | `ledger/pb/record.proto` | `LADRecord` protobuf message definition — the wire format for records in gossip. Created during Step 1 as part of `gossip.proto`, split out during extraction: Aether keeps the envelope, Ledger keeps the record definition. |

## 2. What Stays in HSTLES

| Path | Reason |
|---|---|
| `directory/ledger/mesh_ledger.go` | Depends on `tursoraft` + `envreq` (HSTLES infrastructure) |
| `directory/gossip/protocol.go` | LAD-specific gossip envelope (implements Aether GossipPayload) |
| `directory/gossip/peer.go` | Legacy — DELETE |
| `directory/gossip/ratelimit.go` | Legacy — DELETE |
| `directory/gossip/discovery.go` | Legacy — DELETE |
| `directory/gossip/adaptive.go` | Legacy — replaced by Task 10a HWP AdaptiveInterval |

## 3. What Moves to Aether (not Ledger)

The generic gossip engine files move to `aether/gossip/`, not Ledger:
- `gossip/sync.go` (G1 wire framing, parameterised payload)
- `gossip/hwp_gossip.go` (HWP stream plumbing)
- `gossip/delta.go`, `digest.go`, `hypercube.go`, `pex.go`, `signaling.go`, `rumor.go`, `backpressure.go`, `rtt.go`

These are transport-level. Ledger implements the `GossipPayload` interface they define.

## 4. Gossip Adapter (stays in HSTLES, wraps Ledger + Aether)

After extraction, HSTLES keeps a thin gossip adapter that:
1. Implements `aether.GossipPayload` for `[]ledger.Record`
2. Connects Aether's gossip engine to Ledger's cache
3. Handles LAD-specific concerns (fingerprint, apply, tombstone)

```go
// mesh/core/directory/gossip/lad_payload.go (stays in HSTLES)
type LADGossipPayload struct {
    cache   *ledger.DirectoryCache
    records []ledger.Record
}

func (p *LADGossipPayload) Marshal() ([]byte, error)  { return proto.Marshal(&pb.LADRecordList{Records: toProto(p.records)}) }
func (p *LADGossipPayload) Unmarshal(b []byte) error   { var list pb.LADRecordList; err := proto.Unmarshal(b, &list); p.records = fromProto(list.Records); return err }
func (p *LADGossipPayload) Fingerprint() uint64        { return p.cache.Fingerprint() }
```

> **Step 1 impact (Task 11):** Gossip encoding switches from JSON to protobuf. The `LADRecord` protobuf
> message definition lives in Ledger (`ledger/pb/record.proto`), not Aether. Aether's gossip envelope
> carries records as opaque `repeated bytes` — each byte slice is a proto-marshalled `LADRecord`.
> The LADGossipPayload handles the LADRecord proto marshal/unmarshal. Hospitium will define its own
> CRDT record proto and its own GossipPayload implementation that marshals CRDT deltas instead.

## 5. Agnostic Redesign (before or during extraction)

### 5.1 Pluggable Merge Functions

The core change. Each topic registers a merge function. Default is overwrite (backward compatible).

```go
// MergeFunc resolves conflicts between an existing record and an incoming record.
// Returns the winning record. Called by DirectoryCache.Apply().
type MergeFunc func(existing, incoming Record) (Record, error)

// Built-in merge functions:
func OverwriteMerge(existing, incoming Record) (Record, error) {
    // Latest timestamp or highest LamportClock wins. Current LAD behavior.
    if incoming.LamportClock > existing.LamportClock { return incoming, nil }
    if incoming.Timestamp.After(existing.Timestamp) { return incoming, nil }
    return existing, nil
}

// Registered by Hospitium for CRDT topics:
func LWWMapMerge(existing, incoming Record) (Record, error) {
    // Parse Body as LWW entries, merge per-field by HLC timestamp
    // Returns merged record with union of all fields, latest value per field
}

func ORSetMerge(existing, incoming Record) (Record, error) {
    // Parse Body as {adds: [], removes: []}, merge with observed-remove semantics
}

func CounterMerge(existing, incoming Record) (Record, error) {
    // Parse Body as per-node increment map, merge with max-per-node
}
```

**DirectoryCache changes:**

```go
type DirectoryCache struct {
    // ... existing fields
    merges   map[Topic]MergeFunc   // NEW: per-topic merge function
    keyFuncs map[Topic]KeyFunc     // NEW: per-topic key derivation
}

func (c *DirectoryCache) RegisterMerge(topic Topic, fn MergeFunc)
func (c *DirectoryCache) RegisterKey(topic Topic, fn KeyFunc)

// Apply now uses registered merge:
func (c *DirectoryCache) Apply(rec Record) error {
    key := c.deriveKey(rec)
    merge := c.merges[rec.Topic]
    if merge == nil { merge = OverwriteMerge }

    existing, exists := c.get(rec.Topic, key)
    if exists && !existing.Tombstone {
        merged, err := merge(existing, rec)
        if err != nil { return err }
        rec = merged
    }
    return c.store(rec.Topic, key, rec)
}
```

### 5.2 Flexible Key Derivation

Currently records are keyed by `topic + nodeID`. CRDTs need `topic + collection + key`.

```go
type KeyFunc func(Record) string

// Built-in key functions:
func NodeIDKey(r Record) string { return r.NodeID }  // HSTLES: member, role, reach
func BodyKey(r Record) string {                        // Hospitium: collection:key from Body
    var meta struct{ Collection, Key string }
    json.Unmarshal(r.Body, &meta)
    return meta.Collection + ":" + meta.Key
}
```

### 5.3 HLC Timestamp on Record

Add optional HLC field for CRDT consumers. Zero value = not used (backward compatible).

```go
type Record struct {
    Topic        Topic
    NodeID       string
    TenantID     string
    Body         json.RawMessage
    Timestamp    time.Time
    LamportClock uint64
    HLCTimestamp uint64         // NEW: hybrid logical clock (0 = unused)
    Seq          uint32
    Tombstone    bool
    ExpiresAt    time.Time
}
```

### 5.4 Generic Query API

Alongside the existing typed helpers (which stay for backward compatibility):

```go
// Generic query (any topic, any filter):
type QueryFilter struct {
    NodeID   string            // optional: filter by node
    KeyPrefix string           // optional: filter by key prefix
    Since    time.Time          // optional: records after this time
    Limit    int                // optional: max results
}

func (c *DirectoryCache) Query(ctx context.Context, topic Topic, filter QueryFilter) ([]Record, error)
func (c *DirectoryCache) Subscribe(topic Topic, handler func(Record))

// Existing typed helpers remain as sugar:
func (c *DirectoryCache) Members(...) ([]MemberRecord, error)  // calls Query("member", ...)
func (c *DirectoryCache) Roles(...) ([]RoleRecord, error)      // calls Query("role", ...)
```

### 5.5 Topic Registration

Consumers register their topics at startup. Ledger doesn't know about record type semantics — it just stores and merges.

```go
// HSTLES registers at node startup:
cache.RegisterMerge(TopicMember, OverwriteMerge)
cache.RegisterMerge(TopicRole, OverwriteMerge)
cache.RegisterMerge(TopicReach, OverwriteMerge)
cache.RegisterMerge(TopicLatency, OverwriteMerge)
cache.RegisterKey(TopicMember, NodeIDKey)
cache.RegisterKey(TopicRole, NodeIDKey)

// Hospitium registers at platform startup:
cache.RegisterMerge("crdt.lww_map", LWWMapMerge)
cache.RegisterMerge("crdt.or_set", ORSetMerge)
cache.RegisterMerge("crdt.counter", CounterMerge)
cache.RegisterKey("crdt.lww_map", BodyKey)
cache.RegisterKey("crdt.or_set", BodyKey)
cache.RegisterKey("crdt.counter", BodyKey)
```

### 5.6 What This Means for Each Consumer

| Consumer | Topics | Merge | Key | Typed Helpers |
|---|---|---|---|---|
| HSTLES/ORBTR | member, role, reach, latency | OverwriteMerge | NodeIDKey | Members(), Roles(), Reach() — existing API preserved |
| Hospitium | crdt.lww_map, crdt.or_set, crdt.counter | CRDT merge fns | BodyKey | Query() + Subscribe() — generic API |
| Mercury | Uses Hospitium topics | Via Hospitium | Via Hospitium | Via Hospitium SDK |

---

## 6. Migration Phases

### Phase 1: Extract + redesign types (2 days)
- Create `github.com/bbmumford/ledger`
- Move `types.go` — add `HLCTimestamp` field, `MergeFunc`, `KeyFunc`, `QueryFilter`
- Move `cache/` — add `RegisterMerge`, `RegisterKey`, `Query`, `Subscribe`, update `Apply`
- Move `anchor/`
- Create `ledger/pb/record.proto` — `LADRecord` protobuf message (split from Step 1 Task 11's `gossip.proto`). This is the wire format consumers use to serialise records for gossip transport.
- Default merge = `OverwriteMerge` (backward compatible)
- All existing HSTLES code works unchanged (defaults)
- Note: `cache/directory.go` will already have `maxLatencyRecords = 250` (from Step 1 Task 6) — no action needed, just carries forward

### Phase 2: Extract storage backends (1 day)
- Move `ledger/` (memory + local)
- Keep `mesh_ledger.go` in HSTLES (depends on private deps)
- Move `internal/bloom/` into ledger as utility

### Phase 3: Create Whisper adapter (1 day)
- Create `LADStateStore` in HSTLES that implements `whisper.StateStore` wrapping `DirectoryCache`
- Register HSTLES topics (`member`, `role`, `reach`, `latency`) with `whisper.Engine` as `StatefulMerge`
- Wire into existing `mesh_connection.go` gossip setup (renamed from hwp_connection.go in Aether migration)
- Whisper replaces the previous gossip approach — uses `aether.Stream` for transport

> **Post-Aether migration notes:**
> - HSTLES Library now imports `github.com/ORBTR/aether` instead of `mesh/core/transport`
> - Gossip functions renamed: `RunGossipLoop` (was RunHWPGossipLoop), `RunGossipLoopWithPeer`, `RunGossipResponder`
> - `AdaptiveInterval` (was AdaptiveHWPInterval) in `gossip/adaptive_interval.go`
> - `stream_gossip.go` (was hwp_gossip.go) wraps Aether streams for gossip
> - `mesh_connection.go` (was hwp_connection.go) manages mesh connections
> - Streams defined in `mesh/core/streams/` package (StreamGossip=0, StreamRPC=1, etc.)
> - Grade system in `mesh/grade/` package (shared by node + dispatch)
> - `aether.Connection` = raw transport, `aether.Session` = stream-multiplexed
> - `aether.SessionOptions` replaces GlobalKillSwitch (per-session feature flags)
> - `aether.StreamLayout` configures keepalive/control stream IDs
> - `aether.TransportCapabilities` replaces Grade at protocol level (Grade rebuilt in mesh/grade)

### Phase 3a: New Ledger Capabilities (5 days)

#### High Priority (all consumers benefit)

**1. TTL Expiration Worker** (1 day)
```go
// ExpiryWorker runs a background goroutine that prunes expired records.
type ExpiryWorker struct {
    cache    *DirectoryCache
    interval time.Duration  // default 60s
}

func NewExpiryWorker(cache *DirectoryCache, interval time.Duration) *ExpiryWorker
func (w *ExpiryWorker) Run(ctx context.Context)
func (w *ExpiryWorker) PruneExpired() (removed int, err error)
```
- Scans all topics for records where `ExpiresAt > 0 && time.Now().After(ExpiresAt)`
- Emits tombstone (gossip propagates deletion)
- Per-topic configurable: `TopicConfig.ExpiryEnabled bool`
- **Consumers:** Mercury witness attestations (10-min TTL), arbitrator profiles (6h), category announcements (6h), HSTLES reach/latency records (4h from Step 1 Task 1)

**2. Record-Level Signatures** (2 days)
```go
type Record struct {
    // ... existing fields
    HLCTimestamp uint64
    AuthorPubKey []byte        // NEW: Ed25519 public key of record author
    Signature    []byte        // NEW: Ed25519 signature over record content
}

// Per-topic signature config:
type TopicConfig struct {
    // ... existing fields
    RequireSignature bool                                        // reject unsigned records
    VerifyFunc       func(pubkey, signature, content []byte) bool // custom verifier (default Ed25519)
    SignFunc         func(content []byte) (pubkey, sig []byte)    // for local writes
}
```
- `Apply()` checks signature before merge if `RequireSignature` is true
- Unsigned records rejected at apply time (not at gossip receive — Whisper is agnostic)
- **Consumers:** Mercury (seller signs listings, witnesses sign attestations), Hospitium (authenticated CRDT deltas), HSTLES (node identity proof)

**3. Metrics / Observability Hooks** (0.5 days)
```go
type MetricsRecorder interface {
    RecordApplied(topic string, merged bool)        // called on every Apply()
    RecordExpired(topic string)                      // called on TTL prune
    MergeConflict(topic string, winner string)       // called when MergeFunc picks a winner
    TopicSize(topic string, count int, bytes int64)  // periodic gauge
    GossipExchange(topic string, sent, received int) // called by Whisper adapter
}

func (c *DirectoryCache) SetMetrics(m MetricsRecorder)
```
- Default: `NoopMetricsRecorder`
- **Consumers:** All three — operational necessity

**4. Change Feed Replay from HLC** (1 day)
```go
// ChangesSince returns all records for a topic with HLCTimestamp > since.
// Used by reconnecting peers and crash recovery.
func (c *DirectoryCache) ChangesSince(topic Topic, since uint64) ([]Record, error)

// Subscribe with replay — delivers historical records first, then live changes.
func (c *DirectoryCache) SubscribeWithReplay(topic Topic, since uint64, handler func(Record))
```
- Implements the `whisper.StateStore.DeltaSince()` method naturally
- **Consumers:** Hospitium (catch up after reconnect), Mercury (witness attestation history), HSTLES (bootstrap)

**5. Blob/CID Store Interface** (2 days)
```go
// BlobStore manages content-addressed binary objects alongside records.
// Records reference blobs by CID (content hash). Blobs are stored separately.
type BlobStore interface {
    Put(data []byte) (cid string, err error)       // returns SHA-256 content hash
    Get(cid string) ([]byte, error)                 // retrieve by CID
    Has(cid string) (bool, error)                   // existence check
    Delete(cid string) error                        // remove (GC)
    Size() (count int, bytes int64, err error)      // total stored
}

// Built-in implementations:
type FileBlobStore struct { ... }     // native: files in directory, CID as filename
type MemoryBlobStore struct { ... }   // testing
// IndexedDB variant provided by Hospitium (browser)
```
- Records can reference blobs via `BlobCID string` field on Record
- Blob lifecycle tied to record lifecycle: when record is tombstoned, blob is eligible for GC
- **Consumers:** Mercury (listing media up to 50MB, evidence files), Hospitium (documents)

#### Medium Priority

**6. Compaction / History Pruning** (1 day)
```go
type CompactionPolicy struct {
    MaxTombstoneAge time.Duration  // prune tombstones older than this
    MaxVersions     int            // keep last N versions per key (0 = unlimited)
    MaxRecords      int            // hard cap per topic (0 = unlimited)
}

func (c *DirectoryCache) RegisterCompaction(topic Topic, policy CompactionPolicy)
func (c *DirectoryCache) Compact(topic Topic) (removed int, err error)
func (c *DirectoryCache) CompactAll() (removed int, err error)
```
- Triggered by timer (default 1h) or explicit call
- **Consumers:** Hospitium (CRDT tombstone pruning), Mercury (reputation counter growth)

**7. Merge Conflict Log** (0.5 days)
```go
type ConflictRecord struct {
    Topic     string
    Key       string
    Winner    Record
    Loser     Record
    Timestamp time.Time
    MergeFunc string  // name of merge function that resolved it
}

func (c *DirectoryCache) ConflictLog(topic Topic, limit int) ([]ConflictRecord, error)
func (c *DirectoryCache) SetConflictLogging(enabled bool, maxEntries int)
```
- Ring buffer of last N conflicts per topic
- **Consumers:** Mercury (dispute evidence — "was this listing price changed concurrently?"), Hospitium (CRDT debugging)

**8. Bloom Filter API** (0.5 days)
```go
// Expose internal/bloom as public API for membership tests during gossip.
func (c *DirectoryCache) BloomFilter(topic Topic) *bloom.Filter
func (c *DirectoryCache) HasRecord(topic Topic, key string) bool  // fast path via bloom
```
- Already exists internally (`internal/bloom/`). Just expose it.
- **Consumers:** Whisper delta sync optimization (skip records peer already has)

**9. Topic-Level ACL Hooks** (1 day)
```go
type ACLFunc func(topic string, authorPubKey []byte, record Record) error

func (c *DirectoryCache) RegisterACL(topic Topic, fn ACLFunc)
```
- Called in `Apply()` before merge. Return error = reject record.
- **Consumers:** Mercury (only seller can write to their store topic), HSTLES (tenant isolation)

**10. Secondary Index Hooks** (1 day)
```go
type IndexFunc func(Record) []string  // returns index keys

func (c *DirectoryCache) RegisterIndex(topic Topic, name string, fn IndexFunc)
func (c *DirectoryCache) QueryByIndex(topic Topic, indexName string, key string) ([]Record, error)
```
- Maintained automatically on Apply/Delete
- **Consumers:** Mercury (listing search by price range, category), Hospitium (large collections)

#### Lower Priority (defer to consumer implementation)

**11. Record-Level Encryption** — Body can be encrypted so only certain peers decrypt. Overlaps with application-layer E2E. Defer.

**12. Cross-Topic Atomic Writes** — Apply multiple records in one transaction. Significant complexity. Defer until concrete use case.

**13. Sharded Topics** — For topics with millions of keys. Premature for current scale. Defer.

### Phase 4: Delete legacy files (1 hour)
- Delete `gossip/peer.go`, `gossip/ratelimit.go`, `gossip/discovery.go` (already deleted by Step 1 pre-cleanup / Step 2 Phase 1 — verify they're gone, skip if already removed)
- `gossip/adaptive.go` — already deleted by Step 1 Task 10a (replaced by `hwp_adaptive.go` which extracted to Whisper)

## 6. Consumer Updates (Phase 5)

After extraction, every consumer that imports `mesh/core/directory` must update to `github.com/bbmumford/ledger`.

### HSTLES Library (~36 importing files)

| Package | Files | Import Change |
|---|---|---|
| `mesh/core/directory/gossip/` | sync.go, rumor.go, protocol.go, lad_payload.go (peer.go, discovery.go, adaptive.go already deleted by Step 1/2) | `lad "github.com/hstles/library/mesh/core/directory"` → `lad "github.com/bbmumford/ledger"` |
| `mesh/core/directory/cache/` | directory.go, store.go, store_memory.go | Same import path change |
| `mesh/core/directory/ledger/` | local_ledger.go, memory_ledger.go, mesh_ledger.go | Same |
| `mesh/core/directory/anchor/` | anchor.go, verify.go | Same |
| `mesh/node/` | runtime.go, peer_connections.go, hwp_connection.go, rpc.go, rpc_forward.go, hwp_session_finder.go | Same |
| `mesh/node/handlers/` | generic.go, monitoring.go | Same |
| `dispatch/` | hwp_dispatch.go | Same |
| `domain/` | monitoring/service/, admin/ | Same |

### ORBTR Agent (~6 importing files)

| File | Import Change |
|---|---|
| `agent/internal/core/wire/agent_lad.go` | `lad "github.com/hstles/library/mesh/core/directory"` → `lad "github.com/bbmumford/ledger"` |
| `agent/internal/store/lad_cache_store.go` | Same |
| `agent/cmd/agentd/main.go` | `cache` import path changes |
| `library/mesh/agent/bootstrap/connect.go` | Both `lad` and `cache` imports change |
| `library/mesh/agent/bootstrap/snapshot.go` | `lad` import changes |

### ORBTR Endpoints (~10 importing files)

| File | Import Change |
|---|---|
| `help.orbtr.io/monitoring_api.go` | `ladcache "github.com/hstles/library/mesh/core/directory/cache"` → `ladcache "github.com/bbmumford/ledger/cache"` |
| All endpoints using `lad.Record` types | `lad` import path changes |

### ORBTR Library

| File | Import Change |
|---|---|
| `library/domain/*/register.go` | If any reference lad types (check each) |
| `library/mesh/agent/bootstrap/` | Both files import lad + cache |

### Process

1. Add `require github.com/bbmumford/ledger v0.1.0` to HSTLES Library `go.mod`
2. `sed -i 's|github.com/hstles/library/mesh/core/directory|github.com/bbmumford/ledger|g'` across all .go files
3. `sed -i 's|github.com/hstles/library/mesh/core/directory/cache|github.com/bbmumford/ledger/cache|g'`
4. `sed -i 's|github.com/hstles/library/mesh/core/directory/gossip|github.com/bbmumford/ledger/gossip|g'` (for the LAD-coupled 20%)
5. Delete `mesh/core/directory/` from HSTLES Library
6. Run `go mod tidy` in HSTLES Library
7. Run `go test ./...` — all tests must pass
8. Tag new HSTLES Library version
9. Bump ORBTR go.mod to new HSTLES version
10. ORBTR agent and endpoints build and deploy

**Note:** The ORBTR agent imports Ledger transitively via HSTLES Library. If the agent's go.mod uses `go.work` with local paths, the local HSTLES Library picks up the Ledger dependency automatically. For Docker builds (Fly.io), the go.mod version must be bumped.

## 7. Dependencies

- Aether extraction must complete first (Ledger types used by Whisper's StateStore)
- **Whisper extraction must complete first** (Ledger's gossip adapter implements `whisper.StateStore`)
- HSTLES Ledger consumer update (Phase 5) happens after Ledger is published
- ORBTR updates happen after HSTLES is tagged with the new imports
- Deploy follows staged procedure (bootstrap first, etc.)

## 8. Timeline

| Phase | Duration |
|---|---|
| Phase 1 (types + cache + proto) | 2 days |
| Phase 2 (storage backends) | 1 day |
| Phase 3 (Whisper adapter) | 1 day |
| Phase 3a (new capabilities — TTL, signatures, metrics, change feed, blob store, compaction, conflict log, bloom, ACL, indexes) | 5 days |
| Phase 4 (cleanup) | 1 hour |
| Phase 5 (consumer updates) | 1 day |
| **Total** | **~10 days** |

Ledger extraction (Phases 1-2) can run in parallel with Whisper extraction. Phase 3 requires Whisper to be published. Phase 3a (new capabilities) is independent and can be developed in any order.
