# ledger

ledger is a store of signed records for peer meshes: an append-only log of
typed records (members, roles, reachability, latency, key operations, quorum
membership, blob references), a directory cache that keeps the current view of
those records per partition, pluggable persistence for both, and signed anchor
snapshots so a new node can bootstrap from one file. It carries no network
code; a gossip engine (whisper, through `whisper_adapter.go`) moves its records
between peers.

```sh
go get github.com/bbmumford/ledger
```

---

## What ledger is

ledger sits between a transport and the applications that read mesh state:

```
Application (a mesh node, a device agent, any app)
  ↓ queries records, applies mutations, subscribes to topic updates
Ledger (state directory)
  ↓ serialises records as opaque gossip payload
Aether + Whisper (transport + epidemic propagation)
```

A transport moves bytes. ledger gives those bytes meaning — typed records for peers, roles, reachability, latency, capabilities, and blob references. whisper's gossip engine carries ledger records between peers using delta sync, digest comparison, and per-peer watermarks.

---

## Components

| Component | Path | Description |
|-----------|------|-------------|
| **Record types** | `types.go` | Record, Topic, MemberRecord, RoleRecord, ReachRecord, LatencyRecord, HandlerMetadata, KeyOpsRecord, QuorumRecord |
| **Cache** | `cache/` | `DirectoryCache` — materialised view with per-topic queries (Members, Roles, Reach, Latency); CRDT semantics |
| **CacheStore** | `cache/store.go`, `cache/store_memory.go` | the cache's backend interface; `NewMemoryCacheStore` is built in, and a consumer plugs in SQLite, Turso or any store that meets the contract |
| **Ledger** | `storage/ledger.go`, `storage/local.go`, `storage/memory.go` | the append-only record log with streaming subscribers: `MemoryLedger`, and `LocalDBLedger` over any `database/sql` SQLite connection |
| **Anchor** | `anchor/` | Signed snapshot of all records for fast bootstrap (`generator.go`, `crypto.go`, `types.go`) |
| **Blob references** | `blob.go`, `blob/distributed/onchain/` | the `BlobStore` interfaces beside records; `blob/distributed` itself is an empty namespace (mesh blob storage lives in diststore), and `onchain` holds the proof-of-storage challenge and response, the paid-pinning envelopes and the `EscrowClient` that reports to the on-chain escrow |
| **Whisper adapter** | `whisper_adapter.go` | makes the directory cache a whisper `StateStore`, so whisper's G1 exchange can drive it |
| **Metrics** | `metrics.go` | Prometheus-compatible counters + gauges |
| **ACL** | `acl.go` | Topic-level write authorisation |
| **Compaction** | `compaction.go` | Tombstone + expiry compaction on the append log |
| **Conflict log** | `conflict.go` | a per-topic ring buffer of recent merge conflicts (`DirectoryCache.SetConflictLogging`) |

---

## Record Schema

```go
// Record is a single entry in the distributed directory.
type Record struct {
    Topic           Topic           // member, role, reach, latency, keyops, quorum, ...
    Seq             uint64          // per-node sequence number
    Partition       string          // partition scope (empty = global)
    NodeID          string          // which peer this record describes
    Body            json.RawMessage // typed payload (MemberRecord, RoleRecord, etc.)
    Signature       []byte          // Ed25519 signature over the canonical encoding
    Timestamp       time.Time
    LamportClock    uint64          // signed; merges do not order by it
    HLCTimestamp    uint64          // hybrid logical clock (0 = unused)
    Tombstone       bool            // deletion marker
    DeletedAt       time.Time
    TombstoneReason string          // "explicit", "cap"
    ExpiresAt       time.Time       // TTL expiration (zero = never)
    AuthorPubKey    []byte          // Ed25519 public key of the record's author
    BlobCID         string          // content-addressed blob reference
}
```

### Topics

| Topic | Record Type | Description |
|-------|-------------|-------------|
| `member` | MemberRecord | Peer identity, region, service name, public key, attributes |
| `role` | RoleRecord | Namespace-qualified roles (e.g., "platform.identity"), handler metadata |
| `reach` | ReachRecord | Advertised network addresses (IP, port, protocol, scope) |
| `latency` | LatencyRecord | Measured RTT between peers |
| `keyops` | KeyOpsRecord | Key rotation operations |
| `quorum` | QuorumRecord | Anchor quorum membership |
| `blob` | — | Content-addressed blob references (SHA-256 hash → source peers) |

---

## Architecture Principles

1. **Append-only**. Every state change appends a new record; older records are tombstoned, never mutated in place. Which record a key keeps is decided by the topic's merge function (see Design).
2. **Signed end-to-end**. Every record carries an Ed25519 signature by its `NodeID`. A peer can verify any record came from the claimed author.
3. **CRDT-friendly**. Records converge deterministically given the same set of observations, whatever order they arrive in; the topic's merge function decides which record a key keeps (see Design).
4. **Transport-agnostic**. ledger imports no network code. `whisper_adapter.go` adapts the cache to whisper's `StateStore`; consumers wire Ledger to Whisper (or any other gossip engine) at the integration layer.
5. **Pluggable persistence**. `CacheStore` is an interface with a memory-backed default; consumers plug in SQLite, Turso, or any KV store that satisfies the contract.
6. **Partition isolation**. Every record carries a `Partition`. Queries scope to a partition; the empty string is the global scope. A deployment with several partitions never serves one partition's state to another.

---

## Design

Each topic's behaviour is registered on the `DirectoryCache`, whole with `RegisterTopic(topic, TopicConfig)` or one part at a time with `RegisterMerge` and `RegisterKey`:

| `TopicConfig` field | When unset | Meaning |
|---|---|---|
| `Merge` | `OverwriteMerge` | which record a key keeps when a record arrives for a key that already holds one (`func(existing, incoming Record) (Record, error)`) |
| `Key` | `NodeIDKey` | the key a record is stored under; `BodyKey` reads `collection` and `key` from the JSON body |
| `ExpiryEnabled` | off | honour `ExpiresAt` |
| `RequireSignature` | off | refuse unsigned records |
| `VerifyFunc` | Ed25519 | the signature check |
| `SignFunc` | none | signs this node's own writes |

`OverwriteMerge` keeps the record with the higher `HLCTimestamp` when both records carry one, and otherwise the later `Timestamp`; an exact tie keeps the existing record. It does not order by `LamportClock`, which restarts from zero when a node restarts. The timestamp fallback compares two records of the same node only while the key is `NodeIDKey`; a topic keyed by `BodyKey` holds several nodes' records under one key, so it needs a merge function of its own rather than the default.

A topic can also carry a write ACL (`RegisterACL`), named secondary indexes (`RegisterIndex`, read with `QueryByIndex`) and a compaction policy (`RegisterCompaction`). `QueryFilter` (node, key prefix, since, limit) is declared for a generic query; no query takes it yet.

---

## Streams and the directory cache: what a consumer sees

`Ledger.Stream` delivers history first, then live records. `LocalDBLedger` and `MemoryLedger` also implement
`EndReportingLedger`, whose `StreamWithEnd` returns a `StreamEnd` that says why the channel closed
(`stream_end.go`). A consumer that keeps a view from the stream reads that reason; one that only ranges over the
channel sees a close and nothing more.

| Behaviour | What the consumer sees | Held by |
|---|---|---|
| A consumer falls behind | When a live record arrives while the stream's live queue (64 records) is full, the stream ends: every record queued before it is handed over, then the channel closes and `StreamEnd.Err()` is `ErrStreamOverflow`. No record is dropped while the stream carries on. The consumer streams again from a watermark no later than the first record it has not applied. | `storage/stream_end_test.go` `TestLocalDBLedger_AFullQueueEndsTheStreamWithOverflow`, `TestMemoryLedger_AFullQueueEndsTheStreamWithOverflow` |
| The ledger closes | Open streams end with `ErrLedgerClosed` after handing over what their queue holds. A closed `MemoryLedger` refuses `Stream` and `StreamWithEnd` with `ErrLedgerClosed` (text `ledger: closed`); a closed `LocalDBLedger` returns a stream that ends at once with `ErrLedgerClosed`. `MemoryLedger`'s `Append`, `BatchAppend`, `Snapshot` and `RestoreSnapshot` on a closed ledger still return the untyped `lad: ledger closed`. | `TestLedgers_CloseEndsAStreamWithLedgerClosed` |
| The caller's context ends | The stream ends with the context's error. | `TestLedgers_ACancelEndsTheStreamWithTheContextError` |
| The history read fails | The stream ends with the query, scan or rows error, wrapped. | `TestLocalDBLedger_AFailedHistoryReadEndsTheStreamWithItsError` |
| A record is appended while a stream starts | `LocalDBLedger` subscribes before its history query, so the record arrives by the history, the live queue, or both: a record can arrive twice at the start of a stream, never not at all. `MemoryLedger` registers and copies its history under one lock, so each record arrives once. | `TestLocalDBLedger_ARecordCommittedDuringTheHistoryReadReachesTheStream`, `TestMemoryLedger_RecordsAppendedWhileAStreamStartsArriveOnce` |
| A tombstone meets a live sequenced record | `DirectoryCache` refuses a tombstone whose `DeletedAt` is not after the newest live member, role or reach record with a sequence number for that key, whichever of the two arrives first. A tombstone equal to the one held applies nothing. | `cache/repeat_apply_test.go` `TestALiveRecordBeatsAnOlderTombstoneInEitherOrder`, `TestATombstoneRepeatedAfterTheMemberReregisteredDeletesNothing` |
| Membership callbacks | `OnMembershipChange` fires only when a member is removed: not for a tombstone of a member the cache does not hold, and not for a refused tombstone. | `TestATombstoneForAnAbsentMemberAnnouncesNoChange`, `TestApplyingAMemberTombstoneTwiceAnnouncesOneChange` |
| A repeated record | An exact repeat of the newest record for a key whose entry is still held is not applied or announced again; a record whose entry was evicted is applied again. An older role or reach record leaves the newer one in place. | `TestAnExactRepeatOfTheNewestRecordIsNotAnnouncedAgain`, `TestARecordWhoseEntryWasEvictedIsAppliedAgain`, `TestAnOlderRoleRecordLeavesTheNewerRoles`, `TestAnOlderReachRecordLeavesTheNewerDeltaBase` |

A ledger wrapper must forward `StreamWithEnd`, or a consumer handed the wrapper cannot tell an overflow from the
ledger's close. loom's signing ledger and reach's `CacheApplyHook.Wrap` forward it.

### Signing a record the ledger numbers

`Seq` is part of the signed bytes (`signatures.go`). A record the ledger numbers (one appended with `Seq` 0) is
signed through `SigningLedger`: `AppendSigned` and `BatchAppendSigned` give the record its `Seq` and `Timestamp` and
then call the caller's sign function, in the one step that stores it, so the signature covers the `Seq` stored.
`LocalDBLedger` and `MemoryLedger` implement it, and loom's signing ledger signs through it. `Append` and
`BatchAppend` refuse a signed record with no `Seq` (`ErrSignedWithoutSeq`). `LocalDBLedger` numbers each append
inside the transaction that stores it, and stores every field of a record, so a record replayed through `Stream` or
`Snapshot` verifies and a tombstone replays as a tombstone.

### Open item: restarting a stream exactly where it stopped

A consumer restarting after `ErrStreamOverflow` replays from the watermark it holds, and the repeat rules above make
the replay harmless. Restarting from the last record it applied is not possible on what the stream delivers today:
history arrives in timestamp order (`LocalDBLedger`) or topic by topic in append order (`MemoryLedger`), and live
records in the order concurrent appends broadcast them, so the highest sequence a consumer has seen can sit above a
record it has not yet received. The per-topic `Seq` a ledger assigns is
not gap-free either, since an append may give its own larger `Seq`. Restarting exactly needs a delivery position that
grows in delivery order (for example the store's row order), which would change this package's contract.

---

## API Docs

Generated API reference (pkgsite) for each tagged release is published to GitHub Pages:

- Latest: https://bbmumford.github.io/ledger/github.com/bbmumford/ledger/
- Published by `.github/workflows/docs.yml` on every `v*` tag.

---

## Quick start

```go
import (
	"context"
	"encoding/json"
	"time"

	"github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/ledger/storage"
)

ctx := context.Background()
log := storage.NewMemoryLedger(storage.MemoryLedgerConfig{MaxRecordsPerTopic: 10000})
dc := cache.NewDirectoryCache() // memory-backed unless stores are passed

body, _ := json.Marshal(ledger.MemberRecord{Partition: "p1", NodeID: "vl1_abc123", CreatedAt: time.Now()})
rec := ledger.Record{
	Topic:     ledger.TopicMember,
	Partition: "p1",
	NodeID:    "vl1_abc123",
	Timestamp: time.Now(),
	Body:      body,
}
_ = log.Append(ctx, rec)

// Keep the cache in step with the log: history first, then live records.
ch, _ := log.Stream(ctx, nil, nil)
go func() {
	for r := range ch {
		_ = dc.Apply(r)
	}
}()

members, _ := dc.Members(ctx, "p1")
```

| package | what it holds |
|---|---|
| `ledger` | `Record`, the record types, topics, signatures, `SigningLedger`, `StreamEnd`, ACL, compaction, expiry, metrics, the whisper adapter |
| `ledger/cache` | `DirectoryCache` and `CacheStore` |
| `ledger/storage` | `MemoryLedger` and `LocalDBLedger` |
| `ledger/anchor` | signed anchor snapshots: generation, signing, verification |
| `ledger/blob/distributed/onchain` | proof of storage and paid pinning for the on-chain escrow |
| `ledger/pb` | the protobuf record messages |

## Dependencies

- `github.com/bbmumford/whisper`: the `StateStore` and record-set types the
  adapter implements.
- `golang.org/x/crypto` (BLAKE2b, SHA-3) and `google.golang.org/protobuf`.
- Tests only: `modernc.org/sqlite`, the pure-Go SQLite driver behind
  `LocalDBLedger`'s tests.

## Used by

Importers in the ORBTR estate:

- `github.com/bbmumford/loom/node`, `directory`, `node/meshstatus` and
  `core/directory/gossip` (with `anchor`, `cache`, `storage`): the platform
  mesh's live directory.
- `github.com/bbmumford/reach` (with `storage`): the reachability records it
  publishes.
- `github.com/bbmumford/mercury`, `github.com/bbmumford/minerva` and
  `github.com/bbmumford/plutus` (`blob/distributed/onchain` among them).
- `github.com/ORBTR/agent/internal/core/wire`, `internal/store` and
  `systempart` (with `cache`): the device agent's directory.
- `github.com/ORBTR/help.orbtr.io` (with `cache`),
  `github.com/ORBTR/Library/domain/monitoringhost`,
  `github.com/ORBTR/cortex/credential/api_key`,
  `github.com/ORBTR/ai/compute.orbtr.ai` and
  `github.com/ORBTR/tools/schemaref/manifest` (`storage`).

## Platforms and build

Pure Go, no cgo and no build tags. The module's `go` directive is 1.25.0.
Releases are cut by release-please from conventional commits.

## Licence

MIT. Copyright (c) 2026 Brandon Miller-Mumford. The full text is in
[LICENSE](LICENSE).

## Contributing

[AGENTS.md](AGENTS.md) holds the repository's two rules, for people and coding
agents alike: no temporary documents in the repository, and conventional commit
messages that stand on their own. [CLAUDE.md](CLAUDE.md) imports it for Claude
Code.
