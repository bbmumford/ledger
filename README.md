# Ledger — Distributed State Directory

```
go get github.com/bbmumford/ledger
```

**Related:** [Aether](https://github.com/ORBTR/Aether) (wire protocol) · [Whisper](https://github.com/bbmumford/whisper) (gossip engine)

> **Ledger** is an append-only, signed-record distributed state directory for peer meshes. It provides typed record storage, materialised caches, pluggable persistence backends (memory / SQLite / Turso), signed anchor snapshots for fast bootstrap, and a distributed blob storage pipeline with quota / pinning / replication / rarest-first fetch. Designed to ride on top of any wire transport — pairs naturally with [Aether](https://github.com/ORBTR/Aether) (Noise-UDP mesh protocol) and [Whisper](https://github.com/bbmumford/whisper) (directory gossip).

---

## Status

| Area | Status |
|------|--------|
| **Record schema + types** | Stable |
| **DirectoryCache** (materialised views) | Stable |
| **Pluggable CacheStore** (memory / SQLite / Turso) | Stable |
| **Append-only ledger + subscriber broadcast** | Stable |
| **Anchor snapshot + signing** | Stable |
| **Distributed blob pipeline** (chunker, fetcher, replicator, rarest-first, endgame, pinning, quota, abuse, proof, resolver) | Stable |
| **Whisper adapter** (Record → GossipPayload) | Stable |
| **v0.0.1 release** | Shipped (2026-04-20) |

In production use by the HSTLES mesh across `bootstrap.hstles.com`, `node.hstles.com`, and every ORBTR endpoint (`{devices,relay,app,get,help,node,orbtr}.orbtr.io`) — full fleet currently carries ~500 directory records per region with anchor snapshots rotating every 5 minutes.

---

## What Ledger Is

Ledger sits between a wire protocol (Aether) and applications:

```
Application (HSTLES mesh, ORBTR agent, custom app)
  ↓ queries records, applies mutations, subscribes to topic updates
Ledger (state directory)
  ↓ serialises records as opaque gossip payload
Aether + Whisper (transport + epidemic propagation)
```

A transport moves bytes. Ledger gives those bytes meaning — typed records for peers, roles, reachability, latency, capabilities, and blob references. Whisper's gossip engine carries Ledger records between peers using delta sync, digest comparison, and per-peer watermarks.

---

## Components

| Component | Path | Description |
|-----------|------|-------------|
| **Record types** | `types.go` | Record, Topic, MemberRecord, RoleRecord, ReachRecord, LatencyRecord, HandlerMetadata, KeyOpsRecord, QuorumRecord |
| **Cache** | `cache/` | `DirectoryCache` — materialised view with per-topic queries (Members, Roles, Reach, Latency); CRDT semantics |
| **CacheStore** | `cache/store.go`, `cache/store_memory.go` | Pluggable backend interface (memory built-in; SQLite/Turso via consumer) |
| **Ledger** | `storage/ledger.go`, `storage/local.go`, `storage/memory.go` | Append-only record log with subscriber broadcast |
| **Anchor** | `anchor/` | Signed snapshot of all records for fast bootstrap (`generator.go`, `crypto.go`, `types.go`) |
| **Blob** | `blob/` + `blob/distributed/` | Content-addressed blob store + distributed pipeline: chunker, encryption, fetcher, advertiser, replicator, rarest-first, endgame, proof (on-chain + local), pinning, quota, abuse guard |
| **Whisper adapter** | `whisper_adapter.go` | Implements Whisper's `GossipPayload` interface for Ledger records |
| **Metrics** | `metrics.go` | Prometheus-compatible counters + gauges |
| **ACL** | `acl.go` | Topic-level write authorisation |
| **Compaction** | `compaction.go` | Tombstone + expiry compaction on the append log |
| **Conflict** | `conflict.go` | Lamport-clock + sequence conflict resolution |

---

## Record Schema

```go
// Record is a single entry in the distributed directory.
type Record struct {
    Topic        Topic           // member, role, reach, latency, keyops, quorum
    NodeID       string          // which peer this record describes
    Partition    string          // partition scope (empty = global)
    Body         json.RawMessage // typed payload (MemberRecord, RoleRecord, etc.)
    Timestamp    time.Time
    LamportClock uint64          // causal ordering
    Seq          uint32          // per-node sequence number
    Tombstone    bool            // soft delete
    ExpiresAt    time.Time       // TTL
    Signature    []byte          // Ed25519 signature over canonical encoding
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

1. **Append-only**. Every state change appends a new record. Conflicts resolved by Lamport clock + sequence; older records tombstoned, never mutated in place.
2. **Signed end-to-end**. Every record carries an Ed25519 signature by its `NodeID`. A peer can verify any record came from the claimed author.
3. **CRDT-friendly**. Records converge deterministically given the same set of observations — ordering doesn't matter for eventual consistency, Lamport clock breaks ties for causally-dependent updates.
4. **Transport-agnostic**. Ledger doesn't import any network code. The `whisper_adapter.go` provides `GossipPayload` serialisation; consumers wire Ledger to Whisper (or any other gossip engine) at the integration layer.
5. **Pluggable persistence**. `CacheStore` is an interface with a memory-backed default; consumers plug in SQLite, Turso, or any KV store that satisfies the contract.
6. **Partition isolation**. Every record carries a `Partition`. Queries scope to a partition; the empty string is the global scope. A deployment with several partitions never serves one partition's state to another.

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
grows in delivery order (for example the store's row order), which is a change to this package's contract that no
design states yet.

---

## API Docs

Generated API reference (pkgsite) for each tagged release is published to GitHub Pages:

- Latest: https://bbmumford.github.io/ledger/github.com/bbmumford/ledger/
- Published by `.github/workflows/docs.yml` on every `v*` tag.

---

## Quick Start

```go
import (
    "context"
    "time"

    "github.com/bbmumford/ledger"
    "github.com/bbmumford/ledger/cache"
    "github.com/bbmumford/ledger/storage"
)

// In-memory ledger + cache — no external dependencies.
ctx := context.Background()
log := storage.NewMemoryLedger()
store := cache.NewMemoryStore()
dc := cache.NewDirectoryCache(store)
log.Subscribe(dc)

// Append a member record.
rec := &ledger.Record{
    Topic:     ledger.TopicMember,
    NodeID:    "vl1_abc123",
    Timestamp: time.Now(),
    Body:      /* encoded MemberRecord */,
}
log.Append(ctx, rec)

// Query the materialised view.
members, _ := dc.Members(ctx, "", cache.MemberQuery{Region: "syd"})
```

---

## License

[MIT](LICENSE)

Copyright (c) 2026 HSTLES / ORBTR Pty Ltd
