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
    TenantID     string          // tenant scope (empty = global)
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
6. **Tenant isolation**. Every record carries a `TenantID`. Queries scope to a tenant; the empty string is the global scope. Multi-tenant deployments never see cross-tenant state.

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
