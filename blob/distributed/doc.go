/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package distributed implements the distributed blob storage system
// described in _OTHER/mercury/ROADMAP.md section 2.
//
// Phase 1 — local content-addressed storage:
//   - chunker.go / piece_index.go : 256 KB chunking, Merkle-root manifests
//   - quota.go                   : headroom-based dynamic disk budget
//   - encryption.go              : XChaCha20-Poly1305 encryption-at-rest
//   - abuse.go                   : reference validation, per-type / per-peer caps
//   - store.go                   : orchestrating BlobStore implementation
//
// Phase 2 — distributed advertisement / discovery / fetch:
//   - advertiser.go              : gossips "I have piece X" over a Whisper
//                                  BroadcastOnly topic (plutus.blob.have).
//                                  Periodic (60s default) emission of the
//                                  current CID list.
//   - resolver.go                : two halves of the "who has piece X?"
//                                  protocol over a Whisper RequestResponse
//                                  topic (plutus.blob.find): a responder
//                                  that replies when this node holds the
//                                  CID, and a caller that collates
//                                  responses with per-peer deduplication.
//   - fetcher.go                 : parallel piece fetch over an abstract
//                                  PeerTransport (stream ID 300 on
//                                  aether.Session in production). Races
//                                  top-K peers ranked by latency ×
//                                  reputation; first CID-verified response
//                                  wins, the rest are cancelled.
//
// Integration:
//   - Store.Get falls back to Config.Fetcher on a local miss. Fetched
//     pieces are cached back into the Store, subject to quota.
//
// Phase 3 (NOT implemented here):
//   - replicator.go              : opt-in seed loop, rarest-first,
//                                  opportunistic + paid pinning
//   - proof.go                   : proof-of-storage challenge/response
//   - settings.go                : role flags, pinning policy, on-chain
//                                  escrow integration
//
// This package imports whisper but does NOT import aether directly — the
// PeerTransport interface is small enough that wiring to an aether.Session
// pool lives with the consumer (e.g. the plutus package) rather than
// bloating ledger's dependency graph.
package distributed
