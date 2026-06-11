/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package cache implements the in-memory DirectoryCache projection of the
// Ledger-as-Directory (LAD) plus its anti-entropy and gossip plumbing.
//
// # Cache ingress trust contract
//
// There are exactly two doors into the cache, and the security of every
// runtime decision that reads from it depends on which door a record came
// through:
//
//   - [DirectoryCache.Apply] runs the per-topic ACL (the aclFuncs map, see
//     applyCore's ACL check and lad.ACLFunc). For signed topics the
//     registered ACL is the signed-topic ACL, which enforces
//     lad.VerifyRecord (ledger/signatures.go) semantics at the topic layer
//     — an unsigned record, or one whose Ed25519 signature does not
//     reproduce over signatureContent, is rejected before it can merge.
//
//   - [DirectoryCache.ApplyLocal] skips the ACL (applyCore skipACL=true).
//
// INVARIANT: every record that originates OUTSIDE this process — gossip
// G1/G3 frames, snapshot transfer, replay from another node — MUST enter
// through Apply. ApplyLocal is an ENUMERATED ALLOWLIST of trusted
// in-process bridges only, each of which has already verified its source's
// identity and signature in a scheme lad.VerifyRecord cannot speak (e.g.
// ed25519 swarm-record signatures vs lad-record signatures), and is
// re-projecting that already-trusted state into the LAD view.
//
// The allowlist of legitimate ApplyLocal callers today is exactly:
//
//   - HSTLES/Library/mesh/node/lad_reach_bridge.go — republishes
//     TopicReach / TopicMember / TopicRole records derived from swarm
//     PeerRecords (verified under the swarm record signature scheme).
//   - HSTLES/Library/mesh/node/swarm_integration.go — the bridge wiring
//     that drives the above.
//
// Adding any new ApplyLocal caller REQUIRES updating this list and the
// guard test in trust_contract_test.go. Because that allowlist spans a
// separate Go module (the HSTLES Library), it cannot be enforced by a
// test from inside this package — it is enforced by code review against
// this doc. What the guard test DOES enforce locally is the gate itself:
// Apply runs the ACL (and rejects records that fail it), ApplyLocal does
// not. A refactor that accidentally routes ApplyLocal through the ACL, or
// routes Apply around it, fails the test.
package cache
