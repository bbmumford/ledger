/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package distributed is a namespace shell. Mesh-only blob storage
// (chunker, store, advertise/resolve/fetch, replicator) lives in
// github.com/bbmumford/diststore — consumers of that surface import
// diststore directly.
//
// The on-chain coupling layer (proof-of-storage challenge/response and
// paid-pinning escrow integration) lives in onchain/ underneath this
// package because it bridges the generic blob store to mercury/plutus
// chain primitives and consumes ledger's SealedColumn + KEK helpers.
//
// This file exists solely to give the directory a valid Go package
// declaration; no functional code lives at this level.
package distributed
