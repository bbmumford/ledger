/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package distributed is a transitional shell. The Phase-1 (local
// content-addressed storage) and Phase-2 (gossip advertise / resolve /
// fetch) implementations that used to live here have been extracted to
// the standalone module github.com/bbmumford/diststore — consumers
// import that directly now.
//
// Only the on-chain coupling layer (proof-of-storage challenge/response
// and paid-pinning escrow integration) remains in this tree, under the
// onchain/ subdirectory. That code stays here because it bridges the
// generic blob store to mercury/plutus chain primitives and is not
// useful outside the ledger module.
//
// This file exists solely to give the directory a valid Go package
// declaration; no functional code lives at this level.
package distributed
