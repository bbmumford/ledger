/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

// On-chain proof-of-storage hashing primitives.
//
// ╔══════════════════════════════════════════════════════════════════════╗
// ║  WARNING — TWO SEPARATE HASH PRIMITIVES LIVE IN THIS PACKAGE         ║
// ║                                                                      ║
// ║  Off-chain gossip (proof.go → ComputeProofHash):                     ║
// ║      BLAKE2b-256(piece || nonce). Used for the Whisper               ║
// ║      challenge/response envelope exchanged between peers.            ║
// ║                                                                      ║
// ║  On-chain escrow (THIS FILE → ComputeOnChainExpectedHash):           ║
// ║      keccak256(piece || nonce). Used for the expectedHash field      ║
// ║      submitted to PlutusStorageEscrow.sol's openChallenge() and      ║
// ║      compared against the replicator's respond(bytes32) payload.     ║
// ║      BLAKE2b is gas-prohibitive on EVM, so the on-chain verifier     ║
// ║      uses keccak256 (native keccak256 opcode = ~36 gas).             ║
// ║                                                                      ║
// ║  Do NOT mix these. Submitting a BLAKE2b digest as the on-chain       ║
// ║  expectedHash will cause the replicator's keccak256 response to      ║
// ║  never match, producing false misses and unjust bond slashing.       ║
// ║  Submitting a keccak256 digest over the off-chain wire will fail     ║
// ║  VerifyResponse because the responder signs the BLAKE2b digest.      ║
// ╚══════════════════════════════════════════════════════════════════════╝
//
// See _PACKAGES/plutus/contracts/evm/PlutusStorageEscrow.sol — the on-chain
// contract computes its reference hash as keccak256(abi.encodePacked(piece,
// nonce)); since abi.encodePacked with two bytes arguments is just the
// concatenation piece||nonce, this Go helper produces the identical bytes.

import (
	"golang.org/x/crypto/sha3"
)

// ComputeOnChainExpectedHash computes keccak256(piece || nonce) as a
// 32-byte array suitable for submission as the `expectedHash` parameter
// of PlutusStorageEscrow.openChallenge(bytes32 nonce, bytes32 expectedHash).
//
// Use this — NOT ComputeProofHash — anywhere you are building a payload
// destined for an EVM contract. ComputeProofHash is BLAKE2b-256 and is
// off-chain only.
//
// The concatenation order (piece first, nonce second) matches both the
// Solidity side (keccak256(abi.encodePacked(piece, nonce))) and the
// off-chain BLAKE2b primitive so callers can reason about "piece, then
// nonce" uniformly across both worlds.
func ComputeOnChainExpectedHash(piece, nonce []byte) [32]byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(piece)
	h.Write(nonce)
	var out [32]byte
	sum := h.Sum(nil)
	copy(out[:], sum)
	return out
}
