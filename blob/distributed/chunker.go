/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"encoding/hex"
	"errors"
	"fmt"

	"golang.org/x/crypto/blake2b"
)

// PieceSize is the fixed chunk size for the distributed blob layer.
const PieceSize = 256 * 1024 // 256 KB

// DefaultMaxFileSizeBytes is the file-size cap (50 MB = 200 x 256 KB chunks).
const DefaultMaxFileSizeBytes int64 = 50 * 1024 * 1024

// ErrFileTooLarge is returned when Split receives a file above the cap.
var ErrFileTooLarge = errors.New("distributed: file exceeds max size")

// PieceRef describes one chunk in a manifest.
type PieceRef struct {
	Index     int    `json:"index"`
	CID       string `json:"cid"`
	SizeBytes int    `json:"sizeBytes"`
}

// Manifest is the piece list that describes a logical file.
type Manifest struct {
	// RootCID is the Merkle root over ordered piece CIDs and is the file's
	// content address on the mesh.
	RootCID string `json:"rootCid"`
	// Pieces are the chunks in order.
	Pieces []PieceRef `json:"pieces"`
	// TotalBytes is the plaintext byte count of the original file.
	TotalBytes int64 `json:"totalBytes"`
	// PieceSize captured for forward-compatibility (currently always 256 KB).
	PieceSize int `json:"pieceSize"`
}

// computePieceCID hashes a plaintext chunk with BLAKE2b-256.
func computePieceCID(chunk []byte) string {
	sum := blake2b.Sum256(chunk)
	return hex.EncodeToString(sum[:])
}

// computeMerkleRoot builds a binary Merkle root over ordered piece CIDs.
// When a level has an odd leaf count the last leaf is duplicated
// (BitTorrent-style). Returns an empty string when there are zero pieces.
func computeMerkleRoot(pieceCIDs []string) string {
	if len(pieceCIDs) == 0 {
		return ""
	}
	layer := make([][]byte, len(pieceCIDs))
	for i, c := range pieceCIDs {
		b, err := hex.DecodeString(c)
		if err != nil {
			// Defensive fallback. Split only ever emits valid hex CIDs.
			sum := blake2b.Sum256([]byte(c))
			b = sum[:]
		}
		layer[i] = b
	}
	for len(layer) > 1 {
		if len(layer)%2 == 1 {
			layer = append(layer, layer[len(layer)-1])
		}
		next := make([][]byte, len(layer)/2)
		for i := 0; i < len(next); i++ {
			concat := make([]byte, 0, len(layer[2*i])+len(layer[2*i+1]))
			concat = append(concat, layer[2*i]...)
			concat = append(concat, layer[2*i+1]...)
			sum := blake2b.Sum256(concat)
			next[i] = sum[:]
		}
		layer = next
	}
	return hex.EncodeToString(layer[0])
}

// Split chunks plaintext into 256 KB pieces, computes each piece's CID, and
// builds the Merkle root. Pass zero or a negative value for maxFileSizeBytes
// to use DefaultMaxFileSizeBytes.
func Split(data []byte, maxFileSizeBytes int64) (*Manifest, [][]byte, error) {
	if maxFileSizeBytes <= 0 {
		maxFileSizeBytes = DefaultMaxFileSizeBytes
	}
	if int64(len(data)) > maxFileSizeBytes {
		return nil, nil, fmt.Errorf("%w: %d > %d", ErrFileTooLarge, len(data), maxFileSizeBytes)
	}
	if len(data) == 0 {
		return &Manifest{
			RootCID:    "",
			Pieces:     []PieceRef{},
			TotalBytes: 0,
			PieceSize:  PieceSize,
		}, [][]byte{}, nil
	}

	numPieces := (len(data) + PieceSize - 1) / PieceSize
	pieces := make([][]byte, 0, numPieces)
	refs := make([]PieceRef, 0, numPieces)
	cids := make([]string, 0, numPieces)

	for i := 0; i < numPieces; i++ {
		start := i * PieceSize
		end := start + PieceSize
		if end > len(data) {
			end = len(data)
		}
		// Copy so callers that mutate `data` don't affect stored chunks.
		chunk := make([]byte, end-start)
		copy(chunk, data[start:end])
		cid := computePieceCID(chunk)
		pieces = append(pieces, chunk)
		cids = append(cids, cid)
		refs = append(refs, PieceRef{
			Index:     i,
			CID:       cid,
			SizeBytes: len(chunk),
		})
	}

	return &Manifest{
		RootCID:    computeMerkleRoot(cids),
		Pieces:     refs,
		TotalBytes: int64(len(data)),
		PieceSize:  PieceSize,
	}, pieces, nil
}

// Reassemble concatenates chunks in order after verifying each chunk's CID
// matches the manifest. Returns an error if any piece fails.
func Reassemble(m *Manifest, pieces [][]byte) ([]byte, error) {
	if m == nil {
		return nil, errors.New("distributed: nil manifest")
	}
	if len(pieces) != len(m.Pieces) {
		return nil, fmt.Errorf("distributed: piece count mismatch: got %d want %d",
			len(pieces), len(m.Pieces))
	}
	out := make([]byte, 0, m.TotalBytes)
	for i, ref := range m.Pieces {
		if len(pieces[i]) != ref.SizeBytes {
			return nil, fmt.Errorf("distributed: piece %d size %d want %d",
				i, len(pieces[i]), ref.SizeBytes)
		}
		if computePieceCID(pieces[i]) != ref.CID {
			return nil, fmt.Errorf("distributed: piece %d cid mismatch", i)
		}
		out = append(out, pieces[i]...)
	}
	if int64(len(out)) != m.TotalBytes {
		return nil, fmt.Errorf("distributed: reassembled size %d want %d",
			len(out), m.TotalBytes)
	}
	return out, nil
}
