/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"golang.org/x/crypto/blake2b"
)

func TestSplit1MBProducesFour256KBPieces(t *testing.T) {
	data := make([]byte, 1024*1024) // 1 MB
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand: %v", err)
	}
	m, pieces, err := Split(data, 0)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if got := len(pieces); got != 4 {
		t.Fatalf("piece count: got %d want 4", got)
	}
	for i, p := range pieces {
		if len(p) != PieceSize {
			t.Errorf("piece %d size: got %d want %d", i, len(p), PieceSize)
		}
		expect := blake2b.Sum256(p)
		if m.Pieces[i].CID == "" {
			t.Errorf("piece %d: empty CID", i)
		}
		if got := m.Pieces[i].SizeBytes; got != PieceSize {
			t.Errorf("piece %d sizeBytes: got %d want %d", i, got, PieceSize)
		}
		_ = expect
	}
	if m.TotalBytes != int64(len(data)) {
		t.Errorf("totalBytes: got %d want %d", m.TotalBytes, len(data))
	}
	if m.RootCID == "" {
		t.Error("empty root CID")
	}

	// Round trip: reassemble and compare.
	got, err := Reassemble(m, pieces)
	if err != nil {
		t.Fatalf("Reassemble: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("reassembled bytes differ from original")
	}
}

func TestSplitRejects60MB(t *testing.T) {
	data := make([]byte, 60*1024*1024)
	_, _, err := Split(data, 0)
	if err == nil {
		t.Fatal("expected ErrFileTooLarge, got nil")
	}
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("expected ErrFileTooLarge, got %v", err)
	}
}

func TestSplitUnevenTail(t *testing.T) {
	// 300 KB — expect 2 pieces (256 KB + 44 KB).
	data := make([]byte, 300*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand: %v", err)
	}
	_, pieces, err := Split(data, 0)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(pieces) != 2 {
		t.Fatalf("got %d pieces, want 2", len(pieces))
	}
	if pieces[0] == nil || len(pieces[0]) != PieceSize {
		t.Errorf("piece 0 size: got %d", len(pieces[0]))
	}
	if len(pieces[1]) != 300*1024-PieceSize {
		t.Errorf("piece 1 size: got %d want %d", len(pieces[1]), 300*1024-PieceSize)
	}
}

func TestSplitEmpty(t *testing.T) {
	m, pieces, err := Split(nil, 0)
	if err != nil {
		t.Fatalf("Split empty: %v", err)
	}
	if len(pieces) != 0 {
		t.Fatalf("pieces: %d want 0", len(pieces))
	}
	if m.TotalBytes != 0 {
		t.Fatalf("totalBytes: %d want 0", m.TotalBytes)
	}
	if m.RootCID != "" {
		t.Fatalf("root CID: %q want empty", m.RootCID)
	}
}

func TestReassembleDetectsTamper(t *testing.T) {
	data := make([]byte, 600*1024)
	rand.Read(data)
	m, pieces, err := Split(data, 0)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	// Flip a byte in piece 1.
	pieces[1][0] ^= 0x01
	if _, err := Reassemble(m, pieces); err == nil {
		t.Fatal("expected mismatch, got nil")
	}
}
