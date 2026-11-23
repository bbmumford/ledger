/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package ledger_test

import (
	"testing"

	"github.com/bbmumford/whisper"
)

// The adapter answers whisper's two bucket extensions over the same records
// its Fingerprint summarises: the buckets XOR to the fingerprint, and every
// bucket selected serves the whole snapshot.
func TestCacheStateStore_BucketsSummariseTheSameRecords(t *testing.T) {
	a, _ := twoCachesWithTheSameMembers(t)
	s := adapterOver(a)

	var _ whisper.BucketFingerprintProvider = s
	var _ whisper.BucketRecordsProvider = s

	var xor uint64
	for _, d := range s.BucketDigest(16) {
		xor ^= d
	}
	if xor != s.Fingerprint() {
		t.Fatalf("XOR(BucketDigest(16)) = %#x, Fingerprint = %#x", xor, s.Fingerprint())
	}
	want := make([]bool, 16)
	for i := range want {
		want[i] = true
	}
	if got, all := len(s.RecordsInBuckets(16, want)), len(s.Snapshot()); got != all {
		t.Fatalf("every bucket selected %d records, Snapshot holds %d", got, all)
	}
}
