/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package bloom

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	tests := []struct {
		name              string
		expectedElements  uint64
		falsePositiveRate float64
	}{
		{"small_filter", 100, 0.01},
		{"medium_filter", 10000, 0.01},
		{"large_filter", 100000, 0.001},
		{"high_fp_rate", 1000, 0.1},
		{"low_fp_rate", 1000, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf := NewBloomFilter(tt.expectedElements, tt.falsePositiveRate)
			if bf == nil {
				t.Fatal("NewBloomFilter returned nil")
			}
			if bf.numBits == 0 {
				t.Error("numBits should be > 0")
			}
			if bf.numHash == 0 {
				t.Error("numHash should be > 0")
			}
			if len(bf.bits) == 0 {
				t.Error("bits array should not be empty")
			}
		})
	}
}

func TestBloomFilterAddAndContains(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	testData := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("bloom"),
		[]byte("filter"),
		[]byte("test"),
	}

	// Initially, nothing should be present
	for _, data := range testData {
		if bf.Contains(data) {
			t.Errorf("Contains(%s) returned true before Add", data)
		}
	}

	// Add all items
	for _, data := range testData {
		bf.Add(data)
	}

	// All added items should be present (no false negatives)
	for _, data := range testData {
		if !bf.Contains(data) {
			t.Errorf("Contains(%s) returned false after Add (false negative)", data)
		}
	}
}

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(10000, 0.01)

	// Add 1000 random items
	items := make([][]byte, 1000)
	for i := range items {
		items[i] = make([]byte, 32)
		rand.Read(items[i])
		bf.Add(items[i])
	}

	// Verify all items are found (no false negatives guaranteed)
	for _, item := range items {
		if !bf.Contains(item) {
			t.Fatal("Bloom filter produced a false negative - this should never happen")
		}
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	// Use a larger filter and higher FP rate for more stable testing
	expectedFPRate := 0.05 // 5% target
	bf := NewBloomFilter(5000, expectedFPRate)

	// Track what we actually add to avoid testing items we added
	added := make(map[string]bool)

	// Add 5000 items
	for i := 0; i < 5000; i++ {
		data := make([]byte, 32)
		rand.Read(data)
		bf.Add(data)
		added[string(data)] = true
	}

	// Test 10000 items that were definitely NOT added
	falsePositives := 0
	testCount := 0
	for i := 0; i < 20000 && testCount < 10000; i++ {
		// Generate test data with different structure to avoid collision with added items
		data := make([]byte, 32)
		rand.Read(data)
		// Only count items we're sure weren't added
		if !added[string(data)] {
			testCount++
			if bf.Contains(data) {
				falsePositives++
			}
		}
	}

	actualFPRate := float64(falsePositives) / float64(testCount)
	// Allow generous tolerance (10x) since this is probabilistic
	maxAcceptableFPRate := expectedFPRate * 10
	if actualFPRate > maxAcceptableFPRate {
		t.Errorf("False positive rate %.4f exceeds acceptable %.4f", actualFPRate, maxAcceptableFPRate)
	}
	t.Logf("False positive rate: %.4f (target: %.4f)", actualFPRate, expectedFPRate)
}

func TestBloomFilterClear(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Add items
	testData := []byte("test data")
	bf.Add(testData)

	if !bf.Contains(testData) {
		t.Fatal("Contains returned false after Add")
	}

	// Clear the filter
	bf.Clear()

	// After clear, nothing should be present
	if bf.Contains(testData) {
		t.Error("Contains returned true after Clear")
	}
}

func TestBloomFilterSerializeDeserialize(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	testData := [][]byte{
		[]byte("item1"),
		[]byte("item2"),
		[]byte("item3"),
	}

	for _, data := range testData {
		bf.Add(data)
	}

	// Serialize
	serialized := bf.Serialize()
	if len(serialized) < 12 {
		t.Fatal("Serialized data too short")
	}

	// Deserialize
	restored, err := DeserializeBloomFilter(serialized)
	if err != nil {
		t.Fatalf("DeserializeBloomFilter failed: %v", err)
	}

	// Verify restored filter has same properties
	if restored.numBits != bf.numBits {
		t.Errorf("numBits mismatch: got %d, want %d", restored.numBits, bf.numBits)
	}
	if restored.numHash != bf.numHash {
		t.Errorf("numHash mismatch: got %d, want %d", restored.numHash, bf.numHash)
	}

	// Verify all items are still found
	for _, data := range testData {
		if !restored.Contains(data) {
			t.Errorf("Restored filter doesn't contain %s", data)
		}
	}
}

func TestBloomFilterDeserializeInvalidData(t *testing.T) {
	// Too short
	_, err := DeserializeBloomFilter([]byte{1, 2, 3})
	if err == nil {
		t.Error("DeserializeBloomFilter should fail with short data")
	}
}

func TestBloomFilterEstimatedFillRatio(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Empty filter should have 0 fill ratio
	ratio := bf.EstimatedFillRatio()
	if ratio != 0 {
		t.Errorf("Empty filter should have 0 fill ratio, got %f", ratio)
	}

	// Add items and verify ratio increases
	for i := 0; i < 500; i++ {
		data := make([]byte, 8)
		rand.Read(data)
		bf.Add(data)
	}

	ratio = bf.EstimatedFillRatio()
	if ratio <= 0 {
		t.Error("Fill ratio should be > 0 after adding items")
	}
	if ratio >= 1 {
		t.Error("Fill ratio should be < 1")
	}
}

func TestBloomFilterConcurrency(t *testing.T) {
	bf := NewBloomFilter(10000, 0.01)
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				data := make([]byte, 32)
				rand.Read(data)
				bf.Add(data)
			}
		}(i)
	}

	// Concurrent reads while adding
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				data := make([]byte, 32)
				rand.Read(data)
				bf.Contains(data)
			}
		}()
	}

	wg.Wait()
}

func TestBloomFilterManagerGetOrCreate(t *testing.T) {
	cfg := DefaultBloomConfig()
	mgr := NewBloomFilterManager(cfg)

	// Get filter for scope A
	filterA1 := mgr.GetOrCreate("scope-a")
	if filterA1 == nil {
		t.Fatal("GetOrCreate returned nil")
	}

	// Get same scope again - should return same filter
	filterA2 := mgr.GetOrCreate("scope-a")
	if filterA1 != filterA2 {
		t.Error("GetOrCreate should return same filter for same scope")
	}

	// Get different scope - should return different filter
	filterB := mgr.GetOrCreate("scope-b")
	if filterA1 == filterB {
		t.Error("Different scopes should have different filters")
	}
}

func TestBloomFilterManagerConcurrency(t *testing.T) {
	cfg := DefaultBloomConfig()
	mgr := NewBloomFilterManager(cfg)
	var wg sync.WaitGroup

	// Concurrent GetOrCreate for multiple scopes
	scopes := []string{"scope-1", "scope-2", "scope-3", "scope-4", "scope-5"}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			scope := scopes[id%len(scopes)]
			filter := mgr.GetOrCreate(scope)
			if filter == nil {
				t.Error("GetOrCreate returned nil")
			}
			// Do some operations
			data := make([]byte, 32)
			rand.Read(data)
			filter.Add(data)
			filter.Contains(data)
		}(i)
	}

	wg.Wait()
}

func TestDefaultBloomConfig(t *testing.T) {
	cfg := DefaultBloomConfig()

	if cfg.ExpectedElements == 0 {
		t.Error("ExpectedElements should be > 0")
	}
	if cfg.FalsePositiveRate <= 0 || cfg.FalsePositiveRate >= 1 {
		t.Error("FalsePositiveRate should be between 0 and 1")
	}
	if cfg.RebuildInterval == 0 {
		t.Error("RebuildInterval should be > 0")
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	data := make([]byte, 32)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(data)
	}
}

func BenchmarkBloomFilterContains(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		data := make([]byte, 32)
		rand.Read(data)
		bf.Add(data)
	}

	testData := make([]byte, 32)
	rand.Read(testData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Contains(testData)
	}
}

// A caller-supplied BloomConfig reaches NewBloomFilter unchecked through
// BloomFilterManager.GetOrCreate, which LocalDBLedger.updateBloomFilter calls
// on every Append. Each input here sized the filter to zero bits or fed
// math.Log a value outside (0,1); the filter must still take an Add and
// answer Contains for what it holds.
func TestNewBloomFilterTakesAnAddForUnusableSizingInputs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected uint64
		rate     float64
	}{
		{"zero expected elements", 0, 0.01},
		{"zero false-positive rate", 100, 0},
		{"negative false-positive rate", 100, -0.5},
		{"false-positive rate of one", 100, 1},
		{"false-positive rate above one", 100, 2},
		{"zero elements and zero rate", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("NewBloomFilter(%d, %v) then Add panicked: %v", tc.expected, tc.rate, r)
				}
			}()
			bf := NewBloomFilter(tc.expected, tc.rate)
			if bf.numBits == 0 || bf.numHash == 0 || uint64(bf.numHash) > bf.numBits {
				t.Fatalf("NewBloomFilter(%d, %v) sized %d bits and %d hash rounds; Add needs at least one bit "+
					"and no more rounds than bits", tc.expected, tc.rate, bf.numBits, bf.numHash)
			}
			bf.Add([]byte("held"))
			if !bf.Contains([]byte("held")) {
				t.Fatalf("NewBloomFilter(%d, %v): an added element is reported absent", tc.expected, tc.rate)
			}
		})
	}

	t.Run("manager with a zero-element config", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("GetOrCreate under BloomConfig{ExpectedElements: 0} then Add panicked: %v", r)
			}
		}()
		mgr := NewBloomFilterManager(BloomConfig{ExpectedElements: 0, FalsePositiveRate: 0.01})
		mgr.GetOrCreate("scope").Add([]byte("record"))
	})
}

// LocalDBLedger.loadBloomFilters hands every stored row to
// DeserializeBloomFilter and skips a row it refuses, so the scope gets a fresh
// filter from GetOrCreate. A row whose bit count is zero or larger than its bit
// slice would make the next Add divide by zero or index past the slice, and a
// row whose hash count exceeds its bit count would make every Add and Contains
// run that many hash rounds under the filter's lock, so each is refused; a row
// whose hash count is zero would report every element present, so the count
// is lifted to one.
func TestDeserializeBloomFilterRefusesACorruptStoredRow(t *testing.T) {
	row := func(numBits uint64, numHash uint32, bits []byte) []byte {
		data := make([]byte, 12+len(bits))
		binary.LittleEndian.PutUint64(data[0:8], numBits)
		binary.LittleEndian.PutUint32(data[8:12], numHash)
		copy(data[12:], bits)
		return data
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"zero bit count", row(0, 3, make([]byte, 8))},
		{"bit count past the slice", row(65, 3, make([]byte, 8))},
		{"bit count with no slice", row(8, 3, nil)},
		{"hash count past the bit count", row(64, 65, make([]byte, 8))},
		{"hash count of billions", row(64, 1<<32-1, make([]byte, 8))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bf, err := DeserializeBloomFilter(tc.data)
			if err == nil {
				t.Fatalf("DeserializeBloomFilter accepted a corrupt row (filter %+v); the next Add would panic", bf)
			}
		})
	}

	t.Run("bit count at the slice edge is accepted", func(t *testing.T) {
		bf, err := DeserializeBloomFilter(row(64, 3, make([]byte, 8)))
		if err != nil {
			t.Fatalf("a row whose bit count fills its slice exactly was refused: %v", err)
		}
		bf.Add([]byte("edge"))
		if !bf.Contains([]byte("edge")) {
			t.Fatal("an element added to an edge-sized row is reported absent")
		}
	})

	t.Run("zero hash count is lifted to one", func(t *testing.T) {
		bf, err := DeserializeBloomFilter(row(64, 0, make([]byte, 8)))
		if err != nil {
			t.Fatalf("a row with a zero hash count was refused: %v", err)
		}
		if bf.Contains([]byte("never added")) {
			t.Fatal("an empty filter read from a row with a zero hash count reports an element present")
		}
	})
}
