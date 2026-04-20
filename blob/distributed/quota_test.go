/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"testing"
	"time"
)

const gb = int64(1024) * 1024 * 1024

// TestComputeBudgetWorkedExamples pins ComputeBudget against every scenario
// in the task spec. Any drift here means the quota math has regressed.
func TestComputeBudgetWorkedExamples(t *testing.T) {
	type row struct {
		name               string
		totalGB, freeGB, ourGB int64
		maxPct             int
		targetPct          int
		minReserveGB       int
		hardMinGB          int
		wantBudget         int64 // in bytes
		wantCanStore       bool
	}
	half := gb / 2
	cases := []row{
		{"100G free/empty", 100, 100, 0, 70, 50, 5, 1, 35 * gb, true},
		{"100G free=70", 100, 70, 0, 70, 50, 5, 1, 20 * gb, true},
		{"100G free=50", 100, 50, 0, 70, 50, 5, 1, 10 * gb, true},
		{"100G free=35", 100, 35, 0, 70, 50, 5, 1, 2*gb + half, true},
		{"100G free=30 (at ceiling)", 100, 30, 0, 70, 50, 5, 1, 0, false},
		{"100G free=15 (over ceiling)", 100, 15, 0, 70, 50, 5, 1, 0, false},
		{"100G free=80 ourStorage=20 (full headroom)", 100, 80, 20, 70, 50, 5, 1, 35 * gb, true},
		{"10G free=3 (MinReserve dominates)", 10, 3, 0, 70, 50, 5, 1, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := DiskState{
				TotalBytes:        c.totalGB * gb,
				FreeBytes:         c.freeGB * gb,
				OurStorage:        c.ourGB * gb,
				MaxPct:            c.maxPct,
				TargetHeadroomPct: c.targetPct,
				MinReserveGB:      c.minReserveGB,
				HardMinGB:         c.hardMinGB,
			}
			gotBudget := d.ComputeBudget()
			if gotBudget != c.wantBudget {
				t.Errorf("ComputeBudget: got %d bytes, want %d bytes (%.2f GB vs %.2f GB)",
					gotBudget, c.wantBudget,
					float64(gotBudget)/float64(gb),
					float64(c.wantBudget)/float64(gb))
			}
			gotCan := d.CanStore()
			if gotCan != c.wantCanStore {
				t.Errorf("CanStore: got %v, want %v", gotCan, c.wantCanStore)
			}
		})
	}
}

func TestPressureBoundaries(t *testing.T) {
	// Empty disk -> pressure 0.
	empty := DiskState{
		TotalBytes: 100 * gb, FreeBytes: 100 * gb, OurStorage: 0,
		MaxPct: 70, TargetHeadroomPct: 50, MinReserveGB: 5, HardMinGB: 1,
	}
	if p := empty.Pressure(); p != 0 {
		t.Errorf("empty pressure: got %v want 0", p)
	}

	// External usage at ceiling -> pressure 1.0.
	atCeiling := DiskState{
		TotalBytes: 100 * gb, FreeBytes: 30 * gb, OurStorage: 0,
		MaxPct: 70, TargetHeadroomPct: 50, MinReserveGB: 5, HardMinGB: 1,
	}
	if p := atCeiling.Pressure(); p != 1.0 {
		t.Errorf("at-ceiling pressure: got %v want 1.0", p)
	}

	// External usage beyond ceiling -> clamped to 1.0.
	over := DiskState{
		TotalBytes: 100 * gb, FreeBytes: 15 * gb, OurStorage: 0,
		MaxPct: 70, TargetHeadroomPct: 50, MinReserveGB: 5, HardMinGB: 1,
	}
	if p := over.Pressure(); p != 1.0 {
		t.Errorf("over-ceiling pressure: got %v want 1.0 (clamped)", p)
	}
}

// fakeStatter lets us drive the Manager deterministically.
type fakeStatter struct {
	total, free int64
}

func (f *fakeStatter) DiskStats(path string) (int64, int64, error) {
	return f.total, f.free, nil
}

func TestManagerLifecycle(t *testing.T) {
	fs := &fakeStatter{total: 100 * gb, free: 100 * gb}
	cfg := DefaultManagerConfig()
	cfg.PollInterval = 10 * time.Millisecond
	m := NewManager(cfg, "", fs)

	if got := m.CurrentBudget(); got != 35*gb {
		t.Errorf("initial budget: got %d want %d", got, 35*gb)
	}
	if !m.CanStore() {
		t.Error("should be able to store on empty disk")
	}
	if !m.HasRoomFor(10 * gb) {
		t.Error("expected room for 10 GB")
	}
	if m.HasRoomFor(40 * gb) {
		t.Error("should NOT have room for 40 GB when budget is 35 GB")
	}

	// Low-budget callback when free drops and budget -> 0.
	ch := make(chan int64, 1)
	m.OnLowBudget(func(budget int64) {
		select {
		case ch <- budget:
		default:
		}
	})
	fs.free = 15 * gb
	if err := m.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	select {
	case got := <-ch:
		if got != 0 {
			t.Errorf("callback budget: got %d want 0", got)
		}
	case <-time.After(time.Second):
		t.Fatal("low-budget callback never fired")
	}
}
