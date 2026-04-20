/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// DiskState is the headroom-based quota input. Values are in bytes except
// where a unit is named explicitly (e.g. GB).
//
// The algorithm below treats headroom as the gap between the configured
// max-usage ceiling (MaxPct of total) and what non-cache processes have
// already consumed. Our budget is a fraction (TargetHeadroomPct) of that
// gap. Two absolute floors are applied: MinReserveGB (never let free drop
// below this) and HardMinGB (below this free, disable entirely).
type DiskState struct {
	TotalBytes        int64
	FreeBytes         int64
	OurStorage        int64 // bytes we've stored in our cache
	MaxPct            int   // 70 default  -- total-disk ceiling
	TargetHeadroomPct int   // 50 default  -- fraction of headroom we claim
	MinReserveGB      int   // 5  default  -- absolute free floor
	HardMinGB         int   // 1  default  -- below this, disable entirely
}

// DefaultDiskState returns a DiskState pre-populated with the recommended
// policy defaults (caller still fills in Total/Free/OurStorage).
func DefaultDiskState() DiskState {
	return DiskState{
		MaxPct:            70,
		TargetHeadroomPct: 50,
		MinReserveGB:      5,
		HardMinGB:         1,
	}
}

// ComputeBudget returns the number of additional bytes the cache is
// permitted to hold given the current disk state. See the exact formula
// documented in _OTHER/mercury/roadmap.md section 2. The function is pure.
func (d DiskState) ComputeBudget() int64 {
	external := d.TotalBytes - d.FreeBytes - d.OurStorage
	if external < 0 {
		external = 0
	}
	maxAllowed := d.TotalBytes * int64(d.MaxPct) / 100
	headroom := maxAllowed - external
	if headroom < 0 {
		headroom = 0
	}
	budget := headroom * int64(d.TargetHeadroomPct) / 100

	minReserveBytes := int64(d.MinReserveGB) * 1024 * 1024 * 1024
	if d.FreeBytes-budget < minReserveBytes {
		budget = d.FreeBytes - minReserveBytes
	}
	if budget < 0 {
		budget = 0
	}
	return budget
}

// CanStore reports whether the node should accept any new writes at all.
// Returns false when free space is below HardMinGB or the computed budget
// is zero.
func (d DiskState) CanStore() bool {
	hardMinBytes := int64(d.HardMinGB) * 1024 * 1024 * 1024
	return d.FreeBytes >= hardMinBytes && d.ComputeBudget() > 0
}

// Pressure returns a 0.0..1.0 gauge of how close the cache is to the
// configured MaxPct ceiling. 0.0 means there's plenty of headroom; 1.0
// means we're at or over the ceiling and should evict.
func (d DiskState) Pressure() float64 {
	if d.TotalBytes <= 0 || d.MaxPct <= 0 {
		return 0
	}
	external := d.TotalBytes - d.FreeBytes - d.OurStorage
	if external < 0 {
		external = 0
	}
	maxAllowed := d.TotalBytes * int64(d.MaxPct) / 100
	if maxAllowed <= 0 {
		return 0
	}
	// Fraction of the ceiling consumed by external usage. Our own storage
	// does not contribute because the cache voluntarily shrinks under
	// pressure -- pressure measures how much outside-our-control load is
	// eating the ceiling.
	p := float64(external) / float64(maxAllowed)
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	return p
}

// DiskStatter produces a raw disk snapshot for the path backing the cache.
// Implementations live in quota_native.go / quota_wasm.go.
type DiskStatter interface {
	DiskStats(path string) (totalBytes, freeBytes int64, err error)
}

// ManagerConfig collects the tunables that don't depend on the transient
// disk snapshot.
type ManagerConfig struct {
	MaxPct            int           // default 70
	TargetHeadroomPct int           // default 50
	MinReserveGB      int           // default 5
	HardMinGB         int           // default 1
	PollInterval      time.Duration // default 30s
}

// DefaultManagerConfig returns the roadmap defaults.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		MaxPct:            70,
		TargetHeadroomPct: 50,
		MinReserveGB:      5,
		HardMinGB:         1,
		PollInterval:      30 * time.Second,
	}
}

func (c ManagerConfig) normalized() ManagerConfig {
	if c.MaxPct <= 0 {
		c.MaxPct = 70
	}
	if c.TargetHeadroomPct <= 0 {
		c.TargetHeadroomPct = 50
	}
	if c.MinReserveGB <= 0 {
		c.MinReserveGB = 5
	}
	if c.HardMinGB <= 0 {
		c.HardMinGB = 1
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 30 * time.Second
	}
	return c
}

// LowBudgetCallback is invoked when a Refresh observes the budget has
// fallen low enough that the cache should start evicting. The argument is
// the current computed budget in bytes; zero means "stop accepting writes
// entirely". Called from the poll goroutine -- callers should not block.
type LowBudgetCallback func(budget int64)

// Manager polls disk state on an interval and serves the current quota
// answer to callers. Safe for concurrent use.
type Manager struct {
	cfg     ManagerConfig
	path    string
	statter DiskStatter

	ourStorage int64 // atomic; bytes we currently cache
	budget     int64 // atomic; last-computed budget
	canStore   int32 // atomic 0/1

	mu            sync.RWMutex
	lastState     DiskState
	lastStateTime time.Time

	cbMu sync.Mutex
	cb   LowBudgetCallback

	stopOnce sync.Once
	stopC    chan struct{}
	startC   sync.Once
}

// NewManager constructs a quota Manager. Pass statter=nil to use the
// platform default (unix Statfs / GetDiskFreeSpaceEx / navigator.storage).
func NewManager(cfg ManagerConfig, cachePath string, statter DiskStatter) *Manager {
	cfg = cfg.normalized()
	if statter == nil {
		statter = defaultStatter{}
	}
	m := &Manager{
		cfg:     cfg,
		path:    cachePath,
		statter: statter,
		stopC:   make(chan struct{}),
	}
	_ = m.Refresh() // best-effort initial snapshot
	return m
}

// SetOurStorage records the bytes currently cached by this node.
func (m *Manager) SetOurStorage(bytes int64) {
	atomic.StoreInt64(&m.ourStorage, bytes)
	_ = m.Refresh()
}

// AddOurStorage atomically adjusts the cached-bytes counter.
func (m *Manager) AddOurStorage(delta int64) {
	atomic.AddInt64(&m.ourStorage, delta)
	_ = m.Refresh()
}

// OurStorage returns the caller-provided cached-bytes total.
func (m *Manager) OurStorage() int64 { return atomic.LoadInt64(&m.ourStorage) }

// Refresh polls disk stats and recomputes the budget.
func (m *Manager) Refresh() error {
	total, free, err := m.statter.DiskStats(m.path)
	if err != nil {
		return err
	}
	ours := atomic.LoadInt64(&m.ourStorage)
	d := DiskState{
		TotalBytes:        total,
		FreeBytes:         free,
		OurStorage:        ours,
		MaxPct:            m.cfg.MaxPct,
		TargetHeadroomPct: m.cfg.TargetHeadroomPct,
		MinReserveGB:      m.cfg.MinReserveGB,
		HardMinGB:         m.cfg.HardMinGB,
	}
	budget := d.ComputeBudget()
	canStore := d.CanStore()
	atomic.StoreInt64(&m.budget, budget)
	if canStore {
		atomic.StoreInt32(&m.canStore, 1)
	} else {
		atomic.StoreInt32(&m.canStore, 0)
	}
	m.mu.Lock()
	m.lastState = d
	m.lastStateTime = time.Now()
	m.mu.Unlock()

	// Fire the low-budget callback when we've dropped to zero or the
	// current cached bytes exceed the budget (eviction required).
	m.cbMu.Lock()
	cb := m.cb
	m.cbMu.Unlock()
	if cb != nil && (budget == 0 || ours > budget) {
		cb(budget)
	}
	return nil
}

// CurrentBudget returns the most recently computed budget in bytes.
func (m *Manager) CurrentBudget() int64 { return atomic.LoadInt64(&m.budget) }

// CanStore reports whether new writes are permitted at all. Combined with
// CurrentBudget, callers can decide whether a particular N-byte write fits.
func (m *Manager) CanStore() bool { return atomic.LoadInt32(&m.canStore) == 1 }

// HasRoomFor reports whether nBytes can be added within the current budget.
func (m *Manager) HasRoomFor(nBytes int64) bool {
	if !m.CanStore() {
		return false
	}
	cur := atomic.LoadInt64(&m.ourStorage)
	return cur+nBytes <= atomic.LoadInt64(&m.budget)
}

// Pressure reports the latest pressure value (see DiskState.Pressure).
func (m *Manager) Pressure() float64 {
	m.mu.RLock()
	d := m.lastState
	m.mu.RUnlock()
	return d.Pressure()
}

// LastState returns a snapshot of the most recent disk reading.
func (m *Manager) LastState() DiskState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastState
}

// OnLowBudget registers a callback invoked when a Refresh observes a
// too-low budget (either zero or below current cached bytes).
func (m *Manager) OnLowBudget(cb LowBudgetCallback) {
	m.cbMu.Lock()
	m.cb = cb
	m.cbMu.Unlock()
}

// Start launches the background polling goroutine. Idempotent.
func (m *Manager) Start(ctx context.Context) {
	m.startC.Do(func() {
		go m.loop(ctx)
	})
}

// Stop halts the poll loop.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() { close(m.stopC) })
}

func (m *Manager) loop(ctx context.Context) {
	t := time.NewTicker(m.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopC:
			return
		case <-t.C:
			_ = m.Refresh()
		}
	}
}

// Config exposes a read-only view of the manager's configuration.
func (m *Manager) Config() ManagerConfig { return m.cfg }
