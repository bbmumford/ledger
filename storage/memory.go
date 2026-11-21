/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	lad "github.com/bbmumford/ledger"
)

// MemoryLedger is an in-memory ledger implementation for scenarios where persistence
// is not required. Use cases include:
//   - Unit testing and integration testing
//   - Short-lived processes that don't need restart recovery
//   - Edge/embedded deployments with limited storage
//   - Apps with LADStorage="memory" that still want basic peer tracking
//
// Unlike LocalDBLedger, records are lost on restart. For production apps that need
// gossip synchronization and crash recovery, use LocalDBLedger instead.
type MemoryLedger struct {
	mu          sync.RWMutex
	seq         map[lad.Topic]uint64
	logs        map[lad.Topic][]lad.Record
	subscribers map[int]*subscriber
	nextSubID   int
	stopChan    chan struct{}
	stopOnce    sync.Once

	// Configuration
	maxRecordsPerTopic int           // Max records per topic (0 = unlimited)
	maxAge             time.Duration // Max age for records (0 = unlimited)
	gcTicker           *time.Ticker  // Background GC ticker

	// Metrics
	appendCount    uint64
	broadcastCount uint64
}

// MemoryLedgerConfig holds configuration for MemoryLedger
type MemoryLedgerConfig struct {
	// MaxRecordsPerTopic limits memory usage by capping records per topic.
	// When exceeded, oldest records are evicted. 0 = unlimited (use with caution).
	MaxRecordsPerTopic int

	// MaxAge defines how long records are kept. Records older than this are
	// evicted during GC. 0 = keep forever.
	MaxAge time.Duration

	// GCInterval defines how often garbage collection runs.
	// Only relevant if MaxAge > 0. 0 = disabled.
	GCInterval time.Duration
}

// DefaultMemoryLedgerConfig returns sensible defaults for production use
func DefaultMemoryLedgerConfig() MemoryLedgerConfig {
	return MemoryLedgerConfig{
		MaxRecordsPerTopic: 10000,         // 10k records per topic
		MaxAge:             1 * time.Hour, // Keep records for 1 hour
		GCInterval:         5 * time.Minute,
	}
}

// NewMemoryLedger constructs a ledger with the given configuration.
// Use DefaultMemoryLedgerConfig() for sensible defaults.
func NewMemoryLedger(cfg MemoryLedgerConfig) *MemoryLedger {
	ml := &MemoryLedger{
		seq:                make(map[lad.Topic]uint64),
		logs:               make(map[lad.Topic][]lad.Record),
		subscribers:        make(map[int]*subscriber),
		stopChan:           make(chan struct{}),
		maxRecordsPerTopic: cfg.MaxRecordsPerTopic,
		maxAge:             cfg.MaxAge,
	}

	// Start background GC if configured
	if cfg.GCInterval > 0 && (cfg.MaxAge > 0 || cfg.MaxRecordsPerTopic > 0) {
		ml.gcTicker = time.NewTicker(cfg.GCInterval)
		go ml.runGC()
	}

	return ml
}

// Close stops background goroutines and releases resources. It ends every
// subscription, so each Stream goroutine hands its consumer what its queue
// holds and closes its channel; subscribe registers nothing once stopChan is
// closed.
func (l *MemoryLedger) Close() error {
	l.stopOnce.Do(func() {
		close(l.stopChan)
		l.mu.Lock()
		for _, sub := range l.subscribers {
			sub.endFor(lad.ErrLedgerClosed)
		}
		l.mu.Unlock()
		if l.gcTicker != nil {
			l.gcTicker.Stop()
		}
	})
	return nil
}

// runGC runs periodic garbage collection
func (l *MemoryLedger) runGC() {
	for {
		select {
		case <-l.stopChan:
			return
		case <-l.gcTicker.C:
			l.gc()
		}
	}
}

// gc performs garbage collection based on maxAge and maxRecordsPerTopic
func (l *MemoryLedger) gc() {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()

	for topic, records := range l.logs {
		if len(records) == 0 {
			continue
		}

		// Find cutoff index for age-based eviction
		cutoff := 0
		if l.maxAge > 0 {
			for i, rec := range records {
				if now.Sub(rec.Timestamp) > l.maxAge {
					cutoff = i + 1
				} else {
					break
				}
			}
		}

		// Apply age-based eviction
		if cutoff > 0 {
			records = records[cutoff:]
		}

		// Apply count-based eviction
		if l.maxRecordsPerTopic > 0 && len(records) > l.maxRecordsPerTopic {
			excess := len(records) - l.maxRecordsPerTopic
			records = records[excess:]
		}

		l.logs[topic] = records
	}
}

// Stats returns current ledger statistics
func (l *MemoryLedger) Stats() MemoryLedgerStats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var totalRecords int
	for _, records := range l.logs {
		totalRecords += len(records)
	}

	return MemoryLedgerStats{
		Topics:         len(l.logs),
		TotalRecords:   totalRecords,
		Subscribers:    len(l.subscribers),
		AppendCount:    l.appendCount,
		BroadcastCount: l.broadcastCount,
	}
}

// MemoryLedgerStats provides observability into ledger state
type MemoryLedgerStats struct {
	Topics         int    // Number of unique topics
	TotalRecords   int    // Total records across all topics
	Subscribers    int    // Active subscribers
	AppendCount    uint64 // Total appends since creation
	BroadcastCount uint64 // Total broadcasts sent
}

// Head returns the latest sequence per topic.
func (l *MemoryLedger) Head(ctx context.Context) (lad.CausalWatermark, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	wm := make(lad.CausalWatermark, len(l.seq))
	for topic, seq := range l.seq {
		wm[topic] = seq
	}
	return wm, nil
}

// Append validates and stores a record. A record with no Seq is given the
// topic's next one under the lock that stores it. A signed record with no Seq
// is refused with lad.ErrSignedWithoutSeq: AppendSigned numbers before it signs.
func (l *MemoryLedger) Append(ctx context.Context, rec lad.Record) error {
	return l.append(ctx, rec, nil)
}

// AppendSigned is Append with sign called on the record after it is numbered
// and before it is stored, under the same lock (lad.SigningLedger).
func (l *MemoryLedger) AppendSigned(ctx context.Context, rec lad.Record, sign lad.SignFunc) error {
	if sign == nil {
		return fmt.Errorf("lad: AppendSigned needs a sign function")
	}
	return l.append(ctx, rec, sign)
}

func (l *MemoryLedger) append(ctx context.Context, rec lad.Record, sign lad.SignFunc) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.stopChan:
		return fmt.Errorf("lad: ledger closed")
	default:
	}
	if rec.Topic == "" {
		return fmt.Errorf("lad: missing topic")
	}
	if sign == nil && rec.Seq == 0 && len(rec.Signature) > 0 {
		return lad.ErrSignedWithoutSeq
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.seq[rec.Topic]
	if rec.Seq == 0 {
		rec.Seq = seq + 1
	} else if rec.Seq <= seq {
		return fmt.Errorf("lad: sequence regression for topic %s: %d <= %d", rec.Topic, rec.Seq, seq)
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}
	if sign != nil {
		if err := sign(&rec); err != nil {
			return fmt.Errorf("lad: sign record: %w", err)
		}
	}
	l.seq[rec.Topic] = rec.Seq
	l.logs[rec.Topic] = append(l.logs[rec.Topic], rec)
	l.appendCount++

	// Inline eviction if over limit (avoid waiting for GC)
	if l.maxRecordsPerTopic > 0 {
		if len(l.logs[rec.Topic]) > l.maxRecordsPerTopic {
			excess := len(l.logs[rec.Topic]) - l.maxRecordsPerTopic
			l.logs[rec.Topic] = l.logs[rec.Topic][excess:]
		}
	}

	subs := make([]*subscriber, 0, len(l.subscribers))
	for _, sub := range l.subscribers {
		subs = append(subs, sub)
	}
	l.broadcastCount++
	go broadcast(subs, rec)
	return nil
}

// BatchAppend appends multiple records atomically. A signed record with no
// Seq refuses the whole batch with lad.ErrSignedWithoutSeq.
func (l *MemoryLedger) BatchAppend(ctx context.Context, records []lad.Record) error {
	return l.batchAppend(ctx, records, nil)
}

// BatchAppendSigned is BatchAppend with sign called on each record after it is
// numbered and before any is stored (lad.SigningLedger).
func (l *MemoryLedger) BatchAppendSigned(ctx context.Context, records []lad.Record, sign lad.SignFunc) error {
	if sign == nil {
		return fmt.Errorf("lad: BatchAppendSigned needs a sign function")
	}
	return l.batchAppend(ctx, records, sign)
}

func (l *MemoryLedger) batchAppend(ctx context.Context, records []lad.Record, sign lad.SignFunc) error {
	if len(records) == 0 {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.stopChan:
		return fmt.Errorf("lad: ledger closed")
	default:
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Validate, number and sign every record against a copy of the topic
	// heads, so a batch refused at any record changes no head.
	seqs := make(map[lad.Topic]uint64, len(l.seq))
	for topic, seq := range l.seq {
		seqs[topic] = seq
	}
	for i := range records {
		rec := &records[i]

		if rec.Topic == "" {
			return fmt.Errorf("lad: missing topic in record %d", i)
		}
		if sign == nil && rec.Seq == 0 && len(rec.Signature) > 0 {
			return fmt.Errorf("lad: record %d: %w", i, lad.ErrSignedWithoutSeq)
		}

		seq := seqs[rec.Topic]
		if rec.Seq == 0 {
			rec.Seq = seq + 1
		} else if rec.Seq <= seq {
			return fmt.Errorf("lad: sequence regression for topic %s in record %d: %d <= %d",
				rec.Topic, i, rec.Seq, seq)
		}

		if rec.Timestamp.IsZero() {
			rec.Timestamp = time.Now().UTC()
		}
		if sign != nil {
			if err := sign(rec); err != nil {
				return fmt.Errorf("lad: sign record %d: %w", i, err)
			}
		}

		seqs[rec.Topic] = rec.Seq
	}
	l.seq = seqs

	// Append all records
	for _, rec := range records {
		l.logs[rec.Topic] = append(l.logs[rec.Topic], rec)
		l.appendCount++
	}

	// Apply eviction per topic
	if l.maxRecordsPerTopic > 0 {
		for topic := range l.logs {
			if len(l.logs[topic]) > l.maxRecordsPerTopic {
				excess := len(l.logs[topic]) - l.maxRecordsPerTopic
				l.logs[topic] = l.logs[topic][excess:]
			}
		}
	}

	// Notify subscribers
	subs := make([]*subscriber, 0, len(l.subscribers))
	for _, sub := range l.subscribers {
		subs = append(subs, sub)
	}
	l.broadcastCount += uint64(len(records))

	// Broadcast all records asynchronously
	go func() {
		for _, rec := range records {
			broadcast(subs, rec)
		}
	}()

	return nil
}

// Stream forwards historical and live records to the caller.
// The returned channel is closed when the context is cancelled or the ledger
// is closed. It is StreamWithEnd without the reason the stream ended.
func (l *MemoryLedger) Stream(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) (<-chan lad.Record, error) {
	out, _, err := l.StreamWithEnd(ctx, from, topics)
	return out, err
}

// StreamWithEnd forwards the records past from on the given topics (all
// topics when none is given): first the history the ledger holds, then live
// ones as Append and BatchAppend store them. The returned StreamEnd holds
// why the channel closed: ctx's error, lad.ErrLedgerClosed or
// lad.ErrStreamOverflow. A closed ledger refuses with lad.ErrLedgerClosed.
//
// The subscription registers and the history is copied under one hold of
// l.mu, the lock Append and BatchAppend store and take their subscriber copy
// under, so each record reaches the stream exactly once: in the copy if it
// was stored before, through the live queue if after.
func (l *MemoryLedger) StreamWithEnd(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) (<-chan lad.Record, *lad.StreamEnd, error) {
	topicSet := make(map[lad.Topic]struct{}, len(topics))
	for _, topic := range topics {
		if topic != "" {
			topicSet[topic] = struct{}{}
		}
	}
	filter := make(map[lad.Topic]struct{}, len(topicSet))
	for k, v := range topicSet {
		filter[k] = v
	}

	sub, history := l.subscribeWithHistory(filter, from)
	if sub == nil {
		return nil, nil, lad.ErrLedgerClosed
	}

	out := make(chan lad.Record, 64)
	end := &lad.StreamEnd{}

	go func() {
		defer close(out)
		// Close ends the subscription, and forward hands over what its
		// queue holds before out closes.
		defer l.unsubscribe(sub)

		for _, rec := range history {
			select {
			case out <- rec:
			case <-ctx.Done():
				end.Set(ctx.Err())
				return
			}
		}

		end.Set(forward(ctx, sub, out))
	}()

	return out, end, nil
}

// subscribeWithHistory registers a subscriber for filter (all topics when
// empty) and copies the records past from on those topics, both under l.mu,
// so no record stored meanwhile misses both. It returns a nil subscriber once
// Close has closed stopChan.
func (l *MemoryLedger) subscribeWithHistory(filter map[lad.Topic]struct{}, from lad.CausalWatermark) (*subscriber, []lad.Record) {
	l.mu.Lock()
	defer l.mu.Unlock()

	sub := l.subscribeLocked(filter)
	if sub == nil {
		return nil, nil
	}
	history := make([]lad.Record, 0)
	for topic, records := range l.logs {
		if len(filter) > 0 {
			if _, ok := filter[topic]; !ok {
				continue
			}
		}
		start := from[topic]
		for _, rec := range records {
			if rec.Seq <= start {
				continue
			}
			history = append(history, rec)
		}
	}
	return sub, history
}

// subscribe registers a subscriber for live updates, or returns nil once
// Close has closed stopChan: Close ends only the subscribers it finds, so one
// registered after it would never end.
func (l *MemoryLedger) subscribe(topics map[lad.Topic]struct{}) *subscriber {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.subscribeLocked(topics)
}

// subscribeLocked is subscribe for a caller that holds l.mu.
func (l *MemoryLedger) subscribeLocked(topics map[lad.Topic]struct{}) *subscriber {
	select {
	case <-l.stopChan:
		return nil
	default:
	}

	sub := newSubscriber(l.nextSubID, topics)
	l.nextSubID++
	l.subscribers[sub.id] = sub
	return sub
}

// unsubscribe ends a subscriber and removes it; the Stream goroutine that
// registered it defers this. The broadcast goroutine Append or BatchAppend
// starts may hold the subscriber from a copy taken before the removal, and end
// makes that send a no-op.
func (l *MemoryLedger) unsubscribe(sub *subscriber) {
	l.mu.Lock()
	defer l.mu.Unlock()

	sub.end()
	delete(l.subscribers, sub.id)
}

// Snapshot returns a JSON representation of the ledger for debugging and persistence.
// The snapshot can be used to restore state via RestoreSnapshot.
func (l *MemoryLedger) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.stopChan:
		return nil, fmt.Errorf("lad: ledger closed")
	default:
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	snapshot := struct {
		Seq   map[lad.Topic]uint64       `json:"seq"`
		Logs  map[lad.Topic][]lad.Record `json:"logs"`
		Stats MemoryLedgerStats          `json:"stats"`
	}{
		Seq:  make(map[lad.Topic]uint64, len(l.seq)),
		Logs: make(map[lad.Topic][]lad.Record, len(l.logs)),
		Stats: MemoryLedgerStats{
			Topics:         len(l.logs),
			Subscribers:    len(l.subscribers),
			AppendCount:    l.appendCount,
			BroadcastCount: l.broadcastCount,
		},
	}
	var totalRecords int
	for topic, seq := range l.seq {
		snapshot.Seq[topic] = seq
	}
	for topic, records := range l.logs {
		copyRecords := make([]lad.Record, len(records))
		copy(copyRecords, records)
		snapshot.Logs[topic] = copyRecords
		totalRecords += len(records)
	}
	snapshot.Stats.TotalRecords = totalRecords
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

// RestoreSnapshot restores ledger state from a snapshot created by Snapshot().
// This is useful for warm-starting a MemoryLedger from persisted state.
func (l *MemoryLedger) RestoreSnapshot(ctx context.Context, r io.Reader) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.stopChan:
		return fmt.Errorf("lad: ledger closed")
	default:
	}

	var snapshot struct {
		Seq  map[lad.Topic]uint64       `json:"seq"`
		Logs map[lad.Topic][]lad.Record `json:"logs"`
	}

	if err := json.NewDecoder(r).Decode(&snapshot); err != nil {
		return fmt.Errorf("lad: decode snapshot: %w", err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Restore sequences
	for topic, seq := range snapshot.Seq {
		l.seq[topic] = seq
	}

	// Restore logs
	for topic, records := range snapshot.Logs {
		l.logs[topic] = records
	}

	return nil
}

// Clear removes all records from the ledger. Useful for testing or reset scenarios.
func (l *MemoryLedger) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.seq = make(map[lad.Topic]uint64)
	l.logs = make(map[lad.Topic][]lad.Record)
}
