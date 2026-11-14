/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/internal/bloom"
)

// LocalDBLedger implements the Ledger interface using a local embedded libSQL database.
// This is used for LAD peer discovery records - ephemeral data that survives restarts
// but is NOT distributed via Raft. Synchronization happens via LAD gossip protocol.
type LocalDBLedger struct {
	db          *sql.DB
	mu          sync.RWMutex
	subscribers map[int]*subscriber
	nextSubID   int
	stopChan    chan struct{}
	stopOnce    sync.Once

	// Compaction configuration
	retentionDays int
	compactTicker *time.Ticker

	// Bloom filter manager for efficient queries
	bloomMgr    *bloom.BloomFilterManager
	bloomTicker *time.Ticker
}

// LedgerConfig holds configuration for LocalDBLedger
type LedgerConfig struct {
	RetentionDays      int               // Keep records for this many days (0 = keep forever)
	CompactionInterval time.Duration     // How often to run compaction (0 = disabled)
	EnableBloomFilters bool              // Enable Bloom filters for query optimization
	BloomConfig        bloom.BloomConfig // Bloom filter configuration
}

// DefaultLedgerConfig returns sensible defaults
func DefaultLedgerConfig() LedgerConfig {
	return LedgerConfig{
		RetentionDays:      30,             // Keep 30 days of history
		CompactionInterval: 24 * time.Hour, // Compact daily
		EnableBloomFilters: true,           // Enable Bloom filters
		BloomConfig:        bloom.DefaultBloomConfig(),
	}
}

// RequirePartitionColumn refuses a ladRecords table that keeps each record's
// partition in a tenantId column. Every statement in this ledger and in loom's
// mesh ledger reads and writes partitionId, and CREATE TABLE IF NOT EXISTS
// leaves an existing table as it is, so such a table would fail on the
// partitionId index. The table is not rewritten: the error tells the operator
// to delete the ledger database so it is created again. A missing table passes.
func RequirePartitionColumn(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(ladRecords)`)
	if err != nil {
		return fmt.Errorf("lad: read ladRecords columns: %w", err)
	}
	defer rows.Close()

	var hasTenantColumn, hasPartitionColumn bool
	for rows.Next() {
		var (
			cid        int
			name, kind string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultVal, &pk); err != nil {
			return fmt.Errorf("lad: read ladRecords columns: %w", err)
		}
		switch name {
		case "tenantId":
			hasTenantColumn = true
		case "partitionId":
			hasPartitionColumn = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("lad: read ladRecords columns: %w", err)
	}
	if hasTenantColumn && !hasPartitionColumn {
		return fmt.Errorf("lad: ladRecords keeps the partition in a tenantId column, " +
			"but this ledger reads partitionId; delete the ledger database so it is created again")
	}
	return nil
}

// NewLocalDBLedger creates a persistent ledger backed by a local embedded libSQL database.
// Pass DefaultLedgerConfig() for sensible defaults, or customize retention and compaction.
//
// The schema is automatically initialized:
//
//	CREATE TABLE IF NOT EXISTS ladRecords (
//	    topic TEXT NOT NULL,
//	    seq INTEGER NOT NULL,
//	    partitionId TEXT NOT NULL,
//	    nodeId TEXT NOT NULL,
//	    body TEXT NOT NULL,
//	    signature BLOB,
//	    timestamp TEXT NOT NULL,
//	    PRIMARY KEY (topic, seq)
//	);
func NewLocalDBLedger(db *sql.DB, cfg LedgerConfig) (*LocalDBLedger, error) {
	if db == nil {
		return nil, fmt.Errorf("lad: database connection required")
	}
	if err := RequirePartitionColumn(context.Background(), db); err != nil {
		return nil, err
	}

	// Initialize schema
	schema := `
		CREATE TABLE IF NOT EXISTS ladRecords (
			topic TEXT NOT NULL,
			seq INTEGER NOT NULL,
			partitionId TEXT NOT NULL,
			nodeId TEXT NOT NULL,
			body TEXT NOT NULL,
			signature BLOB,
			timestamp TEXT NOT NULL,
			PRIMARY KEY (topic, seq)
		);
		CREATE INDEX IF NOT EXISTS idxLadPartition ON ladRecords(partitionId, topic, seq);
		CREATE INDEX IF NOT EXISTS idxLadNode ON ladRecords(nodeId, topic, seq);
		CREATE INDEX IF NOT EXISTS idxLadTimestamp ON ladRecords(timestamp DESC);
		
		-- Bloom filter table for efficient existence checks
		CREATE TABLE IF NOT EXISTS ladBloomFilters (
			scope TEXT NOT NULL,
			filterData BLOB NOT NULL,
			updatedAt TEXT NOT NULL,
			PRIMARY KEY (scope)
		);
	`

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("lad: failed to initialize schema: %w", err)
	}

	ledger := &LocalDBLedger{
		db:            db,
		subscribers:   make(map[int]*subscriber),
		stopChan:      make(chan struct{}),
		retentionDays: cfg.RetentionDays,
	}

	// Initialize Bloom filter manager if enabled
	if cfg.EnableBloomFilters {
		ledger.bloomMgr = bloom.NewBloomFilterManager(cfg.BloomConfig)

		// Load existing Bloom filters from database
		if err := ledger.loadBloomFilters(); err != nil {
			// Log error but don't fail initialization
			// Bloom filters will be rebuilt on first rebuild cycle
		}

		// Start periodic rebuild if configured
		if cfg.BloomConfig.RebuildInterval > 0 {
			ledger.bloomTicker = time.NewTicker(cfg.BloomConfig.RebuildInterval)
			go ledger.runBloomRebuild()
		}
	}

	// Start background compaction if configured
	if cfg.CompactionInterval > 0 && cfg.RetentionDays > 0 {
		ledger.compactTicker = time.NewTicker(cfg.CompactionInterval)
		go ledger.runCompaction()
	}

	return ledger, nil
}

// Head returns the latest sequence per topic.
func (l *LocalDBLedger) Head(ctx context.Context) (lad.CausalWatermark, error) {
	query := `
		SELECT topic, MAX(seq) as max_seq
		FROM ladRecords
		GROUP BY topic
	`

	rows, err := l.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("lad: query head: %w", err)
	}
	defer rows.Close()

	wm := make(lad.CausalWatermark)
	for rows.Next() {
		var topic string
		var seq uint64
		if err := rows.Scan(&topic, &seq); err != nil {
			return nil, fmt.Errorf("scan head: %w", err)
		}
		wm[lad.Topic(topic)] = seq
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lad: rows error: %w", err)
	}

	return wm, nil
}

// Append validates and stores a record with Raft-replicated persistence.
func (l *LocalDBLedger) Append(ctx context.Context, rec lad.Record) error {
	if rec.Topic == "" {
		return fmt.Errorf("lad: missing topic")
	}

	// Auto-assign sequence if not set
	if rec.Seq == 0 {
		head, err := l.Head(ctx)
		if err != nil {
			return fmt.Errorf("lad: get head for seq: %w", err)
		}
		rec.Seq = head[rec.Topic] + 1
	}

	// Auto-set timestamp if not provided
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}

	// Insert record into database
	query := `
		INSERT INTO ladRecords (topic, seq, partitionId, nodeId, body, signature, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`

	bodyJSON, err := json.Marshal(rec.Body)
	if err != nil {
		return fmt.Errorf("lad: marshal body: %w", err)
	}

	_, err = l.db.ExecContext(ctx, query,
		string(rec.Topic),
		rec.Seq,
		rec.Partition,
		rec.NodeID,
		string(bodyJSON),
		rec.Signature,
		rec.Timestamp.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("lad: insert record: %w", err)
	}

	// Update Bloom filter if enabled
	if l.bloomMgr != nil {
		l.updateBloomFilter(rec)
	}

	// Notify subscribers
	l.broadcast(rec)

	return nil
}

// BatchAppend appends multiple records in a single transaction for high-throughput scenarios.
// Records are assigned sequences atomically and subscribers are notified after commit.
// Maximum batch size is 1000 records to avoid large transactions.
func (l *LocalDBLedger) BatchAppend(ctx context.Context, records []lad.Record) error {
	if len(records) == 0 {
		return nil
	}

	const maxBatchSize = 1000
	if len(records) > maxBatchSize {
		return fmt.Errorf("lad: batch size %d exceeds maximum %d", len(records), maxBatchSize)
	}

	// Create a transaction with 5-second timeout
	txCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := l.db.BeginTx(txCtx, nil)
	if err != nil {
		return fmt.Errorf("lad: begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback on error

	// Get current head for each topic to assign sequences
	head, err := l.Head(ctx)
	if err != nil {
		return fmt.Errorf("lad: get head for batch: %w", err)
	}

	// Track next sequence per topic within this batch
	nextSeq := make(map[lad.Topic]uint64)
	for topic, seq := range head {
		nextSeq[topic] = seq + 1
	}

	// Prepare insert statement
	stmt, err := tx.PrepareContext(txCtx, `
		INSERT INTO ladRecords (topic, seq, partitionId, nodeId, body, signature, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("lad: prepare batch insert: %w", err)
	}
	defer stmt.Close()

	// Process each record
	processedRecords := make([]lad.Record, 0, len(records))
	for i := range records {
		rec := &records[i]

		if rec.Topic == "" {
			return fmt.Errorf("lad: missing topic in record %d", i)
		}

		// Auto-assign sequence if not set
		if rec.Seq == 0 {
			rec.Seq = nextSeq[rec.Topic]
			nextSeq[rec.Topic]++
		} else {
			// If sequence is manually set, update our tracking
			if rec.Seq >= nextSeq[rec.Topic] {
				nextSeq[rec.Topic] = rec.Seq + 1
			}
		}

		// Auto-set timestamp if not provided
		if rec.Timestamp.IsZero() {
			rec.Timestamp = time.Now().UTC()
		}

		// Marshal body
		bodyJSON, err := json.Marshal(rec.Body)
		if err != nil {
			return fmt.Errorf("lad: marshal body for record %d: %w", i, err)
		}

		// Execute insert
		_, err = stmt.ExecContext(txCtx,
			string(rec.Topic),
			rec.Seq,
			rec.Partition,
			rec.NodeID,
			string(bodyJSON),
			rec.Signature,
			rec.Timestamp.Format(time.RFC3339Nano),
		)
		if err != nil {
			return fmt.Errorf("lad: insert record %d: %w", i, err)
		}

		processedRecords = append(processedRecords, *rec)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("lad: commit batch: %w", err)
	}

	// Update Bloom filters if enabled
	if l.bloomMgr != nil {
		for _, rec := range processedRecords {
			l.updateBloomFilter(rec)
		}
	}

	// Notify subscribers after successful commit
	for _, rec := range processedRecords {
		l.broadcast(rec)
	}

	return nil
}

// Stream forwards historical and live records to the caller. It is
// StreamWithEnd without the reason the stream ended.
func (l *LocalDBLedger) Stream(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) (<-chan lad.Record, error) {
	out, _, err := l.StreamWithEnd(ctx, from, topics)
	return out, err
}

// StreamWithEnd forwards the records past from on the given topics (all
// topics when none is given): first those the history query reads, then
// live ones as Append and BatchAppend commit them. The returned StreamEnd
// holds why the channel closed: ctx's error, lad.ErrLedgerClosed,
// lad.ErrStreamOverflow, or the history query's error.
//
// The subscription registers before the history query runs, so a record
// committed while the query reads reaches the stream by one path or both:
// the query's rows, the live queue, or each. A record arriving twice is the
// price of losing none, and the consumer applies a repeat harmlessly.
func (l *LocalDBLedger) StreamWithEnd(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) (<-chan lad.Record, *lad.StreamEnd, error) {
	topicSet := make(map[lad.Topic]struct{}, len(topics))
	for _, topic := range topics {
		if topic != "" {
			topicSet[topic] = struct{}{}
		}
	}

	out := make(chan lad.Record, 64)
	end := &lad.StreamEnd{}

	go func() {
		defer close(out)

		// Register for live updates first. Close ends the subscription,
		// and forward hands over what its queue holds before out closes.
		subID := l.subscribe(topicSet)
		if subID < 0 {
			end.Set(lad.ErrLedgerClosed)
			return
		}
		defer l.unsubscribe(subID)
		sub := l.getSubscriber(subID)
		if sub == nil {
			// Close ran between subscribe and here and dropped the map.
			end.Set(lad.ErrLedgerClosed)
			return
		}

		if err := l.streamHistory(ctx, from, topicSet, out); err != nil {
			end.Set(err)
			return
		}

		end.Set(forward(ctx, sub, out))
	}()

	return out, end, nil
}

// streamHistory sends the stored records past from on topicSet (all topics
// when it is empty) to out, in timestamp order. It returns ctx's error when
// the consumer's context ends first, and the query's error, wrapped, when
// the read fails; StreamWithEnd ends the stream with either.
func (l *LocalDBLedger) streamHistory(ctx context.Context, from lad.CausalWatermark, topicSet map[lad.Topic]struct{}, out chan<- lad.Record) error {
	// Build WHERE clause for topic filter
	var topicFilter string
	var args []interface{}
	if len(topicSet) > 0 {
		topicFilter = " AND topic IN ("
		first := true
		for topic := range topicSet {
			if !first {
				topicFilter += ", "
			}
			topicFilter += "?"
			args = append(args, string(topic))
			first = false
		}
		topicFilter += ")"
	}

	// Query historical records
	query := fmt.Sprintf(`
		SELECT topic, seq, partitionId, nodeId, body, signature, timestamp
		FROM ladRecords
		WHERE 1=1 %s
		ORDER BY timestamp ASC, topic ASC, seq ASC
	`, topicFilter)

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("lad: stream history query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var rec lad.Record
		var topicStr, bodyStr, timestampStr string

		if err := rows.Scan(&topicStr, &rec.Seq, &rec.Partition, &rec.NodeID, &bodyStr, &rec.Signature, &timestampStr); err != nil {
			return fmt.Errorf("lad: stream history scan: %w", err)
		}

		rec.Topic = lad.Topic(topicStr)
		rec.Body = json.RawMessage(bodyStr)
		rec.Timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)

		// Skip if before watermark
		if from != nil && rec.Seq <= from[rec.Topic] {
			continue
		}

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("lad: stream history rows: %w", err)
	}
	return nil
}

// Snapshot returns a reader for the entire ledger state.
// This enables rapid bootstrap of new nodes from existing ledger data.
func (l *LocalDBLedger) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	query := `
		SELECT topic, seq, partitionId, nodeId, body, signature, timestamp
		FROM ladRecords
		ORDER BY timestamp ASC, topic ASC, seq ASC
	`

	rows, err := l.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("lad: snapshot query: %w", err)
	}

	pr, pw := io.Pipe()

	go func() {
		defer rows.Close()
		defer pw.Close()

		encoder := json.NewEncoder(pw)

		for rows.Next() {
			var rec lad.Record
			var topicStr, bodyStr, timestampStr string

			if err := rows.Scan(&topicStr, &rec.Seq, &rec.Partition, &rec.NodeID, &bodyStr, &rec.Signature, &timestampStr); err != nil {
				pw.CloseWithError(err)
				return
			}

			rec.Topic = lad.Topic(topicStr)
			rec.Body = json.RawMessage(bodyStr)
			rec.Timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)

			if err := encoder.Encode(rec); err != nil {
				pw.CloseWithError(err)
				return
			}
		}

		if err := rows.Err(); err != nil {
			pw.CloseWithError(err)
		}
	}()

	return pr, nil
}

// Close stops all background tasks and closes the ledger.
func (l *LocalDBLedger) Close() error {
	l.stopOnce.Do(func() {
		close(l.stopChan)

		// End every subscription before the Bloom filters are saved, which
		// can take up to 10 s: each Stream goroutine then hands its consumer
		// what its queue holds and closes its channel, and no Append from here
		// on queues anything for it. The map is dropped, and subscribe
		// registers nothing once stopChan is closed.
		l.mu.Lock()
		for _, sub := range l.subscribers {
			sub.endFor(lad.ErrLedgerClosed)
		}
		l.subscribers = nil
		l.mu.Unlock()

		// Stop compaction ticker if running
		if l.compactTicker != nil {
			l.compactTicker.Stop()
		}

		// Stop Bloom rebuild ticker if running
		if l.bloomTicker != nil {
			l.bloomTicker.Stop()

			// Save Bloom filters one last time
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			l.saveBloomFilters(ctx)
			cancel()
		}
	})

	return nil
}

// subscribe registers a new subscriber for live updates and returns its id,
// or -1 once Close has closed stopChan. Close ends only the subscribers it
// finds and drops the map, so a Stream goroutine that starts after Close
// registers nothing (registering into the dropped map would write a nil map)
// and StreamWithEnd ends its stream with lad.ErrLedgerClosed.
func (l *LocalDBLedger) subscribe(topics map[lad.Topic]struct{}) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	select {
	case <-l.stopChan:
		return -1
	default:
	}

	id := l.nextSubID
	l.nextSubID++

	l.subscribers[id] = newSubscriber(id, topics)

	return id
}

// unsubscribe ends a subscriber and removes it; the Stream goroutine that
// registered it defers this. The subscriber's channel stays open: broadcast
// may hold the subscriber from a copy taken before the removal, and end makes
// that send a no-op.
func (l *LocalDBLedger) unsubscribe(id int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if sub, ok := l.subscribers[id]; ok {
		sub.end()
		delete(l.subscribers, id)
	}
}

// getSubscriber retrieves a subscriber by ID.
func (l *LocalDBLedger) getSubscriber(id int) *subscriber {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.subscribers[id]
}

// broadcast offers a record to every matching subscriber, over a copy of the
// subscriber set taken under l.mu so that no send holds the lock.
func (l *LocalDBLedger) broadcast(rec lad.Record) {
	l.mu.RLock()
	subs := make([]*subscriber, 0, len(l.subscribers))
	for _, sub := range l.subscribers {
		subs = append(subs, sub)
	}
	l.mu.RUnlock()

	// Use common broadcast function
	broadcast(subs, rec)
}

// runCompaction runs the compaction task in the background
func (l *LocalDBLedger) runCompaction() {
	for {
		select {
		case <-l.stopChan:
			return
		case <-l.compactTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := l.Compact(ctx); err != nil {
				// Log error but don't crash - compaction failures are non-fatal
				log.Printf("[LAD] Compaction error: %v", err)
			}
			cancel()
		}
	}
}

// Compact removes records older than the configured retention period.
// This method is safe to call concurrently and will not affect active records.
func (l *LocalDBLedger) Compact(ctx context.Context) error {
	if l.retentionDays <= 0 {
		return nil // Compaction disabled
	}

	cutoffTime := time.Now().UTC().Add(-time.Duration(l.retentionDays) * 24 * time.Hour)
	cutoffStr := cutoffTime.Format(time.RFC3339)

	// Delete old records in batches to avoid long-running transactions
	const batchSize = 10000
	deletedTotal := 0

	for {
		result, err := l.db.ExecContext(ctx,
			`DELETE FROM ladRecords 
			 WHERE rowid IN (
				SELECT rowid FROM ladRecords 
				WHERE timestamp < ? 
				LIMIT ?
			)`,
			cutoffStr, batchSize,
		)
		if err != nil {
			return fmt.Errorf("lad: compaction failed: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("lad: failed to get rows affected: %w", err)
		}

		deletedTotal += int(rowsAffected)

		// If we deleted fewer rows than batch size, we're done
		if rowsAffected < batchSize {
			break
		}

		// Check if context is cancelled between batches
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	if deletedTotal > 0 {
		dbgLocal.Printf("Compaction deleted %d records older than %s", deletedTotal, cutoffStr)
	}

	return nil
}

// CompactStats returns information about the current ledger size and retention
type CompactStats struct {
	TotalRecords   int64
	OldestRecord   time.Time
	NewestRecord   time.Time
	RetentionDays  int
	CompactEnabled bool
}

// GetCompactStats returns current compaction statistics
func (l *LocalDBLedger) GetCompactStats(ctx context.Context) (CompactStats, error) {
	stats := CompactStats{
		RetentionDays:  l.retentionDays,
		CompactEnabled: l.compactTicker != nil,
	}

	// Count total records
	err := l.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ladRecords`).Scan(&stats.TotalRecords)
	if err != nil {
		return stats, fmt.Errorf("lad: failed to count records: %w", err)
	}

	// Get oldest record timestamp
	var oldestStr sql.NullString
	err = l.db.QueryRowContext(ctx,
		`SELECT timestamp FROM ladRecords ORDER BY timestamp ASC LIMIT 1`,
	).Scan(&oldestStr)
	if err != nil && err != sql.ErrNoRows {
		return stats, fmt.Errorf("lad: failed to get oldest record: %w", err)
	}
	if oldestStr.Valid {
		stats.OldestRecord, _ = time.Parse(time.RFC3339, oldestStr.String)
	}

	// Get newest record timestamp
	var newestStr sql.NullString
	err = l.db.QueryRowContext(ctx,
		`SELECT timestamp FROM ladRecords ORDER BY timestamp DESC LIMIT 1`,
	).Scan(&newestStr)
	if err != nil && err != sql.ErrNoRows {
		return stats, fmt.Errorf("lad: failed to get newest record: %w", err)
	}
	if newestStr.Valid {
		stats.NewestRecord, _ = time.Parse(time.RFC3339, newestStr.String)
	}

	return stats, nil
}

// updateBloomFilter adds a record to the appropriate Bloom filters
func (l *LocalDBLedger) updateBloomFilter(rec lad.Record) {
	if l.bloomMgr == nil {
		return
	}

	// Scope by topic
	topicFilter := l.bloomMgr.GetOrCreate(fmt.Sprintf("topic:%s", rec.Topic))
	topicFilter.Add([]byte(fmt.Sprintf("%s:%d", rec.Topic, rec.Seq)))

	// Scope by partition+topic for efficient partition queries
	if rec.Partition != "" {
		partitionFilter := l.bloomMgr.GetOrCreate(fmt.Sprintf("partition:%s:%s", rec.Partition, rec.Topic))
		partitionFilter.Add([]byte(fmt.Sprintf("%s:%s:%d", rec.Partition, rec.Topic, rec.Seq)))
	}

	// Scope by node for node-specific queries
	if rec.NodeID != "" {
		nodeFilter := l.bloomMgr.GetOrCreate(fmt.Sprintf("node:%s", rec.NodeID))
		nodeFilter.Add([]byte(fmt.Sprintf("%s:%d", rec.NodeID, rec.Seq)))
	}
}

// loadBloomFilters loads persisted Bloom filters from the database
func (l *LocalDBLedger) loadBloomFilters() error {
	if l.bloomMgr == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := l.db.QueryContext(ctx, `SELECT scope, filterData FROM ladBloomFilters`)
	if err != nil {
		return fmt.Errorf("lad: query bloom filters: %w", err)
	}
	defer rows.Close()

	loaded := 0
	for rows.Next() {
		var scope string
		var data []byte
		if err := rows.Scan(&scope, &data); err != nil {
			continue // Skip invalid entries
		}

		filter, err := bloom.DeserializeBloomFilter(data)
		if err != nil {
			continue // Skip corrupt filters
		}

		l.bloomMgr.Set(scope, filter)
		loaded++
	}

	if loaded > 0 {
		dbgLocal.Printf("Loaded %d Bloom filters from storage", loaded)
	}

	return rows.Err()
}

// saveBloomFilters persists all Bloom filters to the database
func (l *LocalDBLedger) saveBloomFilters(ctx context.Context) error {
	if l.bloomMgr == nil {
		return nil
	}

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("lad: begin bloom save: %w", err)
	}
	defer tx.Rollback()

	// Clear existing filters
	if _, err := tx.ExecContext(ctx, `DELETE FROM ladBloomFilters`); err != nil {
		return fmt.Errorf("lad: clear bloom filters: %w", err)
	}

	// Save current filters
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ladBloomFilters (scope, filterData, updatedAt)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("lad: prepare bloom insert: %w", err)
	}
	defer stmt.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	for scope, filter := range l.bloomMgr.Filters() {
		data := filter.Serialize()
		if _, err := stmt.ExecContext(ctx, scope, data, now); err != nil {
			return fmt.Errorf("lad: insert bloom filter %s: %w", scope, err)
		}
	}

	return tx.Commit()
}

// runBloomRebuild periodically rebuilds Bloom filters from scratch
func (l *LocalDBLedger) runBloomRebuild() {
	for {
		select {
		case <-l.stopChan:
			return
		case <-l.bloomTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			if err := l.rebuildBloomFilters(ctx); err != nil {
				// Log error but don't crash - Bloom filter rebuilds are non-critical
				log.Printf("[LAD] Bloom filter rebuild error: %v", err)
			}
			cancel()
		}
	}
}

// rebuildBloomFilters clears and rebuilds all Bloom filters from current ledger data
func (l *LocalDBLedger) rebuildBloomFilters(ctx context.Context) error {
	if l.bloomMgr == nil {
		return nil
	}

	// Clear existing filters
	l.bloomMgr.Clear()

	// Query all records and rebuild filters
	rows, err := l.db.QueryContext(ctx, `
		SELECT topic, seq, partitionId, nodeId FROM ladRecords
	`)
	if err != nil {
		return fmt.Errorf("lad: query for bloom rebuild: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var topic, partitionID, nodeID string
		var seq uint64
		if err := rows.Scan(&topic, &seq, &partitionID, &nodeID); err != nil {
			continue
		}

		// Rebuild each scope's filter
		rec := lad.Record{
			Topic:     lad.Topic(topic),
			Seq:       seq,
			Partition: partitionID,
			NodeID:    nodeID,
		}
		l.updateBloomFilter(rec)
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("lad: rows error during bloom rebuild: %w", err)
	}

	// Persist rebuilt filters
	if err := l.saveBloomFilters(ctx); err != nil {
		return fmt.Errorf("lad: save bloom filters: %w", err)
	}

	dbgLocal.Printf("Rebuilt Bloom filters from %d records", count)

	return nil
}
