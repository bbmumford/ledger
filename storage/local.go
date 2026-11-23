/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
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
	db *sql.DB
	// appendMu makes one append's head read, signature and insert finish
	// before the next append reads the head. SQLite takes a transaction's
	// write lock only at its first write, so two appends that each read a
	// topic's head inside their own transaction could both read it before
	// either inserts, and the second would be refused by the primary key.
	appendMu    sync.Mutex
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
	defer func() { _ = rows.Close() }()

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

// addedRecordColumns are the ladRecords columns a table holding only topic,
// seq, partitionId, nodeId, body, signature and timestamp lacks, each with the
// definition ALTER TABLE adds it by. Each default is the zero value of the field
// the column holds, so a row stored without the column reads back with that
// field at its zero value.
var addedRecordColumns = []struct{ name, definition string }{
	{"lamportClock", "lamportClock INTEGER NOT NULL DEFAULT 0"},
	{"hlcTimestamp", "hlcTimestamp INTEGER NOT NULL DEFAULT 0"},
	{"tombstone", "tombstone INTEGER NOT NULL DEFAULT 0"},
	{"deletedAtUnix", "deletedAtUnix INTEGER NOT NULL DEFAULT 0"},
	{"tombstoneReason", "tombstoneReason TEXT NOT NULL DEFAULT ''"},
	{"expiresAtUnix", "expiresAtUnix INTEGER NOT NULL DEFAULT 0"},
	{"authorPubkey", "authorPubkey BLOB"},
	{"blobCid", "blobCid TEXT NOT NULL DEFAULT ''"},
}

// addRecordColumns adds to ladRecords every column of addedRecordColumns it
// lacks. NewLocalDBLedger runs it after CREATE TABLE IF NOT EXISTS, which leaves
// an existing table as it is, so a LAD file whose ladRecords lacks them opens
// with every column Append, BatchAppend, Stream and Snapshot name. A row stored
// without them reads back with no author key, no tombstone and zero clocks.
func addRecordColumns(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(ladRecords)`)
	if err != nil {
		return fmt.Errorf("lad: read ladRecords columns: %w", err)
	}
	have := map[string]bool{}
	for rows.Next() {
		var (
			cid        int
			name, kind string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultVal, &pk); err != nil {
			_ = rows.Close()
			return fmt.Errorf("lad: read ladRecords columns: %w", err)
		}
		have[name] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("lad: read ladRecords columns: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("lad: read ladRecords columns: %w", err)
	}
	for _, c := range addedRecordColumns {
		if have[c.name] {
			continue
		}
		if _, err := db.ExecContext(ctx, `ALTER TABLE ladRecords ADD COLUMN `+c.definition); err != nil {
			return fmt.Errorf("lad: add ladRecords column %s: %w", c.name, err)
		}
	}
	return nil
}

// recordColumns names every ladRecords column in the order recordArgs binds
// them and scanRecord reads them.
const recordColumns = `topic, seq, partitionId, nodeId, body, signature, timestamp,
	lamportClock, hlcTimestamp, tombstone, deletedAtUnix, tombstoneReason, expiresAtUnix,
	authorPubkey, blobCid`

// insertRecord is the INSERT Append and BatchAppend store a record with.
const insertRecord = `INSERT INTO ladRecords (` + recordColumns + `)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// recordArgs binds rec's fields to insertRecord's placeholders. The body is
// stored as the bytes given, never re-encoded, because the signature covers
// those bytes; a body that is not JSON is refused. A zero DeletedAt or
// ExpiresAt is stored as 0, as the gossip codec sends it.
func recordArgs(rec lad.Record) ([]any, error) {
	if len(rec.Body) > 0 && !json.Valid(rec.Body) {
		return nil, fmt.Errorf("lad: record body is not JSON")
	}
	var tombstone int64
	if rec.Tombstone {
		tombstone = 1
	}
	return []any{
		string(rec.Topic),
		rec.Seq,
		rec.Partition,
		rec.NodeID,
		string(rec.Body),
		rec.Signature,
		rec.Timestamp.Format(time.RFC3339Nano),
		rec.LamportClock,
		rec.HLCTimestamp,
		tombstone,
		unixNanos(rec.DeletedAt),
		rec.TombstoneReason,
		unixNanos(rec.ExpiresAt),
		rec.AuthorPubKey,
		rec.BlobCID,
	}, nil
}

// scanRecord reads one row selected by recordColumns.
func scanRecord(rows *sql.Rows) (lad.Record, error) {
	var (
		rec                    lad.Record
		topic, body, timestamp string
		tombstone              int64
		deletedAt, expiresAt   int64
	)
	if err := rows.Scan(&topic, &rec.Seq, &rec.Partition, &rec.NodeID, &body, &rec.Signature, &timestamp,
		&rec.LamportClock, &rec.HLCTimestamp, &tombstone, &deletedAt, &rec.TombstoneReason, &expiresAt,
		&rec.AuthorPubKey, &rec.BlobCID); err != nil {
		return lad.Record{}, err
	}
	rec.Topic = lad.Topic(topic)
	if body != "" {
		rec.Body = json.RawMessage(body)
	}
	rec.Timestamp, _ = time.Parse(time.RFC3339Nano, timestamp)
	rec.Tombstone = tombstone != 0
	rec.DeletedAt = fromUnixNanos(deletedAt)
	rec.ExpiresAt = fromUnixNanos(expiresAt)
	return rec, nil
}

// unixNanos is t in Unix nanoseconds, 0 for the zero time.
func unixNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// fromUnixNanos is unixNanos's inverse: 0 is the zero time.
func fromUnixNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// NewLocalDBLedger creates a persistent ledger backed by a local embedded libSQL database.
// Pass DefaultLedgerConfig() for sensible defaults, or customize retention and compaction.
//
// The schema is automatically initialized: ladRecords holds one column for every
// field of a lad.Record (recordColumns), so a record read back through Stream or
// Snapshot carries the bytes it was signed over. A ladRecords table that lacks
// any of those columns gets them added (addRecordColumns), each at its zero value
// for the rows already stored.
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
			lamportClock INTEGER NOT NULL DEFAULT 0,
			hlcTimestamp INTEGER NOT NULL DEFAULT 0,
			tombstone INTEGER NOT NULL DEFAULT 0,
			deletedAtUnix INTEGER NOT NULL DEFAULT 0,
			tombstoneReason TEXT NOT NULL DEFAULT '',
			expiresAtUnix INTEGER NOT NULL DEFAULT 0,
			authorPubkey BLOB,
			blobCid TEXT NOT NULL DEFAULT '',
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
	if err := addRecordColumns(context.Background(), db); err != nil {
		return nil, err
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

		// Load existing Bloom filters from database. A failed load does not
		// fail initialization: the rebuild cycle, when configured, rebuilds
		// the filters from the stored records.
		_ = ledger.loadBloomFilters()

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
	defer func() { _ = rows.Close() }()

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

// Append validates and stores a record in the local database. A record with no
// Seq is given the topic's next one and a zero Timestamp is set to now, inside
// the transaction that stores it. A signed record with no Seq is refused with
// lad.ErrSignedWithoutSeq: AppendSigned numbers before it signs.
func (l *LocalDBLedger) Append(ctx context.Context, rec lad.Record) error {
	return l.append(ctx, rec, nil)
}

// AppendSigned is Append with sign called on the record after it is numbered
// and before it is stored, in the same transaction (lad.SigningLedger).
func (l *LocalDBLedger) AppendSigned(ctx context.Context, rec lad.Record, sign lad.SignFunc) error {
	if sign == nil {
		return fmt.Errorf("lad: AppendSigned needs a sign function")
	}
	return l.append(ctx, rec, sign)
}

func (l *LocalDBLedger) append(ctx context.Context, rec lad.Record, sign lad.SignFunc) error {
	if rec.Topic == "" {
		return fmt.Errorf("lad: missing topic")
	}
	if sign == nil && rec.Seq == 0 && len(rec.Signature) > 0 {
		return lad.ErrSignedWithoutSeq
	}

	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("lad: begin append: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if rec.Seq == 0 {
		head, err := topicHead(ctx, tx, rec.Topic)
		if err != nil {
			return fmt.Errorf("lad: get head for seq: %w", err)
		}
		rec.Seq = head + 1
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}
	if sign != nil {
		if err := sign(&rec); err != nil {
			return fmt.Errorf("lad: sign record: %w", err)
		}
	}

	args, err := recordArgs(rec)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, insertRecord, args...); err != nil {
		return fmt.Errorf("lad: insert record: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("lad: commit record: %w", err)
	}

	// Update Bloom filter if enabled
	if l.bloomMgr != nil {
		l.updateBloomFilter(rec)
	}

	// Notify subscribers
	l.broadcast(rec)

	return nil
}

// topicHead reads topic's highest stored Seq through q, 0 for an empty topic.
func topicHead(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}, topic lad.Topic) (uint64, error) {
	var head sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT MAX(seq) FROM ladRecords WHERE topic = ?`, string(topic)).Scan(&head); err != nil {
		return 0, err
	}
	if !head.Valid {
		return 0, nil
	}
	return uint64(head.Int64), nil
}

// BatchAppend appends multiple records in a single transaction for high-throughput scenarios.
// Records are assigned sequences inside that transaction and subscribers are notified after commit.
// Maximum batch size is 1000 records to avoid large transactions. A signed record with no Seq
// refuses the whole batch with lad.ErrSignedWithoutSeq.
func (l *LocalDBLedger) BatchAppend(ctx context.Context, records []lad.Record) error {
	return l.batchAppend(ctx, records, nil)
}

// BatchAppendSigned is BatchAppend with sign called on each record after it is
// numbered and before it is stored, in the batch's transaction
// (lad.SigningLedger).
func (l *LocalDBLedger) BatchAppendSigned(ctx context.Context, records []lad.Record, sign lad.SignFunc) error {
	if sign == nil {
		return fmt.Errorf("lad: BatchAppendSigned needs a sign function")
	}
	return l.batchAppend(ctx, records, sign)
}

func (l *LocalDBLedger) batchAppend(ctx context.Context, records []lad.Record, sign lad.SignFunc) error {
	if len(records) == 0 {
		return nil
	}

	const maxBatchSize = 1000
	if len(records) > maxBatchSize {
		return fmt.Errorf("lad: batch size %d exceeds maximum %d", len(records), maxBatchSize)
	}
	for i := range records {
		if records[i].Topic == "" {
			return fmt.Errorf("lad: missing topic in record %d", i)
		}
		if sign == nil && records[i].Seq == 0 && len(records[i].Signature) > 0 {
			return fmt.Errorf("lad: record %d: %w", i, lad.ErrSignedWithoutSeq)
		}
	}

	// Create a transaction with 5-second timeout
	txCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	tx, err := l.db.BeginTx(txCtx, nil)
	if err != nil {
		return fmt.Errorf("lad: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // Rollback on error

	// Next sequence per topic within this batch, each topic's head read
	// through the batch's own transaction when the batch first meets it.
	nextSeq := make(map[lad.Topic]uint64)
	next := func(topic lad.Topic) (uint64, error) {
		if seq, ok := nextSeq[topic]; ok {
			return seq, nil
		}
		head, err := topicHead(txCtx, tx, topic)
		if err != nil {
			return 0, fmt.Errorf("lad: get head for batch: %w", err)
		}
		nextSeq[topic] = head + 1
		return head + 1, nil
	}

	// Prepare insert statement
	stmt, err := tx.PrepareContext(txCtx, insertRecord)
	if err != nil {
		return fmt.Errorf("lad: prepare batch insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	// Process each record
	processedRecords := make([]lad.Record, 0, len(records))
	for i := range records {
		rec := &records[i]

		seq, err := next(rec.Topic)
		if err != nil {
			return err
		}
		if rec.Seq == 0 {
			rec.Seq = seq
			nextSeq[rec.Topic] = seq + 1
		} else if rec.Seq >= seq {
			// A Seq the caller set moves the batch's numbering past it.
			nextSeq[rec.Topic] = rec.Seq + 1
		}

		// Auto-set timestamp if not provided
		if rec.Timestamp.IsZero() {
			rec.Timestamp = time.Now().UTC()
		}
		if sign != nil {
			if err := sign(rec); err != nil {
				return fmt.Errorf("lad: sign record %d: %w", i, err)
			}
		}

		args, err := recordArgs(*rec)
		if err != nil {
			return fmt.Errorf("lad: record %d: %w", i, err)
		}
		if _, err := stmt.ExecContext(txCtx, args...); err != nil {
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
		SELECT %s
		FROM ladRecords
		WHERE 1=1 %s
		ORDER BY timestamp ASC, topic ASC, seq ASC
	`, recordColumns, topicFilter)

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("lad: stream history query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return fmt.Errorf("lad: stream history scan: %w", err)
		}

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
	query := `SELECT ` + recordColumns + `
		FROM ladRecords
		ORDER BY timestamp ASC, topic ASC, seq ASC
	`

	rows, err := l.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("lad: snapshot query: %w", err)
	}

	pr, pw := io.Pipe()

	go func() {
		defer func() { _ = rows.Close() }()
		defer func() { _ = pw.Close() }()

		encoder := json.NewEncoder(pw)

		for rows.Next() {
			rec, err := scanRecord(rows)
			if err != nil {
				pw.CloseWithError(err)
				return
			}

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

			// Save Bloom filters one last time. Close does not fail on it: a
			// failed save rolls back and leaves the filters stored before.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = l.saveBloomFilters(ctx)
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
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = tx.Rollback() }()

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
	defer func() { _ = stmt.Close() }()

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
	defer func() { _ = rows.Close() }()

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
