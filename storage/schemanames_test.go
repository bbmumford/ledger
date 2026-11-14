/*
 * Copyright (c) 2026 ORBTR Pty Ltd. All Rights Reserved.
 */

package storage

import (
	"database/sql"
	"database/sql/driver"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// ruleName is the one spelling every identifier this ledger declares takes: lowerCamelCase
// with an acronym written as a word, so a record row reads the same names as the record
// it scans into. Two adjacent capitals are an acronym kept in capitals; one trailing capital
// is a one-letter final word.
var ruleName = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[A-Z][a-z0-9]+)*[A-Z]?$`)

// schemaRecorder is a database/sql driver that answers every statement with nothing and
// keeps the text of each one prepared, so the test reads the schema NewLocalDBLedger sends
// without a database engine (this module carries none; its caller supplies the driver).
type schemaRecorder struct {
	mu    sync.Mutex
	stmts []string
}

func (r *schemaRecorder) Open(string) (driver.Conn, error) { return recorderConn{r}, nil }

type recorderConn struct{ r *schemaRecorder }

func (c recorderConn) Prepare(q string) (driver.Stmt, error) {
	c.r.mu.Lock()
	c.r.stmts = append(c.r.stmts, q)
	c.r.mu.Unlock()
	return nullStmt{}, nil
}
func (recorderConn) Close() error              { return nil }
func (recorderConn) Begin() (driver.Tx, error) { return nullTx{}, nil }

var (
	registerRecorder sync.Once
	recorder         = &schemaRecorder{}
	createdObject    = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX)\s+IF\s+NOT\s+EXISTS\s+(\w+)`)
	tableBody        = regexp.MustCompile(`(?is)CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+\w+\s*\((.*?)\n\s*\);`)
	columnLine       = regexp.MustCompile(`(?m)^\s*(\w+)\s+(?:TEXT|INTEGER|BLOB|REAL)\b`)
)

// The record and bloom-filter tables, their columns and the record indexes are declared in
// that one spelling, read from the statements the ledger sends when it is opened.
func TestTheSchemaDeclaresOnlyRuleNames(t *testing.T) {
	registerRecorder.Do(func() { sql.Register("ledger-storage-schema-recorder", recorder) })
	db, err := sql.Open("ledger-storage-schema-recorder", "")
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewLocalDBLedger(db, LedgerConfig{})
	if err != nil {
		t.Fatalf("NewLocalDBLedger: %v", err)
	}
	t.Cleanup(func() {
		_ = l.Close()
		_ = db.Close()
	})
	recorder.mu.Lock()
	schema := strings.Join(recorder.stmts, "\n")
	recorder.mu.Unlock()
	var declared []string
	for _, m := range createdObject.FindAllStringSubmatch(schema, -1) {
		declared = append(declared, m[1])
	}
	for _, body := range tableBody.FindAllStringSubmatch(schema, -1) {
		for _, m := range columnLine.FindAllStringSubmatch(body[1], -1) {
			declared = append(declared, m[1])
		}
	}
	if len(declared) == 0 {
		t.Fatal("NewLocalDBLedger sent no CREATE statement the recorder could read")
	}
	var bad []string
	for _, n := range declared {
		if !ruleName.MatchString(n) {
			bad = append(bad, n)
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Fatalf("the schema declares %d names outside the rule: %q", len(bad), bad)
	}
}
