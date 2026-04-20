/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"log"
	"os"
	"strings"
)

// DebugLogger provides conditional debug logging for Ledger subsystems.
type DebugLogger struct {
	name    string
	enabled bool
	logger  *log.Logger
}

var debugEnvParsed = parseDebugEnv()

func parseDebugEnv() map[string]bool {
	env := os.Getenv("DEBUG")
	if env == "" {
		return nil
	}
	m := make(map[string]bool)
	for _, s := range strings.Split(env, ",") {
		m[strings.TrimSpace(s)] = true
	}
	return m
}

func isDebugEnabled(name string) bool {
	if debugEnvParsed == nil {
		return false
	}
	if debugEnvParsed["ledger.*"] || debugEnvParsed["*"] {
		return true
	}
	if debugEnvParsed[name] {
		return true
	}
	return false
}

// NewDebug creates a debug logger for the named subsystem.
func NewDebug(name string) *DebugLogger {
	return &DebugLogger{
		name:    name,
		enabled: isDebugEnabled(name),
		logger:  log.New(os.Stderr, "["+name+"] ", log.LstdFlags),
	}
}

// Printf logs a formatted message if this subsystem is enabled.
func (d *DebugLogger) Printf(format string, args ...interface{}) {
	if d.enabled {
		d.logger.Printf(format, args...)
	}
}

var dbgLADTypes = NewDebug("ledger.types")
