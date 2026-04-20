/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

// NoopMetricsRecorder is a no-op implementation of MetricsRecorder.
// Use it as a default when no real metrics backend is configured.
type NoopMetricsRecorder struct{}

func (NoopMetricsRecorder) RecordApplied(topic string, merged bool)        {}
func (NoopMetricsRecorder) RecordExpired(topic string)                     {}
func (NoopMetricsRecorder) MergeConflict(topic string, winner string)      {}
func (NoopMetricsRecorder) TopicSize(topic string, count int, bytes int64) {}
func (NoopMetricsRecorder) GossipExchange(topic string, sent, received int) {}

// Verify interface compliance at compile time.
var _ MetricsRecorder = NoopMetricsRecorder{}
