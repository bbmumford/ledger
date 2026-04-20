/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// Topic identifies a logical log shard within the ledger.
type Topic string

const (
	TopicMember  Topic = "member"
	TopicRole    Topic = "role"
	TopicReach   Topic = "reach"
	TopicKeyOps  Topic = "keyops"
	TopicQuorum  Topic = "quorum"
	TopicLatency Topic = "latency"
)

// CausalWatermark tracks the last observed sequence per topic.
type CausalWatermark map[Topic]uint64

// Copy returns a deep copy of the watermark map.
func (cw CausalWatermark) Copy() CausalWatermark {
	dup := make(CausalWatermark, len(cw))
	for topic, seq := range cw {
		dup[topic] = seq
	}
	return dup
}

// Record is the canonical ledger entry.
type Record struct {
	Topic           Topic           `json:"topic"`
	Seq             uint64          `json:"seq"`
	TenantID        string          `json:"tenant_id"`
	NodeID          string          `json:"node_id"`
	Body            json.RawMessage `json:"body"`
	Signature       []byte          `json:"sig"`
	Timestamp       time.Time       `json:"ts"`
	LamportClock    uint64          `json:"lc,omitempty"`               // causal ordering
	HLCTimestamp    uint64          `json:"hlc,omitempty"`              // hybrid logical clock (0 = unused)
	Tombstone       bool            `json:"tombstone,omitempty"`        // true = deletion marker
	DeletedAt       time.Time       `json:"deleted_at,omitempty"`
	TombstoneReason string          `json:"tombstone_reason,omitempty"` // "explicit", "cap"
	ExpiresAt       time.Time       `json:"expires_at,omitempty"`       // TTL expiration (zero = never)
	AuthorPubKey    []byte          `json:"author_pubkey,omitempty"`    // Ed25519 public key of record author
	BlobCID         string          `json:"blob_cid,omitempty"`         // content-addressed blob reference
}

// MergeFunc resolves conflicts between an existing record and an incoming record.
// Returns the winning record. Called by DirectoryCache.Apply().
type MergeFunc func(existing, incoming Record) (Record, error)

// KeyFunc derives the storage key from a record.
// Default is NodeIDKey (keyed by NodeID). CRDTs use BodyKey.
type KeyFunc func(Record) string

// OverwriteMerge is the default merge function.
// Latest timestamp or highest LamportClock wins.
func OverwriteMerge(existing, incoming Record) (Record, error) {
	if incoming.LamportClock > existing.LamportClock {
		return incoming, nil
	}
	if incoming.LamportClock == existing.LamportClock && incoming.Timestamp.After(existing.Timestamp) {
		return incoming, nil
	}
	return existing, nil
}

// NodeIDKey derives the storage key from the record's NodeID.
func NodeIDKey(r Record) string { return r.NodeID }

// BodyKey derives the storage key from the record's Body (collection:key pattern).
func BodyKey(r Record) string {
	var meta struct {
		Collection string `json:"collection"`
		Key        string `json:"key"`
	}
	if err := json.Unmarshal(r.Body, &meta); err != nil {
		return r.NodeID // fallback
	}
	return meta.Collection + ":" + meta.Key
}

// QueryFilter provides generic query parameters for any topic.
type QueryFilter struct {
	NodeID    string    // filter by node (empty = all)
	KeyPrefix string    // filter by key prefix (empty = all)
	Since     time.Time // records after this time (zero = all)
	Limit     int       // max results (0 = unlimited)
}

// TopicConfig configures per-topic behavior.
type TopicConfig struct {
	Merge            MergeFunc                                        // conflict resolution (nil = OverwriteMerge)
	Key              KeyFunc                                          // key derivation (nil = NodeIDKey)
	ExpiryEnabled    bool                                             // enable TTL expiration
	RequireSignature bool                                             // reject unsigned records
	VerifyFunc       func(pubkey, signature, content []byte) bool     // custom signature verifier (nil = Ed25519)
	SignFunc         func(content []byte) (pubkey, sig []byte)        // for local writes
}

// MetricsRecorder receives operational metrics from the cache.
type MetricsRecorder interface {
	RecordApplied(topic string, merged bool)
	RecordExpired(topic string)
	MergeConflict(topic string, winner string)
	TopicSize(topic string, count int, bytes int64)
	GossipExchange(topic string, sent, received int)
}

// HandlerMetadata describes security requirements for a handler
type HandlerMetadata struct {
	Name           string   `json:"name"`
	RequiresAuth   bool     `json:"requires_auth"`
	RequiredScopes []string `json:"required_scopes,omitempty"`
	TimeoutMs      int      `json:"timeout_ms"`
}

// MemberRecord mirrors the MemberAdd log schema once decoded.
type MemberRecord struct {
	TenantID  string            `json:"tenant"`
	NodeID    string            `json:"node_id"`
	PubKey    []byte            `json:"pubkey"`
	CreatedAt time.Time         `json:"created_at"`
	ExpiresAt time.Time         `json:"expires_at,omitempty"`
	Attrs     map[string]string `json:"attrs,omitempty"`
	Handlers  []HandlerMetadata `json:"handlers,omitempty"` // Handler security metadata
}

// RoleRecord mirrors the RoleSet schema.
type RoleRecord struct {
	TenantID string            `json:"tenant"`
	NodeID   string            `json:"node_id"`
	Roles    []string          `json:"roles"`
	Handlers []HandlerMetadata `json:"handlers,omitempty"` // Handler security metadata
	MaxGrade int               `json:"max_grade,omitempty"` // Highest grade this node can support (0-4)
	Updated  time.Time         `json:"ts"`
}

// ReachAddress represents a single advertised endpoint.
type ReachAddress struct {
	Host  string `json:"ip"`
	Port  int    `json:"port"`
	Proto string `json:"proto"`
	Scope string `json:"scope"` // public|lan|rfc1918
}

// String returns host:port for dialing.
func (a ReachAddress) String() string {
	return net.JoinHostPort(a.Host, strconv.Itoa(a.Port))
}

// DialAddress returns the protocol-appropriate address for dialing.
// For WSS, this builds the full WebSocket URL. For other protocols,
// it returns host:port (same as String()).
func (a ReachAddress) DialAddress() string {
	if a.Proto == "wss" {
		return fmt.Sprintf("wss://%s/mesh/ws", a.Host)
	}
	return a.String()
}

// ReachRecord mirrors the ReachSet schema plus derived metrics.
type ReachRecord struct {
	TenantID      string         `json:"tenant"`
	NodeID        string         `json:"node_id"`
	Seq           uint64         `json:"seq"`
	Addresses     []ReachAddress `json:"addrs"`
	Region        string         `json:"region"` // Hosting region (e.g., "iad", "us-east-1")
	NATType       string         `json:"nat_type"`
	NATObserved   string         `json:"nat_observed"`
	LatencyMillis int64          `json:"latency_ms"`
	Availability  float64        `json:"availability"`
	LoadFactor    float64        `json:"load_factor"` // 0.0 (idle) to 1.0 (overloaded)
	ExpiresAt     time.Time      `json:"expires_at"`
	UpdatedAt     time.Time      `json:"ts"`
}

// LatencyDuration exposes the latency as a time.Duration for callers.
func (r ReachRecord) LatencyDuration() time.Duration {
	if r.LatencyMillis <= 0 {
		return 0
	}
	return time.Duration(r.LatencyMillis) * time.Millisecond
}

// LatencyRecord stores a directional RTT measurement between two nodes.
// Published by the measuring node: "I measured Xms RTT to peer Y."
type LatencyRecord struct {
	FromNode     string    `json:"from_node"`
	ToNode       string    `json:"to_node"`
	RTTMs        int64     `json:"rtt_ms"`                        // Application RTT (gossip/data exchange latency)
	InitialRTTMs int64     `json:"initial_rtt_ms,omitempty"`      // Network RTT (connection setup latency)
	Transport    string    `json:"transport,omitempty"`            // "noise-udp" or "gossip-tls"
	MeasuredAt   time.Time `json:"measured_at"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// ReachQuery provides filters for querying reachability records
type ReachQuery struct {
	NodeID   string  // Filter by specific node ID (optional)
	Role     string  // Filter by role (optional)
	MinScore float64 // Minimum availability score (optional, 0.0-1.0)
	Limit    int     // Maximum number of results (optional)
}

// UnmarshalReach decodes a record body into a ReachRecord.
func UnmarshalReach(body []byte) (ReachRecord, error) {
	var tmp ReachRecord
	if err := json.Unmarshal(body, &tmp); err != nil {
		return ReachRecord{}, err
	}
	return tmp, nil
}

// UnmarshalMember decodes a member record body.
func UnmarshalMember(body []byte) (MemberRecord, error) {
	var tmp MemberRecord
	if err := json.Unmarshal(body, &tmp); err != nil {
		return MemberRecord{}, err
	}
	return tmp, nil
}

// UnmarshalRole decodes a role record body.
func UnmarshalRole(body []byte) (RoleRecord, error) {
	var tmp RoleRecord
	if err := json.Unmarshal(body, &tmp); err != nil {
		return RoleRecord{}, err
	}
	return tmp, nil
}

// UnmarshalLatency decodes a LatencyRecord from JSON.
func UnmarshalLatency(body []byte) (LatencyRecord, error) {
	var tmp LatencyRecord
	if err := json.Unmarshal(body, &tmp); err != nil {
		return LatencyRecord{}, err
	}
	return tmp, nil
}

// Ledger exposes append and replication operations for the directory log.
type Ledger interface {
	Head(ctx context.Context) (CausalWatermark, error)
	Append(ctx context.Context, rec Record) error
	BatchAppend(ctx context.Context, records []Record) error
	Stream(ctx context.Context, from CausalWatermark, topics []Topic) (<-chan Record, error)
	Snapshot(ctx context.Context) (io.ReadCloser, error)
}
