/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// TopicBlobHave is the Whisper BroadcastOnly topic on which nodes advertise
// the CIDs they hold. Callers MUST pre-register this topic with their engine
// using the whisper.BroadcastOnly mode (see Advertiser.RegisterTopic).
const TopicBlobHave = "plutus.blob.have"

// DefaultAdvertiseInterval is the cadence used when Config.Interval is zero.
const DefaultAdvertiseInterval = 60 * time.Second

// HaveAdvertisement is the payload emitted on TopicBlobHave. Field ordering
// drives JSON key order, which Go guarantees is stable — useful when peers
// hash the envelope for dedup.
//
// TODO(phase3-privacy): replace the plaintext CID list with a Bloom filter
// encoded as (k, m, bitmap) so holdings are obscured. See
// _OTHER/mercury/ROADMAP.md section 2 "Privacy" — the "Bloom-filter
// advertisement" row. The resolver will need to shift from set-membership
// tests to bloom-probe + false-positive tolerance.
//
// TODO(phase3-privacy/encrypted-labels): in addition to (or instead of) the
// bloom filter, advertise session-scoped HMAC(piece-cid, session-key) values
// instead of raw CIDs. See _OTHER/mercury/ROADMAP.md section 2 "Privacy" —
// the "Encrypted piece labels" row. Resolvers will need to key their
// has-checks on the per-session HMAC rather than the raw CID so observers
// cannot correlate holdings across sessions.
type HaveAdvertisement struct {
	// PeerID is the advertiser's stable mesh identifier. Whisper's transport
	// layer signs the envelope, so we don't re-sign here.
	PeerID string `json:"peerId"`
	// EmittedAtUnixMs is the wall-clock time the ad was built. Used by
	// resolvers to decide recency / freshness.
	EmittedAtUnixMs int64 `json:"emittedAtMs"`
	// CIDs lists the piece CIDs currently held locally. Empty list allowed
	// (encodes "I'm up but cache is cold").
	CIDs []string `json:"cids"`
}

// PublishTopic is the subset of *whisper.Engine that the Advertiser needs.
// The indirection exists for testability — wiring to a real engine is one
// function: NewAdvertiser(store, engine, cfg).
type PublishTopic interface {
	Publish(topic string, payload []byte) error
}

// TopicLister enumerates the CIDs this node currently holds. The Store
// satisfies this interface via List.
type TopicLister interface {
	List(ctx context.Context) ([]string, error)
}

// AdvertiserConfig tunes the advertiser cadence + identity.
type AdvertiserConfig struct {
	// PeerID identifies this node. Written into every advertisement so
	// resolvers know who to fetch from. Required.
	PeerID string
	// Interval is the cadence between emissions. Defaults to
	// DefaultAdvertiseInterval when zero.
	Interval time.Duration
	// Topic overrides TopicBlobHave (tests).
	Topic string
	// Now, if set, replaces time.Now() for deterministic tests.
	Now func() time.Time
}

// Advertiser periodically gossips the CIDs held by a local Store over a
// Whisper BroadcastOnly topic. Designed to be wired once at boot; lifecycle
// is controlled via Start/Stop.
type Advertiser struct {
	store    TopicLister
	engine   PublishTopic
	cfg      AdvertiserConfig
	topic    string
	interval time.Duration
	now      func() time.Time

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewAdvertiser wires a Store-like topic lister + publisher into a recurring
// advertisement loop. It does NOT start the loop — call Start.
func NewAdvertiser(store TopicLister, engine PublishTopic, cfg AdvertiserConfig) (*Advertiser, error) {
	if store == nil {
		return nil, errors.New("distributed: advertiser needs a non-nil store")
	}
	if engine == nil {
		return nil, errors.New("distributed: advertiser needs a non-nil publisher")
	}
	if cfg.PeerID == "" {
		return nil, errors.New("distributed: advertiser needs a non-empty PeerID")
	}
	topic := cfg.Topic
	if topic == "" {
		topic = TopicBlobHave
	}
	iv := cfg.Interval
	if iv <= 0 {
		iv = DefaultAdvertiseInterval
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Advertiser{
		store:    store,
		engine:   engine,
		cfg:      cfg,
		topic:    topic,
		interval: iv,
		now:      now,
	}, nil
}

// Start begins the advertisement loop. Emits one advertisement immediately,
// then again every Interval until ctx is cancelled or Stop is invoked.
// Returns an error if already running.
func (a *Advertiser) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return errors.New("distributed: advertiser already running")
	}
	a.running = true
	a.done = make(chan struct{})
	loopCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.mu.Unlock()

	go a.loop(loopCtx)
	return nil
}

// Stop halts the advertisement loop and waits for the background goroutine
// to exit. Safe to call multiple times.
func (a *Advertiser) Stop() {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return
	}
	a.running = false
	cancel := a.cancel
	done := a.done
	a.cancel = nil
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (a *Advertiser) loop(ctx context.Context) {
	defer func() {
		a.mu.Lock()
		done := a.done
		a.done = nil
		a.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()

	// Emit immediately so peers learn our cache on boot.
	_ = a.EmitOnce(ctx)

	t := time.NewTicker(a.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = a.EmitOnce(ctx)
		}
	}
}

// EmitOnce builds and publishes a single HaveAdvertisement synchronously.
// Exposed for tests and manual ticks.
func (a *Advertiser) EmitOnce(ctx context.Context) error {
	cids, err := a.store.List(ctx)
	if err != nil {
		return fmt.Errorf("distributed: advertiser list: %w", err)
	}
	ad := HaveAdvertisement{
		PeerID:          a.cfg.PeerID,
		EmittedAtUnixMs: a.now().UnixMilli(),
		CIDs:            cids,
	}
	payload, err := json.Marshal(&ad)
	if err != nil {
		return fmt.Errorf("distributed: advertiser marshal: %w", err)
	}
	if err := a.engine.Publish(a.topic, payload); err != nil {
		return fmt.Errorf("distributed: advertiser publish: %w", err)
	}
	return nil
}

// Topic returns the effective topic name (respecting AdvertiserConfig.Topic
// override). Useful for callers that need to register the topic with their
// Whisper engine.
func (a *Advertiser) Topic() string { return a.topic }

// DecodeHaveAdvertisement parses bytes emitted by EmitOnce. Resolvers and
// discovery layers use this to consume broadcast ads.
func DecodeHaveAdvertisement(b []byte) (*HaveAdvertisement, error) {
	if len(b) == 0 {
		return nil, errors.New("distributed: empty have-advertisement")
	}
	var ad HaveAdvertisement
	if err := json.Unmarshal(b, &ad); err != nil {
		return nil, fmt.Errorf("distributed: have-advertisement decode: %w", err)
	}
	return &ad, nil
}
