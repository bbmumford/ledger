/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakePublisher captures every Publish call for later inspection.
type fakePublisher struct {
	mu     sync.Mutex
	topic  string
	calls  [][]byte
	fail   error
	notify chan struct{}
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{notify: make(chan struct{}, 16)}
}

func (f *fakePublisher) Publish(topic string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topic = topic
	cp := make([]byte, len(payload))
	copy(cp, payload)
	f.calls = append(f.calls, cp)
	err := f.fail
	select {
	case f.notify <- struct{}{}:
	default:
	}
	return err
}

func (f *fakePublisher) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakePublisher) Calls() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.calls))
	copy(out, f.calls)
	return out
}

// staticLister is a TopicLister backed by a fixed slice.
type staticLister struct{ cids []string }

func (s staticLister) List(ctx context.Context) ([]string, error) { return s.cids, nil }

func TestAdvertiserEmitOncePublishes(t *testing.T) {
	pub := newFakePublisher()
	lister := staticLister{cids: []string{"aaa", "bbb"}}
	ad, err := NewAdvertiser(lister, pub, AdvertiserConfig{PeerID: "node-1"})
	if err != nil {
		t.Fatalf("NewAdvertiser: %v", err)
	}
	if err := ad.EmitOnce(context.Background()); err != nil {
		t.Fatalf("EmitOnce: %v", err)
	}
	if pub.topic != TopicBlobHave {
		t.Errorf("topic = %q want %q", pub.topic, TopicBlobHave)
	}
	if pub.Count() != 1 {
		t.Fatalf("expected 1 publish, got %d", pub.Count())
	}
	got, err := DecodeHaveAdvertisement(pub.Calls()[0])
	if err != nil {
		t.Fatalf("DecodeHaveAdvertisement: %v", err)
	}
	if got.PeerID != "node-1" {
		t.Errorf("PeerID = %q want node-1", got.PeerID)
	}
	if len(got.CIDs) != 2 || got.CIDs[0] != "aaa" || got.CIDs[1] != "bbb" {
		t.Errorf("CIDs = %v want [aaa bbb]", got.CIDs)
	}
	if got.EmittedAtUnixMs == 0 {
		t.Error("EmittedAtUnixMs was zero")
	}
}

func TestAdvertiserStartEmitsThenTicks(t *testing.T) {
	pub := newFakePublisher()
	lister := staticLister{cids: []string{"cid-x"}}
	ad, err := NewAdvertiser(lister, pub, AdvertiserConfig{
		PeerID:   "node-2",
		Interval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewAdvertiser: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ad.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Wait for at least 2 emissions: the immediate one + one tick.
	got := 0
	deadline := time.After(2 * time.Second)
	for got < 2 {
		select {
		case <-pub.notify:
			got++
		case <-deadline:
			t.Fatalf("only observed %d emissions in 2s", got)
		}
	}
	ad.Stop()
	if err := ad.Start(ctx); err != nil {
		t.Fatalf("Start after Stop: %v", err)
	}
	ad.Stop()
}

func TestAdvertiserRejectsDoubleStart(t *testing.T) {
	pub := newFakePublisher()
	lister := staticLister{}
	ad, _ := NewAdvertiser(lister, pub, AdvertiserConfig{PeerID: "n", Interval: 5 * time.Second})
	if err := ad.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ad.Stop()
	if err := ad.Start(context.Background()); err == nil {
		t.Error("expected error on double Start")
	}
}

func TestAdvertiserValidation(t *testing.T) {
	cases := []struct {
		name   string
		store  TopicLister
		engine PublishTopic
		cfg    AdvertiserConfig
	}{
		{"nil store", nil, newFakePublisher(), AdvertiserConfig{PeerID: "a"}},
		{"nil engine", staticLister{}, nil, AdvertiserConfig{PeerID: "a"}},
		{"empty peerID", staticLister{}, newFakePublisher(), AdvertiserConfig{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewAdvertiser(tc.store, tc.engine, tc.cfg); err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestAdvertiserPublishErrorPropagates(t *testing.T) {
	pub := newFakePublisher()
	pub.fail = errors.New("boom")
	ad, _ := NewAdvertiser(staticLister{cids: []string{"x"}}, pub, AdvertiserConfig{PeerID: "n"})
	err := ad.EmitOnce(context.Background())
	if err == nil {
		t.Fatal("expected error from EmitOnce on publish failure")
	}
}
