/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// DefaultFetchInterval is the replicator tick cadence used when
// ReplicatorConfig.FetchInterval is zero.
const DefaultFetchInterval = 5 * time.Minute

// DefaultMaxParallelJobs caps how many piece fetches the replicator runs
// concurrently inside a single tick.
const DefaultMaxParallelJobs = 4

// ReplicatorConfig tunes the opt-in seed loop.
type ReplicatorConfig struct {
	// PeerID identifies this node. Required.
	PeerID string
	// FetchInterval is how often the replicator scans for rarer pieces to
	// fetch. Defaults to DefaultFetchInterval when zero.
	FetchInterval time.Duration
	// MaxParallelJobs bounds the number of concurrent piece fetches within
	// one tick. Defaults to DefaultMaxParallelJobs.
	MaxParallelJobs int
	// RarestFirst is the shared state used for candidate discovery and
	// rarest-first ordering. Required — the Replicator has no independent
	// view of what CIDs exist on the mesh.
	RarestFirst *RarestFirstState
	// Candidates, if set, overrides how the replicator enumerates CIDs it
	// might want to fetch. Defaults to pulling RarestFirst.KnownCIDs().
	// Callers that want to restrict to a specific allowlist (e.g., only
	// CIDs referenced by a local catalog) can plug that in here.
	Candidates CandidateSource
	// AbuseGuard, if non-nil, gates every opportunistically-fetched piece
	// via AbuseGuard.Check before calling store.Put. This applies the
	// "replication" quota (per-source-peer + aggregate) which is kept
	// distinct from the uploader quota ValidateStorable charges on fresh
	// uploads. Nil disables the gate (useful for tests and for deployments
	// that trust their candidate source implicitly).
	AbuseGuard *AbuseGuard
	// ReplicationRecordType overrides the record-type label passed to
	// AbuseGuard.Check on each fetch. Defaults to RecordTypeReplication.
	// Callers whose CandidateSource yields CIDs of a known application
	// kind (e.g. a listing-replica job) can supply that here so the guard
	// applies the correct per-type policy.
	ReplicationRecordType RecordType
	// Now, if set, replaces time.Now() for deterministic tests.
	Now func() time.Time
}

// CandidateSource supplies the set of CIDs the replicator should consider
// fetching on each tick. Typical implementations consult the rarest-first
// tracker, a local catalog of signed references, or a pinning contract.
type CandidateSource interface {
	Candidates(ctx context.Context) ([]string, error)
}

// CandidateSourceFunc adapts a plain function to CandidateSource.
type CandidateSourceFunc func(ctx context.Context) ([]string, error)

// Candidates implements CandidateSource.
func (f CandidateSourceFunc) Candidates(ctx context.Context) ([]string, error) { return f(ctx) }

// Replicator is the opt-in seed loop. It periodically asks the rarest-first
// tracker for CIDs we don't hold locally, ranks them rarest-first, then
// fetches up to MaxParallelJobs pieces per tick. Fetched pieces land in
// the Store via the Fetcher's existing cache-back integration.
//
// The Replicator is a passive participant: it does not pin content,
// negotiate fees, or issue proof-of-storage challenges. Those belong to
// the Phase 4 pinning-contract layer (see ROADMAP.md section 2 "Optional
// paid pinning"). The replicator only implements the opportunistic
// replication described at ROADMAP.md lines 179-184.
//
// TODO(phase4-pinning): once PlutusStorageEscrow.sol lands, the replicator
// should pull pinned-CID allowlists from the contract as a CandidateSource
// and retain those pieces against quota pressure.
// TODO(phase4-tit-for-tat): track per-peer served/received bytes so peers
// that have seeded to us get priority when they request pieces back.
type Replicator struct {
	store       *Store
	advertiser  *Advertiser
	resolver    *Resolver
	fetcher     *Fetcher
	cfg         ReplicatorConfig
	interval    time.Duration
	maxJobs     int
	now         func() time.Time
	candidates  CandidateSource
	abuseGuard  *AbuseGuard
	replType    RecordType

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewReplicator wires up the seed loop. The advertiser and resolver args
// are retained even when not strictly needed inside the loop so that a
// future refactor (e.g. adaptive tick rate based on mesh health) has them
// at hand.
func NewReplicator(cfg ReplicatorConfig, store *Store, advertiser *Advertiser, resolver *Resolver, fetcher *Fetcher) (*Replicator, error) {
	if store == nil {
		return nil, errors.New("distributed: replicator needs a non-nil store")
	}
	if fetcher == nil {
		return nil, errors.New("distributed: replicator needs a non-nil fetcher")
	}
	if cfg.PeerID == "" {
		return nil, errors.New("distributed: replicator needs a non-empty PeerID")
	}
	if cfg.RarestFirst == nil {
		return nil, errors.New("distributed: replicator needs a non-nil RarestFirst state")
	}
	interval := cfg.FetchInterval
	if interval <= 0 {
		interval = DefaultFetchInterval
	}
	maxJobs := cfg.MaxParallelJobs
	if maxJobs <= 0 {
		maxJobs = DefaultMaxParallelJobs
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	candidates := cfg.Candidates
	if candidates == nil {
		rf := cfg.RarestFirst
		candidates = CandidateSourceFunc(func(ctx context.Context) ([]string, error) {
			return rf.KnownCIDs(), nil
		})
	}
	replType := cfg.ReplicationRecordType
	if replType == "" {
		replType = RecordTypeReplication
	}
	return &Replicator{
		store:      store,
		advertiser: advertiser,
		resolver:   resolver,
		fetcher:    fetcher,
		cfg:        cfg,
		interval:   interval,
		maxJobs:    maxJobs,
		now:        now,
		candidates: candidates,
		abuseGuard: cfg.AbuseGuard,
		replType:   replType,
	}, nil
}

// Start launches the seed loop. Returns an error if already running. The
// first tick fires immediately so the node doesn't sit idle for a full
// FetchInterval on boot.
func (r *Replicator) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return errors.New("distributed: replicator already running")
	}
	r.running = true
	r.done = make(chan struct{})
	loopCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.mu.Unlock()

	go r.loop(loopCtx)
	return nil
}

// Stop halts the seed loop and waits for any in-flight tick to drain.
// Safe to call multiple times.
func (r *Replicator) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	cancel := r.cancel
	done := r.done
	r.cancel = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (r *Replicator) loop(ctx context.Context) {
	defer func() {
		r.mu.Lock()
		done := r.done
		r.done = nil
		r.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()

	// Prime tick — don't wait a full interval.
	r.tickOnce(ctx)

	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.tickOnce(ctx)
		}
	}
}

// Tick runs a single replication scan synchronously. Exposed for tests and
// manual invocations (e.g., an operator "replicate now" RPC).
func (r *Replicator) Tick(ctx context.Context) error {
	return r.tickOnce(ctx)
}

// tickOnce performs the core scan: enumerate candidate CIDs → filter to
// those we don't hold → rank rarest-first → fetch up to maxJobs. Errors
// from individual piece fetches are swallowed — one bad peer must not
// stall the whole tick. A context cancel aborts and returns ctx.Err().
func (r *Replicator) tickOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cids, err := r.candidates.Candidates(ctx)
	if err != nil {
		return err
	}
	missing := r.filterMissing(ctx, cids)
	if len(missing) == 0 {
		return nil
	}
	// Rarest-first ordering. The replicator is deliberately greedy: it
	// always fetches the least-replicated first so the tail of the
	// distribution gets bandwidth before the popular pieces.
	ordered := r.cfg.RarestFirst.RankRarest(missing)
	return r.fetchBatch(ctx, ordered)
}

// filterMissing returns only the CIDs the store does NOT currently hold.
// A Has error is treated as "missing" (conservative — we'd rather try a
// fetch than skip a potential hole).
func (r *Replicator) filterMissing(ctx context.Context, cids []string) []string {
	out := make([]string, 0, len(cids))
	for _, cid := range cids {
		if cid == "" {
			continue
		}
		has, err := r.store.Has(ctx, cid)
		if err == nil && has {
			continue
		}
		out = append(out, cid)
	}
	return out
}

// fetchBatch launches up to maxJobs concurrent fetches, preserving the
// input order (rarest-first) in the dispatched order. Returns ctx.Err()
// if cancellation fires mid-batch; otherwise nil even if individual
// fetches fail — partial progress is still progress.
func (r *Replicator) fetchBatch(ctx context.Context, ordered []string) error {
	sem := make(chan struct{}, r.maxJobs)
	var wg sync.WaitGroup
	for _, cid := range ordered {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(cid string) {
			defer wg.Done()
			defer func() { <-sem }()
			data, fromPeer, err := r.fetcher.FetchWithPeer(ctx, cid)
			if err != nil {
				// Transient — may be quota, peer offline, or CID unknown.
				// Next tick re-tries. Do not promote to a tick-level error.
				return
			}
			// AbuseGuard admission gate. Replicated pieces carry no
			// uploader signature, so they go through the lightweight
			// Check() path instead of ValidateStorable. The guard keeps a
			// distinct "replication" ledger (per-source-peer + aggregate
			// caps) from the uploader ledger so opportunistic caching
			// cannot starve legitimate uploads, and vice versa.
			//
			// ErrAlreadyHeld is benign — another tick raced us to the
			// charge; skip the Put and let the previous caller's Put
			// carry the storage. Any other error means we drop the
			// fetched bytes entirely (no Put, no re-advertise) so the
			// replicator does NOT help propagate content that cannot
			// pass our admission policy.
			if r.abuseGuard != nil {
				if err := r.abuseGuard.Check(ctx, cid, fromPeer, int64(len(data)), r.replType); err != nil {
					if errors.Is(err, ErrAlreadyHeld) {
						return
					}
					log.Printf("distributed: replicator abuse-check denied cid=%s from=%s: %v",
						cid, fromPeer, err)
					return
				}
			}
			// Cache back. Store.Put ignores duplicates; if quota refuses
			// the put we surface the rejection to the caller via the
			// returned data being cached nowhere — that's fine: next tick
			// will try again once headroom opens up.
			//
			// Unlike Store.Get's fallback path (which also caches), we
			// call Put explicitly because the replicator's Fetch() path
			// does not run through Store.Get.
			if _, err := r.store.Put(ctx, data); err != nil {
				// Storage rejected after the abuse-guard accepted — refund
				// the replication-ledger charge so the peer's quota isn't
				// permanently consumed by a piece we never actually kept.
				if r.abuseGuard != nil {
					r.abuseGuard.ReleaseReplication(cid, fromPeer)
				}
				return
			}
			// Re-advertise immediately so the mesh sees organic replication
			// without waiting a full 60s advertise-interval tick. Best-
			// effort: a failed emission will be covered by the next
			// scheduled tick, so we neither retry nor propagate the error.
			if r.advertiser != nil {
				_ = r.advertiser.EmitOnce(ctx)
			}
		}(cid)
	}
	wg.Wait()
	return nil
}
