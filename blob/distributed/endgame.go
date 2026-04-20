/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package distributed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// DefaultEndgameThreshold is the completion fraction past which the endgame
// fetcher escalates to duplicate-fetch mode. Per the Mercury roadmap
// (section 2 "Endgame fetch") this engages only at the very tail — 0.99 —
// so the bulk of the file still flows through the rarest-first + top-K path
// and we only pay the duplicate-fetch overhead on the last 1% of pieces.
const DefaultEndgameThreshold = 0.99

// DefaultEndgameMinRemaining keeps endgame off for tiny files. A small file
// crossing the 99% threshold may have 0 remaining pieces — endgame is a
// no-op there. The floor ensures we only engage when there's something to
// race.
const DefaultEndgameMinRemaining = 2

// EndgameConfig tunes when and how aggressively the duplicate-fetch mode
// kicks in.
type EndgameConfig struct {
	// Threshold is the completion fraction (held / total) past which endgame
	// kicks in. Zero falls back to DefaultEndgameThreshold.
	Threshold float64
	// MinRemaining is the minimum number of outstanding pieces required
	// before endgame engages. Zero falls back to DefaultEndgameMinRemaining.
	MinRemaining int
	// Now replaces time.Now for deterministic tests. Optional.
	Now func() time.Time
}

// EndgameFetcher wraps Fetcher with BitTorrent-style endgame mode. A
// multi-piece download proceeds normally (rarest-first, topK-by-reputation
// via the underlying Fetcher) until it crosses `Threshold`. Past that point,
// each remaining piece is requested from EVERY known holder in parallel and
// the first valid response wins — this avoids "last-piece stall" where a
// slow peer holds up the whole file.
//
// Endgame is a purely performance feature. Correctness is unchanged: every
// piece is CID-verified by the underlying Fetcher regardless of mode.
type EndgameFetcher struct {
	fetcher  *Fetcher
	resolver *Resolver
	cfg      EndgameConfig
	now      func() time.Time
}

// NewEndgameFetcher wraps fetcher. resolver is used to fan out a final
// dup-fetch call per remaining piece; its Find() is what surfaces "who
// claims to have piece X" so we know who to race against.
//
// Both fetcher and resolver are required — they are the two halves of the
// fetch path and endgame mode needs both.
func NewEndgameFetcher(fetcher *Fetcher, resolver *Resolver, cfg EndgameConfig) (*EndgameFetcher, error) {
	if fetcher == nil {
		return nil, errors.New("distributed: endgame needs a non-nil fetcher")
	}
	if resolver == nil {
		return nil, errors.New("distributed: endgame needs a non-nil resolver")
	}
	if cfg.Threshold <= 0 {
		cfg.Threshold = DefaultEndgameThreshold
	}
	if cfg.Threshold >= 1 {
		return nil, fmt.Errorf("distributed: endgame threshold %.3f >= 1", cfg.Threshold)
	}
	if cfg.MinRemaining <= 0 {
		cfg.MinRemaining = DefaultEndgameMinRemaining
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &EndgameFetcher{
		fetcher:  fetcher,
		resolver: resolver,
		cfg:      cfg,
		now:      now,
	}, nil
}

// FetchFile downloads every piece in manifest.Pieces (in order) and returns
// them in index order. Returns a zero-length slice and an error if any piece
// fails after all fallbacks are exhausted.
//
// Normal mode: each piece goes through f.fetcher.Fetch (which already does
// its own top-K-by-reputation parallelism per piece).
//
// Endgame mode: once `completion >= cfg.Threshold` AND remaining >= MinRemaining,
// the fetcher switches to duplicate-fetch: for every outstanding piece, it
// asks resolver.Find(cid) for all known holders and races FetchPiece against
// all of them (not just top-K). First CID-verified response per piece wins.
func (e *EndgameFetcher) FetchFile(ctx context.Context, manifest *Manifest) ([][]byte, error) {
	if manifest == nil {
		return nil, errors.New("distributed: endgame FetchFile nil manifest")
	}
	n := len(manifest.Pieces)
	if n == 0 {
		return nil, errors.New("distributed: endgame FetchFile empty manifest")
	}

	pieces := make([][]byte, n)
	held := make([]bool, n)
	var mu sync.Mutex
	heldCount := 0

	// Stage 1 — normal mode. Fetch rarest-first through the underlying
	// Fetcher. We issue fetches sequentially (but each Fetch internally
	// races top-K peers) until we cross the endgame threshold, then hand
	// off to stage 2 for the tail.
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Check threshold BEFORE fetching piece i — once we're in endgame
		// territory the remaining pieces get dispatched as a batch.
		mu.Lock()
		done := heldCount
		mu.Unlock()
		if e.shouldEnterEndgame(done, n) {
			break
		}

		p := manifest.Pieces[i]
		data, err := e.fetcher.Fetch(ctx, p.CID)
		if err != nil {
			return nil, fmt.Errorf("distributed: endgame stage-1 piece %d (%s): %w", i, p.CID, err)
		}
		mu.Lock()
		pieces[i] = data
		held[i] = true
		heldCount++
		mu.Unlock()
	}

	// Stage 2 — endgame. Dispatch remaining pieces as parallel duplicate
	// fetches. Each remaining piece races EVERY known holder in parallel
	// (not just top-K); the first CID-verified response wins.
	remaining := e.remainingIndexes(held)
	if len(remaining) == 0 {
		return pieces, nil
	}
	if err := e.fetchEndgameBatch(ctx, manifest, remaining, pieces, held, &mu); err != nil {
		return nil, err
	}

	return pieces, nil
}

// shouldEnterEndgame returns true when the completion ratio crosses the
// configured threshold AND there are at least MinRemaining pieces left to
// fetch. Prevents the 1-piece-file degenerate case from triggering.
func (e *EndgameFetcher) shouldEnterEndgame(done, total int) bool {
	if total == 0 {
		return false
	}
	remaining := total - done
	if remaining < e.cfg.MinRemaining {
		return false
	}
	ratio := float64(done) / float64(total)
	return ratio >= e.cfg.Threshold
}

// remainingIndexes returns indexes for pieces that are not yet held.
func (e *EndgameFetcher) remainingIndexes(held []bool) []int {
	out := make([]int, 0, len(held))
	for i, h := range held {
		if !h {
			out = append(out, i)
		}
	}
	return out
}

// fetchEndgameBatch races every known holder for each remaining piece in
// parallel. Each piece has its own in-flight goroutine that fans out to all
// peers returned by resolver.Find. The first CID-verified response wins;
// the others are cancelled via the per-piece context.
//
// Errors: if ANY remaining piece fails after all fallbacks, the whole
// FetchFile returns that error. Partial progress (some pieces held, some
// not) doesn't produce a valid file so there's no point returning partial
// results.
func (e *EndgameFetcher) fetchEndgameBatch(
	ctx context.Context,
	manifest *Manifest,
	indexes []int,
	pieces [][]byte,
	held []bool,
	mu *sync.Mutex,
) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(indexes))

	for _, idx := range indexes {
		idx := idx
		p := manifest.Pieces[idx]
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each piece: resolve holders, fan out, take first valid.
			answers, err := e.resolver.Find(ctx, p.CID)
			if err != nil {
				errs <- fmt.Errorf("distributed: endgame resolve piece %d (%s): %w", idx, p.CID, err)
				return
			}
			if len(answers) == 0 {
				// Fall back to the regular fetcher one more time — its
				// own resolution path may succeed with a cached topology
				// the bare Find call missed.
				data, ferr := e.fetcher.Fetch(ctx, p.CID)
				if ferr != nil {
					errs <- fmt.Errorf("distributed: endgame fallback piece %d (%s): %w", idx, p.CID, ferr)
					return
				}
				mu.Lock()
				pieces[idx] = data
				held[idx] = true
				mu.Unlock()
				return
			}

			data, err := e.raceAll(ctx, p.CID, answers)
			if err != nil {
				errs <- fmt.Errorf("distributed: endgame race piece %d (%s): %w", idx, p.CID, err)
				return
			}
			mu.Lock()
			pieces[idx] = data
			held[idx] = true
			mu.Unlock()
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// raceAll fans out one FetchPiece call per answer in parallel and returns
// the first CID-verified payload. All losing calls are cancelled via the
// shared context.
func (e *EndgameFetcher) raceAll(ctx context.Context, cid string, answers []PeerAnswer) ([]byte, error) {
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		data []byte
		err  error
	}
	winner := make(chan result, 1)
	var wg sync.WaitGroup

	for _, a := range answers {
		if a.PeerID == "" {
			continue
		}
		a := a
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := e.fetcher.transport.FetchPiece(raceCtx, a.PeerID, FetchRequest{CID: cid})
			if err != nil {
				return
			}
			// Accept empty CID echo (peer opted not to fill it) but reject
			// any non-empty echo that disagrees with the requested CID.
			// Matches Fetcher.Fetch's verification stance.
			if resp.CID != "" && resp.CID != cid {
				return
			}
			// Verify plaintext against CID — the Fetcher does this too,
			// but in endgame mode we're bypassing its parallelism layer
			// so the verification happens here.
			if computePieceCID(resp.Payload) != cid {
				return
			}
			select {
			case winner <- result{data: resp.Payload}:
				cancel()
			default:
				// Someone else already won.
			}
		}()
	}

	go func() {
		wg.Wait()
		// If no-one wrote to winner, signal failure.
		select {
		case winner <- result{err: errors.New("distributed: endgame race exhausted all peers")}:
		default:
		}
	}()

	select {
	case r := <-winner:
		return r.data, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
