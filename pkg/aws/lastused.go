package aws

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// defaultListConcurrency bounds how many GetRole calls run in parallel while
// hydrating last-used data. IAM's read APIs are rate limited, so this caps the
// burst we send while still keeping the wall-clock time dominated by a single
// wave of requests for typical accounts.
const defaultListConcurrency = 55

// lastUsedFetcher resolves the last-used timestamp for a single role.
type lastUsedFetcher func(ctx context.Context, roleName string) (*time.Time, error)

// hydrateLastUsed populates the LastUsed field of every role in place using a
// bounded worker pool.
//
// Work is handed out through an atomic index counter rather than a jobs
// channel, and because each worker owns a distinct slice index there is no
// results channel or collector goroutine: workers write straight into
// roles[i]. This keeps allocations flat (no per-role channel traffic) and
// avoids spawning more goroutines than there are roles. onProgress, when
// non-nil, is invoked exactly once per completed role under a mutex so
// non-thread-safe sinks (such as a progress bar) stay safe.
func hydrateLastUsed(
	ctx context.Context,
	roles []Role,
	concurrency int,
	fetch lastUsedFetcher,
	onProgress func(),
) {
	if len(roles) == 0 {
		return
	}

	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(roles) {
		concurrency = len(roles)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		nextIndex  int64 = -1
		progressMu sync.Mutex
		wg         sync.WaitGroup
	)

	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()

			for {
				i := int(atomic.AddInt64(&nextIndex, 1))
				if i >= len(roles) {
					return
				}

				select {
				case <-ctx.Done():
					return
				default:
				}

				lastUsed, err := fetch(ctx, roles[i].Name)
				if err != nil {
					// Log and continue: one role's failure should not abort
					// usage data for the rest of the account.
					fmt.Fprintf(os.Stderr, "Warning: Failed to get last used info for role %s: %v\n", roles[i].Name, err)
					roles[i].LastUsed = nil
				} else {
					roles[i].LastUsed = lastUsed
				}

				if onProgress != nil {
					progressMu.Lock()
					onProgress()
					progressMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
}
