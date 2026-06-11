package aws

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// makeRoles builds n roles with deterministic names for benchmarking/testing.
func makeRoles(n int) []Role {
	roles := make([]Role, n)
	for i := 0; i < n; i++ {
		roles[i] = Role{
			Name: fmt.Sprintf("Role-%d", i),
			Arn:  fmt.Sprintf("arn:aws:iam::123456789012:role/Role-%d", i),
		}
	}
	return roles
}

// stubFetch returns a fetcher that optionally sleeps to emulate API latency.
func stubFetch(delay time.Duration) lastUsedFetcher {
	return func(ctx context.Context, roleName string) (*time.Time, error) {
		if delay > 0 {
			time.Sleep(delay)
		}
		t := time.Now()
		return &t, nil
	}
}

// hydrateLastUsedChannels is the previous channel-based implementation, kept
// here only so the benchmark can quantify the orchestration overhead removed by
// hydrateLastUsed. It mirrors the old ListRoles worker pool exactly.
func hydrateLastUsedChannels(
	ctx context.Context,
	roles []Role,
	concurrency int,
	fetch lastUsedFetcher,
	onProgress func(),
) {
	type roleResult struct {
		index    int
		lastUsed *time.Time
		err      error
	}

	jobs := make(chan int, len(roles))
	results := make(chan roleResult, len(roles))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				lastUsed, err := fetch(ctx, roles[i].Name)
				select {
				case <-ctx.Done():
					return
				case results <- roleResult{index: i, lastUsed: lastUsed, err: err}:
				}
			}
		}()
	}

	for i := range roles {
		jobs <- i
	}
	close(jobs)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < len(roles); i++ {
			result := <-results
			if result.err != nil {
				roles[result.index].LastUsed = nil
			} else {
				roles[result.index].LastUsed = result.lastUsed
			}
			if onProgress != nil {
				onProgress()
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	<-done
}

func TestHydrateLastUsedPopulatesEveryRole(t *testing.T) {
	roles := makeRoles(200)
	var progress int64
	var mu sync.Mutex

	hydrateLastUsed(context.Background(), roles, defaultListConcurrency, stubFetch(0), func() {
		mu.Lock()
		progress++
		mu.Unlock()
	})

	for i := range roles {
		if roles[i].LastUsed == nil {
			t.Fatalf("role %d was not hydrated", i)
		}
	}
	if progress != int64(len(roles)) {
		t.Fatalf("expected %d progress callbacks, got %d", len(roles), progress)
	}
}

func TestHydrateLastUsedEmptyAndSmall(t *testing.T) {
	// Empty input must not panic or spawn work.
	hydrateLastUsed(context.Background(), nil, defaultListConcurrency, stubFetch(0), nil)

	// Fewer roles than the concurrency cap must still hydrate every role.
	roles := makeRoles(3)
	hydrateLastUsed(context.Background(), roles, defaultListConcurrency, stubFetch(0), nil)
	for i := range roles {
		if roles[i].LastUsed == nil {
			t.Fatalf("small batch: role %d not hydrated", i)
		}
	}
}

func TestHydrateLastUsedRecordsErrorsAsNil(t *testing.T) {
	roles := makeRoles(10)
	failing := func(ctx context.Context, roleName string) (*time.Time, error) {
		return nil, fmt.Errorf("boom")
	}
	hydrateLastUsed(context.Background(), roles, defaultListConcurrency, failing, nil)
	for i := range roles {
		if roles[i].LastUsed != nil {
			t.Fatalf("role %d should be nil after fetch error", i)
		}
	}
}

func benchmarkHydrate(b *testing.B, fn func(context.Context, []Role, int, lastUsedFetcher, func()), n int, delay time.Duration) {
	ctx := context.Background()
	roles := makeRoles(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(ctx, roles, defaultListConcurrency, stubFetch(delay), nil)
	}
}

// BenchmarkHydrateLastUsed measures the new atomic-dispatch implementation.
func BenchmarkHydrateLastUsed(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("atomic-nodelay-%d", n), func(b *testing.B) {
			benchmarkHydrate(b, hydrateLastUsed, n, 0)
		})
	}
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("channels-nodelay-%d", n), func(b *testing.B) {
			benchmarkHydrate(b, hydrateLastUsedChannels, n, 0)
		})
	}
}

// BenchmarkHydrateLastUsedWithLatency mirrors real API latency so the wall-clock
// throughput of the two pools can be compared head to head.
func BenchmarkHydrateLastUsedWithLatency(b *testing.B) {
	const delay = 1 * time.Millisecond
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("atomic-%d", n), func(b *testing.B) {
			benchmarkHydrate(b, hydrateLastUsed, n, delay)
		})
		b.Run(fmt.Sprintf("channels-%d", n), func(b *testing.B) {
			benchmarkHydrate(b, hydrateLastUsedChannels, n, delay)
		})
	}
}
