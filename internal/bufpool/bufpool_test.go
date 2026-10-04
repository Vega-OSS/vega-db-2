package bufpool_test

import (
	"sync"
	"testing"

	"github.com/Vega-OSS/vega-db/internal/bufpool"
)

func TestBufpoolBoundaries(t *testing.T) {
	sizes := []int{
		1, 256, 512, 513,
		4 * 1024, 4*1024 + 1,
		64 * 1024, 64*1024 + 1,
		1024 * 1024, 1024*1024 + 1,
		16 * 1024 * 1024, 16*1024*1024 + 1,
	}

	for _, sz := range sizes {
		buf := bufpool.Get(sz)
		if len(buf.B) != sz {
			t.Fatalf("expected len %d, got %d", sz, len(buf.B))
		}
		if sz <= 16*1024*1024 && cap(buf.B) < sz {
			t.Fatalf("expected cap >= %d, got %d", sz, cap(buf.B))
		}
		buf.Release()
	}
}

func TestBufpoolConcurrent(t *testing.T) {
	const goroutines = 32
	const iters = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iters {
				b1 := bufpool.Get(100)
				b2 := bufpool.Get(4000)
				b3 := bufpool.Get(65000)

				b1.Release()
				b2.Release()
				b3.Release()
			}
		}()
	}
	wg.Wait()
}

func TestBufpoolAllocsSteadyState(t *testing.T) {
	// Prime the pool
	b := bufpool.Get(4096)
	b.Release()

	allocs := testing.AllocsPerRun(1000, func() {
		buf := bufpool.Get(4096)
		buf.Release()
	})

	if allocs > 0 {
		t.Fatalf("expected 0 allocs/run in steady state, got %f", allocs)
	}
}
