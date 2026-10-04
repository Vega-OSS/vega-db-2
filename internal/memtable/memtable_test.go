package memtable_test

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"vegadb/internal/memtable"
	"vegadb/internal/record"
)

func TestMemtableRandomizedMapOracle(t *testing.T) {
	m := memtable.New()
	oracle := make(map[string]record.Record)
	rng := rand.New(rand.NewSource(42))

	const ops = 5000
	for range ops {
		k := fmt.Sprintf("key_%05d", rng.Intn(1000))
		keyBytes := []byte(k)
		op := rng.Intn(3)

		switch op {
		case 0, 1: // Put / Overwrite
			rec := record.Record{
				Kind: record.KindPointer,
				File: uint32(rng.Intn(10)),
				Off:  uint64(rng.Int63n(1000000)),
				Len:  uint32(rng.Intn(5000)),
			}
			m.Put(keyBytes, rec)
			oracle[k] = rec
		case 2: // Get
			rec, found := m.Get(keyBytes)
			expected, expFound := oracle[k]
			if found != expFound {
				t.Fatalf("key %s found=%v, expected=%v", k, found, expFound)
			}
			if found && rec != expected {
				t.Fatalf("key %s rec=%+v, expected=%+v", k, rec, expected)
			}
		}
	}
}

func TestMemtableSortedIteration(t *testing.T) {
	m := memtable.New()
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("k_%04d", (i*37)%200)
		m.Put([]byte(keys[i]), record.Record{Kind: record.KindPointer, Off: uint64(i)})
	}

	sort.Strings(keys)
	// Remove duplicates in sorted slice if any
	uniqueKeys := make([]string, 0, len(keys))
	for i, k := range keys {
		if i == 0 || k != keys[i-1] {
			uniqueKeys = append(uniqueKeys, k)
		}
	}

	m.Freeze()
	it := m.NewIterator()
	defer it.Close()

	idx := 0
	for it.First(); it.Valid(); it.Next() {
		if idx >= len(uniqueKeys) {
			t.Fatalf("iterator produced more items than expected")
		}
		if string(it.Key()) != uniqueKeys[idx] {
			t.Fatalf("at index %d: expected key %s, got %s", idx, uniqueKeys[idx], string(it.Key()))
		}
		idx++
	}
	if idx != len(uniqueKeys) {
		t.Fatalf("iterator yielded %d items, want %d", idx, len(uniqueKeys))
	}
}

func TestMemtableConcurrentReadsWhileWriting(t *testing.T) {
	m := memtable.New()
	const writers = 1
	const readers = 16
	const items = 2000

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Readers
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					k := fmt.Sprintf("key_%04d", rand.Intn(items))
					_, _ = m.Get([]byte(k))
					_ = m.CopyRange([]byte("key_0100"), []byte("key_0200"), 10)
				}
			}
		}()
	}

	// Single writer
	wg.Add(writers)
	go func() {
		defer wg.Done()
		for i := range items {
			k := fmt.Sprintf("key_%04d", i)
			m.Put([]byte(k), record.Record{Kind: record.KindPointer, Off: uint64(i)})
		}
		close(stop)
	}()

	wg.Wait()
}

func TestMemtableAllocations(t *testing.T) {
	m := memtable.New()
	key := []byte("bench_key")
	m.Put(key, record.Record{Kind: record.KindPointer, Off: 100})

	// Get allocations in steady state must be 0
	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = m.Get(key)
	})
	if allocs > 0 {
		t.Fatalf("expected 0 allocs for Get, got %f", allocs)
	}

	// 10k inserts should require far fewer than 10k allocations
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bulk_key_%05d", i))
	}

	m2 := memtable.New()
	allocs10k := testing.AllocsPerRun(1, func() {
		for i := range 10000 {
			m2.Put(keys[i], record.Record{Kind: record.KindPointer, Off: uint64(i)})
		}
	})
	if allocs10k > 50 {
		t.Fatalf("expected <= 50 allocs for 10k Put calls, got %f", allocs10k)
	}

	if m2.ApproxSize() <= 10000 {
		t.Fatalf("expected ApproxSize to reflect inserts, got %d", m2.ApproxSize())
	}
}

func BenchmarkMemtablePutParallel(b *testing.B) {
	m := memtable.New()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			key := []byte(fmt.Sprintf("bench_%08d", i))
			m.Put(key, record.Record{Kind: record.KindPointer, Off: uint64(i)})
		}
	})
}
