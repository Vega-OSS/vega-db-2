package engine_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"vegadb/internal/engine"
)

func TestEngineBasicOps(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.MemtableSize = 64 * 1024
	opts.VlogSegmentSize = 128 * 1024

	db, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open error: %v", err)
	}

	// 1. Put
	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// 2. Get
	val, err := db.Get([]byte("hello"), nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("expected world, got %s", string(val))
	}

	// 3. Delete
	if err := db.Delete([]byte("hello")); err != nil {
		t.Fatalf("del: %v", err)
	}

	// 4. Get after delete
	_, err = db.Get([]byte("hello"), nil)
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// 5. Close twice is safe
	if err := db.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close 2 should be safe, got %v", err)
	}
}

func TestEngineCloseAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.Dir = dir

	db, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	const n = 500
	for i := range n {
		k := fmt.Sprintf("k_%04d", i)
		v := fmt.Sprintf("v_%04d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen
	db2, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	defer db2.Close()

	for i := range n {
		k := fmt.Sprintf("k_%04d", i)
		expected := fmt.Sprintf("v_%04d", i)
		val, err := db2.Get([]byte(k), nil)
		if err != nil {
			t.Fatalf("get %s failed after reopen: %v", k, err)
		}
		if string(val) != expected {
			t.Fatalf("val %s mismatch: got %s, want %s", k, string(val), expected)
		}
	}
}

func TestEngineRandomizedModel(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.MemtableSize = 32 * 1024 // Tiny 32 KiB memtable to trigger frequent flushes
	opts.VlogSegmentSize = 64 * 1024

	db, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	valueSizes := []int{0, 1, 100, 4096, 50000}
	oracle := make(map[string][]byte)
	rng := rand.New(rand.NewSource(999))

	const ops = 2000
	for i := range ops {
		k := fmt.Sprintf("user_%04d", rng.Intn(200))
		keyBytes := []byte(k)

		switch rng.Intn(4) {
		case 0, 1: // Put
			sz := valueSizes[rng.Intn(len(valueSizes))]
			val := make([]byte, sz)
			rng.Read(val)
			if err := db.Put(keyBytes, val); err != nil {
				t.Fatalf("put op %d error: %v", i, err)
			}
			oracle[k] = val

		case 2: // Delete
			if err := db.Delete(keyBytes); err != nil {
				t.Fatalf("del op %d error: %v", i, err)
			}
			delete(oracle, k)

		case 3: // Get
			val, err := db.Get(keyBytes, nil)
			expected, exists := oracle[k]
			if !exists {
				if !errors.Is(err, engine.ErrNotFound) {
					t.Fatalf("expected ErrNotFound for %s, got %v", k, err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected value for %s, got err: %v", k, err)
				}
				if !bytes.Equal(val, expected) {
					t.Fatalf("value mismatch for key %s", k)
				}
			}
		}
	}
}

func TestEngineConcurrentDisjointKeys(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.MemtableSize = 64 * 1024
	opts.VlogSegmentSize = 128 * 1024

	db, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const goroutines = 32
	const opsPerWorker = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(workerID int) {
			defer wg.Done()
			baseKey := workerID * 10000

			for i := range opsPerWorker {
				k := []byte(fmt.Sprintf("k_%08d", baseKey+i))
				v := []byte(fmt.Sprintf("val_w%d_%05d", workerID, i))
				if err := db.Put(k, v); err != nil {
					t.Errorf("worker %d put error: %v", workerID, err)
					return
				}
				got, err := db.Get(k, nil)
				if err != nil || string(got) != string(v) {
					t.Errorf("worker %d get error: got %s, want %s", workerID, string(got), string(v))
					return
				}
			}
		}(g)
	}

	wg.Wait()
}
