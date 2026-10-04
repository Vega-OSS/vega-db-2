package lsm_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/Vega-OSS/vega-db/internal/lsm"
	"github.com/Vega-OSS/vega-db/internal/manifest"
	"github.com/Vega-OSS/vega-db/internal/memtable"
	"github.com/Vega-OSS/vega-db/internal/record"
	"github.com/Vega-OSS/vega-db/internal/vlog"
)

func TestMergeIteratorPriority(t *testing.T) {
	// Child 0 has key "apple" with rec Off=100
	m1 := memtable.New()
	m1.Put([]byte("apple"), record.Record{Kind: record.KindPointer, Off: 100})
	m1.Put([]byte("banana"), record.Record{Kind: record.KindPointer, Off: 200})
	m1.Freeze()

	// Child 1 has key "apple" with rec Off=50 (older), and "cherry"
	m2 := memtable.New()
	m2.Put([]byte("apple"), record.Record{Kind: record.KindPointer, Off: 50})
	m2.Put([]byte("cherry"), record.Record{Kind: record.KindPointer, Off: 300})
	m2.Freeze()

	mIter := lsm.NewMergeIterator(m1.NewIterator(), m2.NewIterator())
	defer mIter.Close()

	var keys []string
	var offsets []uint64

	for mIter.First(); mIter.Valid(); mIter.Next() {
		keys = append(keys, string(mIter.Key()))
		offsets = append(offsets, mIter.Rec().Off)
	}

	if len(keys) != 3 {
		t.Fatalf("expected 3 distinct keys, got %d", len(keys))
	}
	if keys[0] != "apple" || offsets[0] != 100 {
		t.Fatalf("expected apple with highest priority off 100, got %s off %d", keys[0], offsets[0])
	}
	if keys[1] != "banana" || offsets[1] != 200 {
		t.Fatalf("expected banana with off 200, got %s off %d", keys[1], offsets[1])
	}
	if keys[2] != "cherry" || offsets[2] != 300 {
		t.Fatalf("expected cherry with off 300, got %s off %d", keys[2], offsets[2])
	}
}

func TestLSMFlushAndCompactionModel(t *testing.T) {
	dir := t.TempDir()
	manifestDir := filepath.Join(dir, "manifest")
	sstDir := filepath.Join(dir, "sst")

	manifestMgr, _, err := manifest.Open(manifestDir)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}

	version := lsm.NewVersion(nil, nil)
	defer version.Unref()

	oracle := make(map[string]record.Record)
	rng := rand.New(rand.NewSource(12345))

	const totalBatches = 8
	const keysPerBatch = 200

	for b := range totalBatches {
		mem := memtable.New()
		for i := range keysPerBatch {
			k := fmt.Sprintf("k_%04d", rng.Intn(300))
			isDel := rng.Intn(4) == 0

			var rec record.Record
			if isDel {
				rec = record.Record{Kind: record.KindTombstone}
				oracle[k] = rec
			} else {
				rec = record.Record{Kind: record.KindPointer, File: 1, Off: uint64(b*1000 + i)}
				oracle[k] = rec
			}
			mem.Put([]byte(k), rec)
		}
		mem.Freeze()

		newVersion, _, err := lsm.Flush(mem, vlog.Position{File: 1, Off: uint64(b * 1000)}, sstDir, manifestMgr, version)
		if err != nil {
			t.Fatalf("flush %d failed: %v", b, err)
		}
		version.Unref()
		version = newVersion

		// Try compaction if L0 has >= 4 tables
		compactedVersion, didCompact, err := lsm.CompactL0ToL1(sstDir, manifestMgr, version, 4096)
		if err != nil {
			t.Fatalf("compact failed: %v", err)
		}
		if didCompact {
			version.Unref()
			version = compactedVersion
		}
	}

	// Verify all keys against oracle
	for k, expRec := range oracle {
		rec, found, err := version.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s failed: %v", k, err)
		}
		if !found {
			// If not found in LSM, it must have been a tombstone that was compacted away at L1
			if expRec.Kind != record.KindTombstone {
				t.Fatalf("key %s missing in LSM, expected %+v", k, expRec)
			}
		} else {
			if rec.Kind != expRec.Kind {
				t.Fatalf("kind mismatch for key %s: got %d, want %d", k, rec.Kind, expRec.Kind)
			}
			if rec.Kind == record.KindPointer && rec.Off != expRec.Off {
				t.Fatalf("rec mismatch for key %s: got off %d, want %d", k, rec.Off, expRec.Off)
			}
		}
	}
}

func TestLSMConcurrentReadsDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	mDir := filepath.Join(dir, "man")

	manifestMgr, _, err := manifest.Open(mDir)
	if err != nil {
		t.Fatal(err)
	}

	version := lsm.NewVersion(nil, nil)
	defer version.Unref()

	// Flush 4 small L0 tables
	for b := range 4 {
		mem := memtable.New()
		for i := range 100 {
			k := []byte(fmt.Sprintf("key_%04d", i))
			mem.Put(k, record.Record{Kind: record.KindPointer, Off: uint64(b*1000 + i)})
		}
		mem.Freeze()
		v, _, err := lsm.Flush(mem, vlog.Position{File: 1, Off: uint64(b)}, sstDir, manifestMgr, version)
		if err != nil {
			t.Fatal(err)
		}
		version.Unref()
		version = v
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Readers reading from current version
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					k := []byte(fmt.Sprintf("key_%04d", rand.Intn(100)))
					_, _, _ = version.Get(k)
				}
			}
		}()
	}

	// Compact in parallel
	compacted, didCompact, err := lsm.CompactL0ToL1(sstDir, manifestMgr, version, 2048)
	if err != nil {
		t.Fatalf("compaction error: %v", err)
	}
	if !didCompact {
		t.Fatal("expected compaction to trigger with 4 L0 tables")
	}

	close(stop)
	wg.Wait()
	compacted.Unref()
}

func TestLSMScanMergeSorted(t *testing.T) {
	m1 := memtable.New()
	m1.Put([]byte("c"), record.Record{Kind: record.KindPointer, Off: 3})
	m1.Put([]byte("a"), record.Record{Kind: record.KindPointer, Off: 1})
	m1.Freeze()

	m2 := memtable.New()
	m2.Put([]byte("b"), record.Record{Kind: record.KindPointer, Off: 2})
	m2.Put([]byte("d"), record.Record{Kind: record.KindPointer, Off: 4})
	m2.Freeze()

	mIter := lsm.NewMergeIterator(m1.NewIterator(), m2.NewIterator())
	defer mIter.Close()

	var result []string
	for mIter.First(); mIter.Valid(); mIter.Next() {
		result = append(result, string(mIter.Key()))
	}

	expected := []string{"a", "b", "c", "d"}
	if !sort.StringsAreSorted(result) || len(result) != 4 {
		t.Fatalf("expected sorted [a b c d], got %v", result)
	}
	for i := range expected {
		if result[i] != expected[i] {
			t.Fatalf("index %d: got %s, want %s", i, result[i], expected[i])
		}
	}
}
