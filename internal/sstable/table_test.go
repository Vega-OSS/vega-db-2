package sstable_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Vega-OSS/vega-db/internal/record"
	"github.com/Vega-OSS/vega-db/internal/sstable"
)

func TestSSTableBuildAndGet(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "000001.sst")

	builder, err := sstable.NewBuilder(sstPath, 1024, 10)
	if err != nil {
		t.Fatalf("builder error: %v", err)
	}

	const numKeys = 5000
	for i := range numKeys {
		k := []byte(fmt.Sprintf("key_%06d", i))
		rec := record.Record{Kind: record.KindPointer, File: 1, Off: uint64(i * 100), Len: 50}
		if err := builder.Add(k, rec); err != nil {
			t.Fatalf("add %d error: %v", i, err)
		}
	}

	meta, err := builder.Finish()
	if err != nil {
		t.Fatalf("finish error: %v", err)
	}
	if meta.Count != numKeys {
		t.Fatalf("expected count %d, got %d", numKeys, meta.Count)
	}

	tbl, err := sstable.OpenTable(sstPath, 1)
	if err != nil {
		t.Fatalf("open table error: %v", err)
	}
	defer tbl.Unref()

	// 1. Get each key
	for i := range numKeys {
		k := []byte(fmt.Sprintf("key_%06d", i))
		rec, found, err := tbl.Get(k)
		if err != nil || !found {
			t.Fatalf("get key %d failed, found=%v, err=%v", i, found, err)
		}
		if rec.Off != uint64(i*100) {
			t.Fatalf("rec mismatch for key %d: got off %d, want %d", i, rec.Off, i*100)
		}
	}

	// 2. Get absent keys
	for i := range 1000 {
		k := []byte(fmt.Sprintf("absent_%06d", i))
		_, found, err := tbl.Get(k)
		if err != nil {
			t.Fatalf("unexpected error for absent key: %v", err)
		}
		if found {
			t.Fatalf("expected absent key %s to not be found", string(k))
		}
	}

	// 3. Iterate in order
	it := tbl.NewIterator()
	defer it.Close()

	idx := 0
	for it.First(); it.Valid(); it.Next() {
		expectedKey := []byte(fmt.Sprintf("key_%06d", idx))
		if !bytes.Equal(it.Key(), expectedKey) {
			t.Fatalf("iter key mismatch at %d: got %s, want %s", idx, string(it.Key()), string(expectedKey))
		}
		idx++
	}
	if idx != numKeys {
		t.Fatalf("iterated %d keys, want %d", idx, numKeys)
	}

	// 4. Seek between keys
	it.SeekGE([]byte("key_002500"))
	if !it.Valid() || !bytes.Equal(it.Key(), []byte("key_002500")) {
		t.Fatalf("seek failed: got %s", string(it.Key()))
	}
}

func TestSSTableSingleKeyAndEmpty(t *testing.T) {
	dir := t.TempDir()

	// Single key
	p1 := filepath.Join(dir, "single.sst")
	b1, _ := sstable.NewBuilder(p1, 1024, 10)
	_ = b1.Add([]byte("alone"), record.Record{Kind: record.KindPointer, Off: 42})
	_, _ = b1.Finish()

	t1, err := sstable.OpenTable(p1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer t1.Unref()

	rec, found, _ := t1.Get([]byte("alone"))
	if !found || rec.Off != 42 {
		t.Fatalf("single key lookup failed")
	}

	// Empty table
	p2 := filepath.Join(dir, "empty.sst")
	b2, _ := sstable.NewBuilder(p2, 1024, 10)
	_, _ = b2.Finish()

	t2, err := sstable.OpenTable(p2, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer t2.Unref()

	_, found, _ = t2.Get([]byte("missing"))
	if found {
		t.Fatal("empty table should not find any key")
	}
}

func TestSSTableCRCError(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "corrupt.sst")
	b, _ := sstable.NewBuilder(p, 512, 10)
	_ = b.Add([]byte("k1"), record.Record{Kind: record.KindPointer, Off: 1})
	_ = b.Add([]byte("k2"), record.Record{Kind: record.KindPointer, Off: 2})
	_, _ = b.Finish()

	// Corrupt first data byte in file
	f, err := os.OpenFile(p, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var bByte [1]byte
	_, _ = f.ReadAt(bByte[:], 5)
	bByte[0] ^= 0xFF
	_, _ = f.WriteAt(bByte[:], 5)
	_ = f.Close()

	tbl, err := sstable.OpenTable(p, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Unref()

	_, _, err = tbl.Get([]byte("k1"))
	if err == nil {
		t.Fatal("expected CRC error on corrupted data block")
	}
}

func TestSSTableConcurrentGets(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "concurrent.sst")
	b, _ := sstable.NewBuilder(p, 1024, 10)
	for i := range 500 {
		_ = b.Add([]byte(fmt.Sprintf("k_%04d", i)), record.Record{Kind: record.KindPointer, Off: uint64(i)})
	}
	_, _ = b.Finish()

	tbl, err := sstable.OpenTable(p, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Unref()

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				k := []byte(fmt.Sprintf("k_%04d", i*2))
				rec, found, err := tbl.Get(k)
				if err != nil || !found || rec.Off != uint64(i*2) {
					t.Errorf("concurrent get failed for %s", string(k))
					return
				}
			}
		}()
	}
	wg.Wait()
}
