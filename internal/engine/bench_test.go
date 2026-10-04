package engine_test

import (
	"fmt"
	"testing"

	"vegadb/internal/engine"
)

func BenchmarkPutParallel(b *testing.B) {
	dir := b.TempDir()
	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.Sync = engine.SyncNever // benchmark throughput in memory/vlog without disk wait

	db, err := engine.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := []byte("benchmark_value_bytes_128b_payload_for_testing_wisckey_engine_write_path_allocation_discipline_and_group_commit_concurrency")

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			k := []byte(fmt.Sprintf("bench_key_%08d", i))
			_ = db.Put(k, val)
		}
	})
}

func BenchmarkGetMemtable(b *testing.B) {
	dir := b.TempDir()
	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.Sync = engine.SyncNever

	db, err := engine.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	key := []byte("hot_key")
	val := []byte("hot_value")
	_ = db.Put(key, val)

	dst := make([]byte, 0, len(val)+16)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = db.Get(key, dst[:0])
	}
}

func BenchmarkGetSST(b *testing.B) {
	dir := b.TempDir()
	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.MemtableSize = 4096 // Force flush to SST
	opts.Sync = engine.SyncNever

	db, err := engine.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for i := range 500 {
		_ = db.Put([]byte(fmt.Sprintf("key_%05d", i)), []byte("val"))
	}

	dst := make([]byte, 0, 64)
	key := []byte("key_00250")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = db.Get(key, dst[:0])
	}
}

func BenchmarkScan100(b *testing.B) {
	dir := b.TempDir()
	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.Sync = engine.SyncNever

	db, err := engine.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for i := range 200 {
		_ = db.Put([]byte(fmt.Sprintf("k_%05d", i)), []byte("val"))
	}

	start := []byte("k_00050")
	end := []byte("k_00150")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = db.Scan(start, end, 100, func(_, _ []byte) bool {
			return true
		})
	}
}
