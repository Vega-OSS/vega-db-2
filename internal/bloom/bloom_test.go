package bloom_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"vegadb/internal/bloom"
)

func TestBloomAccuracy(t *testing.T) {
	const numKeys = 100000
	keys := make([][]byte, numKeys)
	for i := range numKeys {
		keys[i] = []byte(fmt.Sprintf("key_%08d", i))
	}

	filter := bloom.Build(keys, 10)

	// 1. Zero false negatives
	for i, k := range keys {
		if !bloom.MayContain(filter, k) {
			t.Fatalf("false negative for key %d: %s", i, string(k))
		}
	}

	// 2. False positive rate below 3% for 100k absent keys
	falsePositives := 0
	for i := range numKeys {
		absentKey := []byte(fmt.Sprintf("absent_key_%08d", i))
		if bloom.MayContain(filter, absentKey) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(numKeys)
	t.Logf("False positive rate: %f (%d/%d)", fpRate, falsePositives, numKeys)
	if fpRate >= 0.03 {
		t.Fatalf("false positive rate too high: %f >= 0.03", fpRate)
	}
}

func TestBloomSerialization(t *testing.T) {
	keys := [][]byte{[]byte("apple"), []byte("banana"), []byte("cherry")}
	filter := bloom.Build(keys, 10)

	// Write to temp file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "filter.bin")
	if err := os.WriteFile(path, filter, 0644); err != nil {
		t.Fatal(err)
	}

	// Read back
	loaded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range keys {
		if !bloom.MayContain(loaded, k) {
			t.Fatalf("loaded filter false negative for %s", string(k))
		}
	}
	if bloom.MayContain(loaded, []byte("durian")) {
		t.Log("durian might be in filter (normal probabilistic match or miss)")
	}
}
