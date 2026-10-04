package engine_test

import (
	"bytes"
	"fmt"
	"testing"

	"vegadb/internal/engine"
)

func TestEngineScanBasicAndPrefetch(t *testing.T) {
	for _, workers := range []int{1, 4, 8} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			dir := t.TempDir()
			opts := engine.DefaultOptions()
			opts.Dir = dir
			opts.MemtableSize = 4 * 1024 // 4 KiB to trigger flushes
			opts.VlogSegmentSize = 8 * 1024
			opts.ScanPrefetch = workers

			db, err := engine.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			const n = 300
			for i := range n {
				k := fmt.Sprintf("key_%05d", i)
				v := fmt.Sprintf("val_%05d", i)
				if err := db.Put([]byte(k), []byte(v)); err != nil {
					t.Fatalf("put %s: %v", k, err)
				}
			}

			// Delete every 5th key
			for i := 0; i < n; i += 5 {
				k := fmt.Sprintf("key_%05d", i)
				if err := db.Delete([]byte(k)); err != nil {
					t.Fatalf("del %s: %v", k, err)
				}
			}

			// Full Scan
			var scannedKeys []string
			var scannedVals []string
			err = db.Scan(nil, nil, 0, func(k, v []byte) bool {
				scannedKeys = append(scannedKeys, string(k))
				scannedVals = append(scannedVals, string(v))
				return true
			})
			if err != nil {
				t.Fatalf("scan: %v", err)
			}

			expectedCount := n - (n / 5)
			if len(scannedKeys) != expectedCount {
				t.Fatalf("expected %d keys, got %d", expectedCount, len(scannedKeys))
			}

			// Check sorted order and deleted keys skipped
			var prev string
			for i, k := range scannedKeys {
				if prev != "" && k <= prev {
					t.Fatalf("keys not strictly sorted: %s <= %s", k, prev)
				}
				prev = k

				var idx int
				fmt.Sscanf(k, "key_%05d", &idx)
				if idx%5 == 0 {
					t.Fatalf("deleted key %s appeared in scan", k)
				}
				expectedVal := fmt.Sprintf("val_%05d", idx)
				if scannedVals[i] != expectedVal {
					t.Fatalf("val mismatch for %s: got %s, want %s", k, scannedVals[i], expectedVal)
				}
			}

			// Range scan with limit
			start := []byte("key_00050")
			end := []byte("key_00100")
			limit := 10
			var rangeKeys []string
			err = db.Scan(start, end, limit, func(k, v []byte) bool {
				rangeKeys = append(rangeKeys, string(k))
				return true
			})
			if err != nil {
				t.Fatalf("range scan: %v", err)
			}
			if len(rangeKeys) != limit {
				t.Fatalf("expected %d range keys, got %d", limit, len(rangeKeys))
			}
			for _, k := range rangeKeys {
				if bytes.Compare([]byte(k), start) < 0 || bytes.Compare([]byte(k), end) >= 0 {
					t.Fatalf("key %s out of range [%s, %s)", k, start, end)
				}
			}

			// Early termination via callback returning false
			earlyCount := 0
			err = db.Scan(nil, nil, 0, func(k, v []byte) bool {
				earlyCount++
				return earlyCount < 7
			})
			if err != nil {
				t.Fatalf("early scan: %v", err)
			}
			if earlyCount != 7 {
				t.Fatalf("expected early stop at 7, got %d", earlyCount)
			}
		})
	}
}
