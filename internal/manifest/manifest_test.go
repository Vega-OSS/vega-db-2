package manifest_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Vega-OSS/vega-db/internal/manifest"
	"github.com/Vega-OSS/vega-db/internal/vlog"
)

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()

	m, st, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if st.NextFileID != 1 || st.VlogHead.File != 1 {
		t.Fatalf("unexpected initial state: %+v", st)
	}

	// Apply edit
	err = m.Apply(func(s *manifest.State) {
		s.NextFileID = 10
		s.VlogHead = vlog.Position{File: 2, Off: 4096}
		s.Tables = append(s.Tables, manifest.Table{
			ID:       1,
			Level:    0,
			Smallest: []byte("alpha"),
			Largest:  []byte("omega"),
			Size:     2048,
		})
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Reopen
	m2, st2, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	_ = m2

	if st2.NextFileID != 10 {
		t.Fatalf("expected NextFileID 10, got %d", st2.NextFileID)
	}
	if st2.VlogHead.File != 2 || st2.VlogHead.Off != 4096 {
		t.Fatalf("expected VlogHead {2, 4096}, got %+v", st2.VlogHead)
	}
	if len(st2.Tables) != 1 || !bytes.Equal(st2.Tables[0].Smallest, []byte("alpha")) {
		t.Fatalf("tables mismatch in reopened state: %+v", st2.Tables)
	}
}

func TestManifestCrashArtifactCleanup(t *testing.T) {
	dir := t.TempDir()

	// Plant a leftover MANIFEST.tmp
	tmpPath := filepath.Join(dir, "MANIFEST.tmp")
	if err := os.WriteFile(tmpPath, []byte("corrupt_partial_data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Open should delete MANIFEST.tmp cleanly
	_, _, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open failed with leftover tmp file: %v", err)
	}

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("expected MANIFEST.tmp to be deleted, err=%v", err)
	}
}

func TestManifestConcurrentApply(t *testing.T) {
	dir := t.TempDir()
	m, _, err := manifest.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			err := m.Apply(func(s *manifest.State) {
				s.NextFileID++
				s.Tables = append(s.Tables, manifest.Table{
					ID:    uint64(id),
					Level: 0,
				})
			})
			if err != nil {
				t.Errorf("apply in goroutine %d failed: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	_, st, err := manifest.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Tables) != goroutines {
		t.Fatalf("expected %d tables, got %d", goroutines, len(st.Tables))
	}
}
