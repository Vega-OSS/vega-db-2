package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"vegadb/internal/manifest"
	"vegadb/internal/memtable"
	"vegadb/internal/record"
	"vegadb/internal/sstable"
	"vegadb/internal/vlog"
)

// DefaultCompactionSplitSize is 64 MiB.
const DefaultCompactionSplitSize int64 = 64 * 1024 * 1024

// Flush translates a frozen memtable into a new L0 SSTable, updates the manifest, and returns an updated Version.
func Flush(
	imm *memtable.Memtable,
	vlogHead vlog.Position,
	sstDir string,
	manifestMgr *manifest.Manifest,
	currentVersion *Version,
) (*Version, uint64, error) {
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return nil, 0, err
	}

	var fileID uint64
	err := manifestMgr.Apply(func(s *manifest.State) {
		fileID = s.NextFileID
		s.NextFileID++
	})
	if err != nil {
		return nil, 0, fmt.Errorf("lsm: reserve file id: %w", err)
	}

	sstPath := filepath.Join(sstDir, fmt.Sprintf("%08d.sst", fileID))
	builder, err := sstable.NewBuilder(sstPath, sstable.DefaultBlockSize, 10)
	if err != nil {
		return nil, 0, fmt.Errorf("lsm: create builder: %w", err)
	}

	it := imm.NewIterator()
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		if err := builder.Add(it.Key(), it.Rec()); err != nil {
			return nil, 0, fmt.Errorf("lsm: write key to sstable: %w", err)
		}
	}

	meta, err := builder.Finish()
	if err != nil {
		return nil, 0, fmt.Errorf("lsm: finish sstable: %w", err)
	}

	tbl, err := sstable.OpenTable(sstPath, fileID)
	if err != nil {
		return nil, 0, fmt.Errorf("lsm: open flushed table: %w", err)
	}

	// Update manifest: add table to level 0 and update vlogHead
	err = manifestMgr.Apply(func(s *manifest.State) {
		s.VlogHead = vlogHead
		s.Tables = append(s.Tables, manifest.Table{
			ID:       fileID,
			Level:    0,
			Smallest: meta.Smallest,
			Largest:  meta.Largest,
			Size:     meta.Size,
		})
	})
	if err != nil {
		tbl.Unref()
		return nil, 0, fmt.Errorf("lsm: apply flush to manifest: %w", err)
	}

	newL0 := make([]*sstable.Table, 0, len(currentVersion.L0)+1)
	newL0 = append(newL0, tbl) // Newest first
	newL0 = append(newL0, currentVersion.L0...)

	newVersion := NewVersion(newL0, currentVersion.L1)
	tbl.Unref() // Transfer ownership to Version

	return newVersion, fileID, nil
}

// CompactL0ToL1 merges L0 tables and overlapping L1 tables into new L1 tables when len(L0) >= 4.
// Tombstones are dropped because L1 is the lowest level.
func CompactL0ToL1(
	sstDir string,
	manifestMgr *manifest.Manifest,
	currentVersion *Version,
	splitSize int64,
) (*Version, bool, error) {
	if len(currentVersion.L0) < 4 {
		return currentVersion, false, nil
	}

	if splitSize <= 0 {
		splitSize = DefaultCompactionSplitSize
	}

	// 1. Identify key range of all L0 tables
	var smallest, largest []byte
	for _, t := range currentVersion.L0 {
		if len(smallest) == 0 || bytes.Compare(t.Smallest(), smallest) < 0 {
			smallest = t.Smallest()
		}
		if len(largest) == 0 || bytes.Compare(t.Largest(), largest) > 0 {
			largest = t.Largest()
		}
	}

	// 2. Find overlapping L1 tables
	var overlappingL1 []*sstable.Table
	var nonOverlappingL1 []*sstable.Table
	var oldTableIDs = make(map[uint64]bool)

	for _, t := range currentVersion.L0 {
		oldTableIDs[t.ID()] = true
	}

	for _, t := range currentVersion.L1 {
		if bytes.Compare(t.Largest(), smallest) >= 0 && bytes.Compare(t.Smallest(), largest) <= 0 {
			overlappingL1 = append(overlappingL1, t)
			oldTableIDs[t.ID()] = true
		} else {
			nonOverlappingL1 = append(nonOverlappingL1, t)
		}
	}

	// 3. Build merge iterator across L0 (newest first) and overlapping L1
	var children []Iterator
	for _, t := range currentVersion.L0 {
		children = append(children, t.NewIterator())
	}
	for _, t := range overlappingL1 {
		children = append(children, t.NewIterator())
	}

	mIter := NewMergeIterator(children...)
	defer mIter.Close()

	var newL1Tables []*sstable.Table
	var newManifestTables []manifest.Table

	var currentBuilder *sstable.Builder
	var currentFileID uint64
	var currentPath string

	var nextIDCounter atomic.Uint64
	_ = manifestMgr.Apply(func(s *manifest.State) {
		nextIDCounter.Store(s.NextFileID)
	})

	finishCurrentBuilder := func() error {
		if currentBuilder == nil {
			return nil
		}
		meta, err := currentBuilder.Finish()
		if err != nil {
			return err
		}
		if meta.Count > 0 {
			tbl, err := sstable.OpenTable(currentPath, currentFileID)
			if err != nil {
				return err
			}
			newL1Tables = append(newL1Tables, tbl)
			newManifestTables = append(newManifestTables, manifest.Table{
				ID:       currentFileID,
				Level:    1,
				Smallest: meta.Smallest,
				Largest:  meta.Largest,
				Size:     meta.Size,
			})
		}
		currentBuilder = nil
		return nil
	}

	var bytesWrittenInCurrent int64

	for mIter.First(); mIter.Valid(); mIter.Next() {
		rec := mIter.Rec()
		// L1 is the last level: drop tombstones
		if rec.Kind == record.KindTombstone {
			continue
		}

		if currentBuilder == nil {
			currentFileID = nextIDCounter.Add(1) - 1
			currentPath = filepath.Join(sstDir, fmt.Sprintf("%08d.sst", currentFileID))
			b, err := sstable.NewBuilder(currentPath, sstable.DefaultBlockSize, 10)
			if err != nil {
				return currentVersion, false, err
			}
			currentBuilder = b
			bytesWrittenInCurrent = 0
		}

		k := mIter.Key()
		if err := currentBuilder.Add(k, rec); err != nil {
			return currentVersion, false, err
		}
		bytesWrittenInCurrent += int64(len(k) + record.Size)

		if bytesWrittenInCurrent >= splitSize {
			if err := finishCurrentBuilder(); err != nil {
				return currentVersion, false, err
			}
		}
	}

	if err := finishCurrentBuilder(); err != nil {
		return currentVersion, false, err
	}

	// Combine remaining non-overlapping L1 tables with new L1 tables and sort by key
	allL1 := make([]*sstable.Table, 0, len(nonOverlappingL1)+len(newL1Tables))
	allL1 = append(allL1, nonOverlappingL1...)
	allL1 = append(allL1, newL1Tables...)
	sort.Slice(allL1, func(i, j int) bool {
		return bytes.Compare(allL1[i].Smallest(), allL1[j].Smallest()) < 0
	})

	// 4. Update manifest: remove old L0 and overlapping L1, add new L1, bump NextFileID
	err := manifestMgr.Apply(func(s *manifest.State) {
		s.NextFileID = nextIDCounter.Load()
		var kept []manifest.Table
		for _, t := range s.Tables {
			if !oldTableIDs[t.ID] {
				kept = append(kept, t)
			}
		}
		kept = append(kept, newManifestTables...)
		s.Tables = kept
	})
	if err != nil {
		for _, t := range newL1Tables {
			t.Unref()
		}
		return currentVersion, false, fmt.Errorf("lsm: apply compaction manifest: %w", err)
	}

	// 5. Mark compacted tables obsolete
	for _, t := range currentVersion.L0 {
		t.MarkObsolete()
	}
	for _, t := range overlappingL1 {
		t.MarkObsolete()
	}

	newVersion := NewVersion(nil, allL1)
	for _, t := range newL1Tables {
		t.Unref() // Transfer ownership to newVersion
	}

	return newVersion, true, nil
}
