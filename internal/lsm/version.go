package lsm

import (
	"bytes"
	"sort"
	"sync/atomic"

	"github.com/Vega-OSS/vega-db/internal/record"
	"github.com/Vega-OSS/vega-db/internal/sstable"
)

// Version represents an immutable point-in-time snapshot of active SSTables across levels.
type Version struct {
	L0   []*sstable.Table // Newest first
	L1   []*sstable.Table // Sorted by key, non-overlapping
	refs atomic.Int32
}

// NewVersion constructs a new Version with an initial reference count of 1.
func NewVersion(l0, l1 []*sstable.Table) *Version {
	v := &Version{
		L0: make([]*sstable.Table, len(l0)),
		L1: make([]*sstable.Table, len(l1)),
	}
	copy(v.L0, l0)
	copy(v.L1, l1)
	for _, t := range v.L0 {
		t.Ref()
	}
	for _, t := range v.L1 {
		t.Ref()
	}
	v.refs.Store(1)
	return v
}

// Ref increments the reference count.
func (v *Version) Ref() {
	v.refs.Add(1)
}

// Unref decrements the reference count. When it reaches 0, unrefs all associated tables.
func (v *Version) Unref() {
	if v.refs.Add(-1) == 0 {
		for _, t := range v.L0 {
			t.Unref()
		}
		for _, t := range v.L1 {
			t.Unref()
		}
	}
}

// Get looks up key in L0 tables (newest first), then binary searches L1.
func (v *Version) Get(key []byte) (record.Record, bool, error) {
	// 1. Search L0 (newest to oldest)
	for _, tbl := range v.L0 {
		rec, found, err := tbl.Get(key)
		if err != nil {
			return record.Record{}, false, err
		}
		if found {
			return rec, true, nil
		}
	}

	// 2. Search L1 (tables non-overlapping and sorted)
	idx := sort.Search(len(v.L1), func(i int) bool {
		return bytes.Compare(v.L1[i].Largest(), key) >= 0
	})
	if idx < len(v.L1) {
		tbl := v.L1[idx]
		if bytes.Compare(key, tbl.Smallest()) >= 0 {
			rec, found, err := tbl.Get(key)
			if err != nil {
				return record.Record{}, false, err
			}
			if found {
				return rec, true, nil
			}
		}
	}

	return record.Record{}, false, nil
}

// NewIterator creates a MergeIterator across all L0 and L1 tables in priority order.
func (v *Version) NewIterator() *MergeIterator {
	var children []Iterator
	for _, t := range v.L0 {
		children = append(children, t.NewIterator())
	}
	for _, t := range v.L1 {
		children = append(children, t.NewIterator())
	}
	return NewMergeIterator(children...)
}
