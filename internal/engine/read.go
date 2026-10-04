package engine

import (
	"bytes"

	"github.com/Vega-OSS/vega-db/internal/bufpool"
	"github.com/Vega-OSS/vega-db/internal/lsm"
	"github.com/Vega-OSS/vega-db/internal/memtable"
	"github.com/Vega-OSS/vega-db/internal/record"
	"github.com/Vega-OSS/vega-db/internal/vlog"
)

// Get retrieves the value for key, appending it to dst and returning dst.
// Returns ErrNotFound if the key does not exist or has been deleted.
func (db *DB) Get(key, dst []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	db.gets.Add(1)

	st := db.state.Load()
	st.version.Ref()
	defer st.version.Unref()

	// 1. Check active memtable
	if rec, found := st.mem.Get(key); found {
		return db.resolveRecord(rec, dst)
	}

	// 2. Check frozen memtable if present
	if st.imm != nil {
		if rec, found := st.imm.Get(key); found {
			return db.resolveRecord(rec, dst)
		}
	}

	// 3. Check LSM tables
	rec, found, err := st.version.Get(key)
	if err != nil {
		return nil, err
	}
	if found {
		return db.resolveRecord(rec, dst)
	}

	return nil, ErrNotFound
}

func (db *DB) resolveRecord(rec record.Record, dst []byte) ([]byte, error) {
	if rec.Kind == record.KindTombstone {
		return nil, ErrNotFound
	}

	buf := bufpool.Get(int(rec.Len))
	defer buf.Release()

	if err := db.vlog.ReadFrame(rec.File, rec.Off, rec.Len, buf.B); err != nil {
		return nil, err
	}

	_, _, val, err := vlog.DecodeFrame(buf.B[:rec.Len])
	if err != nil {
		return nil, err
	}

	return append(dst, val...), nil
}

type sliceIterator struct {
	entries []memtable.Entry
	idx     int
}

func (s *sliceIterator) First() { s.idx = 0 }
func (s *sliceIterator) SeekGE(key []byte) {
	for s.idx < len(s.entries) && bytes.Compare(s.entries[s.idx].Key, key) < 0 {
		s.idx++
	}
}
func (s *sliceIterator) Next()              { s.idx++ }
func (s *sliceIterator) Valid() bool        { return s.idx < len(s.entries) }
func (s *sliceIterator) Key() []byte        { return s.entries[s.idx].Key }
func (s *sliceIterator) Rec() record.Record { return s.entries[s.idx].Rec }
func (s *sliceIterator) Close()             { s.entries = nil }

// Scan iterates over keys in [start, end) up to limit entries, invoking fn for each key-value pair.
func (db *DB) Scan(start, end []byte, limit int, fn func(key, value []byte) bool) error {
	if db.closed.Load() {
		return ErrClosed
	}

	db.scans.Add(1)

	st := db.state.Load()
	st.version.Ref()
	defer st.version.Unref()

	// 1. Snapshot active memtable under RLock via CopyRange
	activeEntries := st.mem.CopyRange(start, end, limit)
	activeIter := &sliceIterator{entries: activeEntries}

	var children []lsm.Iterator
	children = append(children, activeIter)

	// 2. Frozen memtable iterator
	if st.imm != nil {
		children = append(children, st.imm.NewIterator())
	}

	// 3. Version iterators
	children = append(children, st.version.NewIterator())

	mIter := lsm.NewMergeIterator(children...)
	defer mIter.Close()

	if len(start) > 0 {
		mIter.SeekGE(start)
	} else {
		mIter.First()
	}

	count := 0
	for mIter.Valid() {
		k := mIter.Key()
		if len(end) > 0 && bytes.Compare(k, end) >= 0 {
			break
		}

		rec := mIter.Rec()
		if rec.Kind != record.KindTombstone {
			buf := bufpool.Get(int(rec.Len))
			err := db.vlog.ReadFrame(rec.File, rec.Off, rec.Len, buf.B)
			if err == nil {
				_, _, val, decodeErr := vlog.DecodeFrame(buf.B[:rec.Len])
				if decodeErr == nil {
					count++
					cont := fn(k, val)
					buf.Release()
					if !cont {
						break
					}
					if limit > 0 && count >= limit {
						break
					}
				} else {
					buf.Release()
				}
			} else {
				buf.Release()
			}
		}

		mIter.Next()
	}

	return nil
}
