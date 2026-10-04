package engine

import (
	"bytes"
	"sync"

	"vegadb/internal/bufpool"
	"vegadb/internal/lsm"
	"vegadb/internal/record"
	"vegadb/internal/vlog"
)

type scanItem struct {
	key []byte
	rec record.Record
	buf *bufpool.Buf
	val []byte
	err error
}

// Scan iterates over keys in [start, end) up to limit entries, invoking fn for each key-value pair.
// Uses windowed parallel prefetch across vLog segments according to Options.ScanPrefetch.
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

	// 3. Version iterators across SSTable levels
	children = append(children, st.version.NewIterator())

	mIter := lsm.NewMergeIterator(children...)
	defer mIter.Close()

	if len(start) > 0 {
		mIter.SeekGE(start)
	} else {
		mIter.First()
	}

	prefetchWorkers := db.opts.ScanPrefetch
	if prefetchWorkers <= 0 {
		prefetchWorkers = 1
	}

	const windowSize = 64
	count := 0

	for mIter.Valid() {
		// Collect next batch of up to windowSize items
		var batch []*scanItem
		for mIter.Valid() && len(batch) < windowSize {
			k := mIter.Key()
			if len(end) > 0 && bytes.Compare(k, end) >= 0 {
				break
			}
			rec := mIter.Rec()
			mIter.Next()

			if rec.Kind == record.KindTombstone {
				continue
			}

			it := &scanItem{
				key: bytes.Clone(k),
				rec: rec,
			}
			batch = append(batch, it)
			if limit > 0 && count+len(batch) >= limit {
				break
			}
		}

		if len(batch) == 0 {
			break
		}

		// Prefetch values in parallel if workers > 1 and len(batch) > 1
		if prefetchWorkers > 1 && len(batch) > 1 {
			var wg sync.WaitGroup
			sem := make(chan struct{}, prefetchWorkers)
			for _, it := range batch {
				wg.Add(1)
				sem <- struct{}{}
				go func(it *scanItem) {
					defer func() {
						<-sem
						wg.Done()
					}()
					buf := bufpool.Get(int(it.rec.Len))
					if err := db.vlog.ReadFrame(it.rec.File, it.rec.Off, it.rec.Len, buf.B); err != nil {
						buf.Release()
						it.err = err
						return
					}
					_, _, val, err := vlog.DecodeFrame(buf.B[:it.rec.Len])
					if err != nil {
						buf.Release()
						it.err = err
						return
					}
					it.buf = buf
					it.val = val
				}(it)
			}
			wg.Wait()
		} else {
			for _, it := range batch {
				buf := bufpool.Get(int(it.rec.Len))
				if err := db.vlog.ReadFrame(it.rec.File, it.rec.Off, it.rec.Len, buf.B); err != nil {
					buf.Release()
					it.err = err
					continue
				}
				_, _, val, err := vlog.DecodeFrame(buf.B[:it.rec.Len])
				if err != nil {
					buf.Release()
					it.err = err
					continue
				}
				it.buf = buf
				it.val = val
			}
		}

		// Stream to fn in strict key order, releasing buffers immediately
		stopped := false
		for _, it := range batch {
			if it.buf != nil {
				if !stopped && it.err == nil {
					count++
					cont := fn(it.key, it.val)
					it.buf.Release()
					it.buf = nil
					if !cont || (limit > 0 && count >= limit) {
						stopped = true
					}
				} else {
					it.buf.Release()
					it.buf = nil
				}
			}
		}

		if stopped {
			break
		}
	}

	return nil
}
