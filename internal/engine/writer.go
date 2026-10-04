package engine

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Vega-OSS/vega-db/internal/bufpool"
	"github.com/Vega-OSS/vega-db/internal/lsm"
	"github.com/Vega-OSS/vega-db/internal/memtable"
	"github.com/Vega-OSS/vega-db/internal/record"
	"github.com/Vega-OSS/vega-db/internal/vlog"
)

type request struct {
	kind  byte // 0 = put, 1 = delete
	key   []byte
	value []byte
	err   error
	done  chan struct{}
}

var reqPool = sync.Pool{
	New: func() any {
		return &request{
			done: make(chan struct{}, 1),
		}
	},
}

func getRequest(kind byte, key, value []byte) *request {
	req := reqPool.Get().(*request)
	req.kind = kind
	req.key = key
	req.value = value
	req.err = nil
	return req
}

func putRequest(req *request) {
	req.key = nil
	req.value = nil
	req.err = nil
	reqPool.Put(req)
}

func (db *DB) enqueue(kind byte, key, value []byte) error {
	if db.closed.Load() {
		return ErrClosed
	}
	if db.readOnly.Load() {
		if db.lastWriteErr != nil {
			return db.lastWriteErr
		}
		return ErrReadOnly
	}

	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	if len(key) > db.opts.MaxKeySize {
		return errors.New("engine: key exceeds MaxKeySize")
	}
	if len(value) > db.opts.MaxValueSize {
		return errors.New("engine: value exceeds MaxValueSize")
	}

	req := getRequest(kind, key, value)
	defer putRequest(req)

	select {
	case db.reqCh <- req:
	case <-db.stopCh:
		return ErrClosed
	}

	<-req.done
	return req.err
}

// Put writes key and value to the database.
func (db *DB) Put(key, value []byte) error {
	db.puts.Add(1)
	return db.enqueue(0, key, value)
}

// Delete marks key as deleted with a tombstone.
func (db *DB) Delete(key []byte) error {
	db.deletes.Add(1)
	return db.enqueue(1, key, nil)
}

// Batch applies a sequence of Put/Delete operations in a single group-commit batch.
func (db *DB) Batch(ops []Op) error {
	for _, op := range ops {
		if err := db.enqueue(op.Kind, op.Key, op.Value); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) writerLoop() {
	defer db.bgWG.Done()
	defer close(db.writerDone)

	var batch []*request
	const maxBatchReqs = 512
	const maxBatchBytes = 4 * 1024 * 1024

	for {
		batch = batch[:0]
		req, ok := <-db.reqCh
		if !ok {
			return // Drain completed
		}
		batch = append(batch, req)

		batchBytes := vlog.FrameSize(len(req.key), len(req.value))

		// Drain non-blocking up to limits
	drain:
		for len(batch) < maxBatchReqs && batchBytes < maxBatchBytes {
			select {
			case r, ok := <-db.reqCh:
				if !ok {
					break drain
				}
				batch = append(batch, r)
				batchBytes += vlog.FrameSize(len(r.key), len(r.value))
			default:
				break drain
			}
		}

		db.processBatch(batch, batchBytes)
	}
}

func (db *DB) processBatch(batch []*request, totalFrameBytes int) {
	db.batches.Add(1)
	db.reqCount.Add(int64(len(batch)))

	buf := bufpool.Get(totalFrameBytes)
	defer buf.Release()

	// Encode all frames into buffer
	type frameInfo struct {
		offset uint64
		length uint32
	}
	frames := make([]frameInfo, len(batch))

	var currentOffset uint64
	for i, req := range batch {
		var flag byte = vlog.FlagPut
		if req.kind == 1 {
			flag = vlog.FlagDelete
		}
		written := vlog.EncodeFrame(buf.B[currentOffset:], flag, req.key, req.value)
		frames[i] = frameInfo{
			offset: currentOffset,
			length: uint32(written),
		}
		currentOffset += uint64(written)
	}

	failpoint("vlog_mid_batch")

	// Append batch to vLog
	startPos, err := db.vlog.AppendBatch(buf.B[:currentOffset])
	if err != nil {
		db.failBatch(batch, fmt.Errorf("engine: append vlog batch: %w", err))
		return
	}

	// Durability sync
	if db.opts.Sync == SyncAlways {
		if err := db.vlog.Sync(); err != nil {
			db.failBatch(batch, fmt.Errorf("engine: sync vlog: %w", err))
			return
		}
	}

	// Insert records into active memtable
	st := db.state.Load()
	activeMem := st.mem

	for i, req := range batch {
		var rec record.Record
		if req.kind == 0 {
			rec = record.Record{
				Kind: record.KindPointer,
				File: startPos.File,
				Off:  startPos.Off + frames[i].offset,
				Len:  frames[i].length,
			}
		} else {
			rec = record.Record{Kind: record.KindTombstone}
		}
		activeMem.Put(req.key, rec)
		close(req.done)
	}

	// Check memtable rotation
	if activeMem.ApproxSize() >= db.opts.MemtableSize {
		db.rotateMemtable(activeMem)
	}
}

func (db *DB) failBatch(batch []*request, err error) {
	db.readOnly.Store(true)
	db.lastWriteErr = err
	for _, req := range batch {
		req.err = err
		close(req.done)
	}
}

func (db *DB) rotateMemtable(activeMem *memtable.Memtable) {
	// Stall writer if previous imm is still unflushed
	for {
		st := db.state.Load()
		if st.imm == nil {
			break
		}
		db.writeStalls.Add(1)
		select {
		case <-db.flushedCh:
		case <-time.After(5 * time.Millisecond):
		}
	}

	activeMem.Freeze()
	vlogTail := db.vlog.Tail()

	freshMem := memtable.New()
	for {
		oldSt := db.state.Load()
		newSt := &readState{
			mem:     freshMem,
			imm:     activeMem,
			version: oldSt.version,
		}
		newSt.version.Ref()
		if db.state.CompareAndSwap(oldSt, newSt) {
			oldSt.version.Unref()
			break
		}
		newSt.version.Unref()
	}

	// Signal flusher with vlogTail
	_ = vlogTail
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
}

func (db *DB) flusherLoop() {
	defer db.bgWG.Done()

	for {
		select {
		case <-db.stopCh:
			return
		case <-db.flushCh:
			db.doFlush()
		}
	}
}

func (db *DB) doFlush() {
	st := db.state.Load()
	if st == nil || st.imm == nil {
		return
	}

	db.flushes.Add(1)
	vlogTail := db.vlog.Tail()

	failpoint("flush_after_sst_before_manifest")

	newVersion, _, err := lsm.Flush(st.imm, vlogTail, db.sstDir, db.manifest, st.version)
	if err != nil {
		return
	}

	failpoint("flush_after_manifest")

	// Atomically swap in new readState
	for {
		curr := db.state.Load()
		if curr.imm != st.imm {
			// Already swapped
			newVersion.Unref()
			break
		}
		swapped := &readState{
			mem:     curr.mem,
			imm:     nil,
			version: newVersion,
		}
		if db.state.CompareAndSwap(curr, swapped) {
			curr.version.Unref()
			break
		}
	}

	// Wake up stalled writer
	select {
	case db.flushedCh <- struct{}{}:
	default:
	}

	// Trigger compaction if L0 has >= 4 tables
	if len(newVersion.L0) >= 4 {
		select {
		case db.compactCh <- struct{}{}:
		default:
		}
	}
}

func (db *DB) compactorLoop() {
	defer db.bgWG.Done()

	for {
		select {
		case <-db.stopCh:
			return
		case <-db.compactCh:
			db.doCompaction()
		}
	}
}

func (db *DB) doCompaction() {
	st := db.state.Load()
	if st == nil || st.version == nil || len(st.version.L0) < 4 {
		return
	}

	db.compactions.Add(1)

	failpoint("compaction_after_output_before_manifest")

	newVersion, didCompact, err := lsm.CompactL0ToL1(db.sstDir, db.manifest, st.version, lsm.DefaultCompactionSplitSize)
	if err != nil || !didCompact {
		return
	}

	// Publish new version
	for {
		curr := db.state.Load()
		swapped := &readState{
			mem:     curr.mem,
			imm:     curr.imm,
			version: newVersion,
		}
		if db.state.CompareAndSwap(curr, swapped) {
			curr.version.Unref()
			break
		}
	}
}
