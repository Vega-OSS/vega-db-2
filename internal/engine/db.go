package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vegadb/internal/lsm"
	"vegadb/internal/manifest"
	"vegadb/internal/memtable"
	"vegadb/internal/record"
	"vegadb/internal/sstable"
	"vegadb/internal/vlog"
)

type readState struct {
	mem     *memtable.Memtable
	imm     *memtable.Memtable
	immTail vlog.Position
	version *lsm.Version
}

// DB represents a VegaDB database instance.
type DB struct {
	opts     Options
	state    atomic.Pointer[readState]
	vlog     *vlog.Log
	manifest *manifest.Manifest
	sstDir   string

	// Concurrency channels
	reqCh         chan *request
	flushCh       chan struct{}
	flushedCh     chan struct{}
	compactCh     chan struct{}
	stopCh        chan struct{}
	flusherDone   chan struct{}
	compactorDone chan struct{}
	writerDone    chan struct{}

	bgWG sync.WaitGroup

	// Operational flags
	closed       atomic.Bool
	readOnly     atomic.Bool
	lastWriteErr error
	closeOnce    sync.Once

	// Mutexes
	compactMu sync.Mutex

	// Counters
	flushes     atomic.Int64
	compactions atomic.Int64
	writeStalls atomic.Int64
	batches     atomic.Int64
	reqCount    atomic.Int64
	puts        atomic.Int64
	gets        atomic.Int64
	deletes     atomic.Int64
	scans       atomic.Int64
}

// Open opens a database instance at opts.Dir according to opts parameters.
func Open(opts Options) (*DB, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("engine: mkdir %s: %w", opts.Dir, err)
	}

	sstDir := filepath.Join(opts.Dir, "sst")
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return nil, fmt.Errorf("engine: mkdir %s: %w", sstDir, err)
	}

	// 1. Open manifest
	manifestMgr, manifestState, err := manifest.Open(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("engine: open manifest: %w", err)
	}

	// 2. Open live SSTables and delete crash orphans
	activeTableIDs := make(map[uint64]bool)
	for _, t := range manifestState.Tables {
		activeTableIDs[t.ID] = true
	}

	entries, err := os.ReadDir(sstDir)
	if err != nil {
		return nil, fmt.Errorf("engine: readdir %s: %w", sstDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".sst")
		id, err := strconv.ParseUint(base, 10, 64)
		if err == nil && !activeTableIDs[id] {
			// Orphan file from incomplete flush or compaction
			_ = os.Remove(filepath.Join(sstDir, entry.Name()))
		}
	}

	var l0Tables []*sstable.Table
	var l1Tables []*sstable.Table

	for _, t := range manifestState.Tables {
		p := filepath.Join(sstDir, fmt.Sprintf("%08d.sst", t.ID))
		tbl, err := sstable.OpenTable(p, t.ID)
		if err != nil {
			return nil, fmt.Errorf("engine: open sstable %d: %w", t.ID, err)
		}
		if t.Level == 0 {
			l0Tables = append(l0Tables, tbl)
		} else {
			l1Tables = append(l1Tables, tbl)
		}
	}

	// L0 newest first: sort by ID descending
	sort.Slice(l0Tables, func(i, j int) bool { return l0Tables[i].ID() > l0Tables[j].ID() })
	// L1 sorted by key
	sort.Slice(l1Tables, func(i, j int) bool {
		return bytes.Compare(l1Tables[i].Smallest(), l1Tables[j].Smallest()) < 0
	})

	initialVersion := lsm.NewVersion(l0Tables, l1Tables)
	for _, t := range l0Tables {
		t.Unref()
	}
	for _, t := range l1Tables {
		t.Unref()
	}

	// 3. Open vLog
	vLog, err := vlog.Open(vlog.Options{
		Dir:         opts.Dir,
		SegmentSize: opts.VlogSegmentSize,
	})
	if err != nil {
		initialVersion.Unref()
		return nil, fmt.Errorf("engine: open vlog: %w", err)
	}

	// 4. Replay vLog from VlogHead into active memtable
	activeMem := memtable.New()
	err = vLog.Replay(manifestState.VlogHead, func(pos vlog.Position, frame []byte) error {
		flags, key, _, err := vlog.DecodeFrame(frame)
		if err != nil {
			return err
		}
		if flags == vlog.FlagPut {
			activeMem.Put(key, record.Record{
				Kind: record.KindPointer,
				File: pos.File,
				Off:  pos.Off,
				Len:  uint32(len(frame)),
			})
		} else {
			activeMem.Put(key, record.Record{Kind: record.KindTombstone})
		}
		return nil
	})
	if err != nil {
		_ = vLog.Close()
		initialVersion.Unref()
		return nil, fmt.Errorf("engine: vlog replay error: %w", err)
	}

	db := &DB{
		opts:          opts,
		vlog:          vLog,
		manifest:      manifestMgr,
		sstDir:        sstDir,
		reqCh:         make(chan *request, 4096),
		flushCh:       make(chan struct{}, 1),
		flushedCh:     make(chan struct{}, 1),
		compactCh:     make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		flusherDone:   make(chan struct{}),
		compactorDone: make(chan struct{}),
		writerDone:    make(chan struct{}),
	}

	db.state.Store(&readState{
		mem:     activeMem,
		imm:     nil,
		version: initialVersion,
	})

	// Start background goroutines
	db.bgWG.Add(3)
	go db.writerLoop()
	go db.flusherLoop()
	go db.compactorLoop()

	if opts.Sync == SyncInterval {
		db.bgWG.Add(1)
		go db.intervalSyncLoop()
	}

	return db, nil
}

func (db *DB) intervalSyncLoop() {
	defer db.bgWG.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-db.stopCh:
			return
		case <-ticker.C:
			_ = db.vlog.Sync()
		}
	}
}

// Close gracefully stops the writer, flushes pending memtable, waits for compaction, fsyncs, and closes files.
// Idempotent and safe to invoke multiple times.
func (db *DB) Close() error {
	var closeErr error
	db.closeOnce.Do(func() {
		db.closed.Store(true)

		// 1. Drain and stop writer
		close(db.reqCh)
		<-db.writerDone

		// 2. Stop background loops
		close(db.stopCh)
		db.bgWG.Wait()

		// 3. Trigger final flush if needed (background loops already stopped)
		st := db.state.Load()
		if st.mem.Len() > 0 && st.imm == nil {
			st.mem.Freeze()
			vlogTail := db.vlog.Tail()
			newSt := &readState{
				mem:     memtable.New(),
				imm:     st.mem,
				immTail: vlogTail,
				version: st.version,
			}
			newSt.version.Ref()
			db.state.Store(newSt)
			st.version.Unref()

			// Perform final flush directly
			newVer, _, err := lsm.Flush(newSt.imm, newSt.immTail, db.sstDir, db.manifest, newSt.version)
			if err == nil {
				finalSt := &readState{
					mem:     newSt.mem,
					imm:     nil,
					version: newVer,
				}
				db.state.Store(finalSt)
				newSt.version.Unref()
			}
		}

		// 4. Final sync and close
		if err := db.vlog.Sync(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
		if err := db.vlog.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}

		currSt := db.state.Load()
		if currSt != nil && currSt.version != nil {
			currSt.version.Unref()
		}
	})
	return closeErr
}

// Stats returns current internal metric counters.
func (db *DB) Stats() Stats {
	st := db.state.Load()
	var l0Count, l1Count int64
	var memBytes int64
	if st != nil {
		if st.mem != nil {
			memBytes = int64(st.mem.ApproxSize())
		}
		if st.version != nil {
			l0Count = int64(len(st.version.L0))
			l1Count = int64(len(st.version.L1))
		}
	}

	return Stats{
		Puts:                db.puts.Load(),
		Gets:                db.gets.Load(),
		Deletes:             db.deletes.Load(),
		Scans:               db.scans.Load(),
		Flushes:             db.flushes.Load(),
		Compactions:         db.compactions.Load(),
		WriteStalls:         db.writeStalls.Load(),
		GroupCommitBatches:  db.batches.Load(),
		GroupCommitRequests: db.reqCount.Load(),
		MemtableBytes:       memBytes,
		L0Tables:            l0Count,
		L1Tables:            l1Count,
	}
}
