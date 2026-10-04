package metrics

import (
	"expvar"
	"sync"
	"sync/atomic"
)

var expvarMu sync.Mutex

// Metrics manages system counters for VegaDB observability.
type Metrics struct {
	Puts                atomic.Int64
	Gets                atomic.Int64
	GetMisses           atomic.Int64
	Deletes             atomic.Int64
	Scans               atomic.Int64
	ScanEntries         atomic.Int64
	BytesIn             atomic.Int64
	BytesOut            atomic.Int64
	VlogBytesWritten    atomic.Int64
	SSTBytesWritten     atomic.Int64
	Flushes             atomic.Int64
	Compactions         atomic.Int64
	WriteStalls         atomic.Int64
	GroupCommitBatches  atomic.Int64
	GroupCommitRequests atomic.Int64
	MemtableBytes       atomic.Int64
	L0Tables            atomic.Int64
	L1Tables            atomic.Int64
	Inflight            atomic.Int64
	RejectedOverloaded  atomic.Int64
}

// New creates a new Metrics instance and registers it with expvar if not already present.
func New() *Metrics {
	m := &Metrics{}
	m.publishExpvars()
	return m
}

func (m *Metrics) publishExpvars() {
	expvarMu.Lock()
	defer expvarMu.Unlock()

	if expvar.Get("puts") != nil {
		return
	}

	// Register counters with expvar for /debug/vars
	expvar.Publish("puts", expvar.Func(func() any { return m.Puts.Load() }))
	expvar.Publish("gets", expvar.Func(func() any { return m.Gets.Load() }))
	expvar.Publish("get_misses", expvar.Func(func() any { return m.GetMisses.Load() }))
	expvar.Publish("deletes", expvar.Func(func() any { return m.Deletes.Load() }))
	expvar.Publish("scans", expvar.Func(func() any { return m.Scans.Load() }))
	expvar.Publish("scan_entries", expvar.Func(func() any { return m.ScanEntries.Load() }))
	expvar.Publish("bytes_in", expvar.Func(func() any { return m.BytesIn.Load() }))
	expvar.Publish("bytes_out", expvar.Func(func() any { return m.BytesOut.Load() }))
	expvar.Publish("vlog_bytes_written", expvar.Func(func() any { return m.VlogBytesWritten.Load() }))
	expvar.Publish("sst_bytes_written", expvar.Func(func() any { return m.SSTBytesWritten.Load() }))
	expvar.Publish("flushes", expvar.Func(func() any { return m.Flushes.Load() }))
	expvar.Publish("compactions", expvar.Func(func() any { return m.Compactions.Load() }))
	expvar.Publish("write_stalls", expvar.Func(func() any { return m.WriteStalls.Load() }))
	expvar.Publish("group_commit_batches", expvar.Func(func() any { return m.GroupCommitBatches.Load() }))
	expvar.Publish("group_commit_requests", expvar.Func(func() any { return m.GroupCommitRequests.Load() }))
	expvar.Publish("memtable_bytes", expvar.Func(func() any { return m.MemtableBytes.Load() }))
	expvar.Publish("l0_tables", expvar.Func(func() any { return m.L0Tables.Load() }))
	expvar.Publish("l1_tables", expvar.Func(func() any { return m.L1Tables.Load() }))
	expvar.Publish("inflight", expvar.Func(func() any { return m.Inflight.Load() }))
	expvar.Publish("rejected_overloaded", expvar.Func(func() any { return m.RejectedOverloaded.Load() }))
}

// Snapshot returns a copy of current metrics values.
func (m *Metrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"puts":                  m.Puts.Load(),
		"gets":                  m.Gets.Load(),
		"get_misses":            m.GetMisses.Load(),
		"deletes":               m.Deletes.Load(),
		"scans":                 m.Scans.Load(),
		"scan_entries":          m.ScanEntries.Load(),
		"bytes_in":              m.BytesIn.Load(),
		"bytes_out":             m.BytesOut.Load(),
		"vlog_bytes_written":    m.VlogBytesWritten.Load(),
		"sst_bytes_written":     m.SSTBytesWritten.Load(),
		"flushes":               m.Flushes.Load(),
		"compactions":           m.Compactions.Load(),
		"write_stalls":          m.WriteStalls.Load(),
		"group_commit_batches":  m.GroupCommitBatches.Load(),
		"group_commit_requests": m.GroupCommitRequests.Load(),
		"memtable_bytes":        m.MemtableBytes.Load(),
		"l0_tables":             m.L0Tables.Load(),
		"l1_tables":             m.L1Tables.Load(),
		"inflight":              m.Inflight.Load(),
		"rejected_overloaded":   m.RejectedOverloaded.Load(),
	}
}
