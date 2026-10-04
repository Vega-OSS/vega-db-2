package engine

import (
	"errors"
	"os"
)

// SyncMode determines the durability fsync policy.
type SyncMode int

const (
	// SyncAlways fsyncs every group commit batch.
	SyncAlways SyncMode = iota

	// SyncInterval fsyncs periodically in the background (every 10 ms).
	SyncInterval

	// SyncNever skips fsyncs (intended for tests only).
	SyncNever
)

// Sentinel errors for the DB engine.
var (
	// ErrNotFound is returned when a queried key does not exist.
	ErrNotFound = errors.New("not found")

	// ErrClosed is returned when an operation is attempted on a closed database.
	ErrClosed = errors.New("db closed")

	// ErrReadOnly indicates the database entered read-only mode following an I/O error.
	ErrReadOnly = errors.New("db is read-only due to previous write failure")
)

// Options specifies configuration parameters for opening a DB instance.
type Options struct {
	Dir             string
	Sync            SyncMode
	MemtableSize    int
	VlogSegmentSize int64
	MaxKeySize      int
	MaxValueSize    int
	ScanPrefetch    int
	BlockCacheSize  int
}

// DefaultOptions returns standard configuration settings.
func DefaultOptions() Options {
	return Options{
		Dir:             "/data",
		Sync:            SyncAlways,
		MemtableSize:    64 * 1024 * 1024,   // 64 MiB
		VlogSegmentSize: 1024 * 1024 * 1024, // 1 GiB
		MaxKeySize:      8192,               // 8 KiB
		MaxValueSize:    16 * 1024 * 1024,   // 16 MiB
		ScanPrefetch:    8,
		BlockCacheSize:  64 * 1024 * 1024,
	}
}

// Validate verifies that required options are well-formed.
func (o Options) Validate() error {
	if o.Dir == "" {
		return errors.New("engine: Dir option must not be empty")
	}
	if o.MemtableSize <= 0 {
		return errors.New("engine: MemtableSize must be positive")
	}
	if o.VlogSegmentSize <= 0 {
		return errors.New("engine: VlogSegmentSize must be positive")
	}
	return nil
}

// Stats provides internal operational counts from the database engine.
type Stats struct {
	Puts                int64
	Gets                int64
	Deletes             int64
	Scans               int64
	Flushes             int64
	Compactions         int64
	WriteStalls         int64
	GroupCommitBatches  int64
	GroupCommitRequests int64
	MemtableBytes       int64
	L0Tables            int64
	L1Tables            int64
}

// Op represents a single batch operation (Put or Delete).
type Op struct {
	Kind  byte // 0 = Put, 1 = Delete
	Key   []byte
	Value []byte
}

func failpoint(name string) {
	if val := os.Getenv("VEGA_FAILPOINT"); val == name {
		os.Exit(1)
	}
}
