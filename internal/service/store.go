package service

import "vegadb/internal/engine"

// Sentinel errors for store operations.
var (
	// ErrNotFound indicates the requested key does not exist.
	ErrNotFound = engine.ErrNotFound

	// ErrClosed indicates the underlying store has been closed.
	ErrClosed = engine.ErrClosed
)

// Store defines the core interface satisfied by any storage engine backing VegaDB.
type Store interface {
	// Put inserts or updates the given key-value pair.
	Put(key, value []byte) error

	// Delete removes the specified key.
	Delete(key []byte) error

	// Get retrieves the value for key, appending it to dst. Returns ErrNotFound if missing.
	Get(key, dst []byte) ([]byte, error)

	// Scan iterates through keys in [start, end) up to limit entries, invoking fn for each pair.
	Scan(start, end []byte, limit int, fn func(key, value []byte) bool) error

	// Stats returns internal counters from the engine.
	Stats() map[string]int64

	// Close shuts down the store and releases resources.
	Close() error
}
