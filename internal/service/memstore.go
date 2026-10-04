package service

import (
	"bytes"
	"sort"
	"sync"
)

// MemStore is an in-memory, volatile implementation of Store used for the walking skeleton.
// It is strictly temporary and not persistent.
type MemStore struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// NewMemStore constructs an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[string][]byte),
	}
}

// Put stores a copy of key and value.
func (m *MemStore) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	v := make([]byte, len(value))
	copy(v, value)
	m.data[string(key)] = v
	return nil
}

// Delete removes the specified key.
func (m *MemStore) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	delete(m.data, string(key))
	return nil
}

// Get appends the stored value for key to dst, returning ErrNotFound if missing.
func (m *MemStore) Get(key, dst []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}
	val, ok := m.data[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return append(dst, val...), nil
}

// Scan iterates over sorted keys within [start, end) up to limit entries.
func (m *MemStore) Scan(start, end []byte, limit int, fn func(key, value []byte) bool) error {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return ErrClosed
	}
	// Copy keys under lock
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	m.mu.RUnlock()

	sort.Strings(keys)

	count := 0
	for _, kStr := range keys {
		k := []byte(kStr)
		if len(start) > 0 && bytes.Compare(k, start) < 0 {
			continue
		}
		if len(end) > 0 && bytes.Compare(k, end) >= 0 {
			break
		}
		m.mu.RLock()
		val, ok := m.data[kStr]
		m.mu.RUnlock()
		if !ok {
			continue
		}
		count++
		if !fn(k, val) {
			break
		}
		if limit > 0 && count >= limit {
			break
		}
	}
	return nil
}

// Stats returns the count of keys in the in-memory store.
func (m *MemStore) Stats() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]int64{
		"keys": int64(len(m.data)),
	}
}

// Close marks the store as closed.
func (m *MemStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
