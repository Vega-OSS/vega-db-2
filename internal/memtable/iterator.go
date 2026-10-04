package memtable

import (
	"bytes"

	"vegadb/internal/record"
)

// Iterator provides in-order traversal over a frozen memtable without acquiring locks.
// Keys returned are direct references into the arena; caller must copy if retained.
type Iterator struct {
	m    *Memtable
	curr *node
}

// NewIterator creates an iterator over a frozen memtable.
func (m *Memtable) NewIterator() *Iterator {
	if !m.frozen {
		panic("memtable: NewIterator is only valid on a frozen memtable")
	}
	return &Iterator{m: m}
}

// First positions the iterator at the smallest key.
func (it *Iterator) First() {
	it.curr = it.m.head.next[0]
}

// SeekGE positions the iterator at the first key greater than or equal to key.
func (it *Iterator) SeekGE(key []byte) {
	curr := it.m.head
	for i := it.m.height - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}
	it.curr = curr.next[0]
}

// Next advances the iterator to the next entry.
func (it *Iterator) Next() {
	if it.curr != nil {
		it.curr = it.curr.next[0]
	}
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.curr != nil
}

// Key returns the key of the current entry.
func (it *Iterator) Key() []byte {
	if it.curr == nil {
		return nil
	}
	return it.curr.key
}

// Rec returns the record of the current entry.
func (it *Iterator) Rec() record.Record {
	if it.curr == nil {
		return record.Record{}
	}
	return it.curr.rec
}

// Close releases any resources held by the iterator.
func (it *Iterator) Close() {
	it.curr = nil
}
