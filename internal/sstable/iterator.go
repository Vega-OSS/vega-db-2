package sstable

import (
	"bytes"
	"sort"

	"vegadb/internal/bufpool"
	"vegadb/internal/encoding"
	"vegadb/internal/record"
)

// Iterator traverses keys and records across data blocks of an SSTable.
// Keys returned are sub-slices of pooled block memory and must be copied if kept.
type Iterator struct {
	table    *Table
	blockIdx int

	activeBuf *bufpool.Buf
	payload   []byte
	offset    int

	currKey []byte
	currRec record.Record
	valid   bool
}

// NewIterator creates a new sequential iterator over the table.
func (t *Table) NewIterator() *Iterator {
	return &Iterator{
		table:    t,
		blockIdx: -1,
	}
}

func (it *Iterator) releaseActive() {
	if it.activeBuf != nil {
		it.activeBuf.Release()
		it.activeBuf = nil
		it.payload = nil
		it.offset = 0
	}
}

func (it *Iterator) loadBlock(idx int) bool {
	it.releaseActive()
	if idx < 0 || idx >= len(it.table.index) {
		it.valid = false
		return false
	}

	it.blockIdx = idx
	blk := it.table.index[idx]

	buf := bufpool.Get(int(blk.length))
	if _, err := it.table.file.ReadAt(buf.B, int64(blk.offset)); err != nil {
		buf.Release()
		it.valid = false
		return false
	}

	it.activeBuf = buf
	it.payload = buf.B[:blk.length-4]
	it.offset = 0
	return true
}

func (it *Iterator) parseCurrent() bool {
	if it.offset >= len(it.payload) {
		it.valid = false
		return false
	}

	k, v, next, err := encoding.DecodeEntry(it.payload, it.offset)
	if err != nil {
		it.valid = false
		return false
	}
	it.offset = next

	rec, err := record.Decode(v)
	if err != nil {
		it.valid = false
		return false
	}

	it.currKey = k
	it.currRec = rec
	it.valid = true
	return true
}

// First positions the iterator at the very first entry in the table.
func (it *Iterator) First() {
	if len(it.table.index) == 0 {
		it.valid = false
		return
	}
	if !it.loadBlock(0) {
		return
	}
	it.parseCurrent()
}

// SeekGE positions the iterator at the first entry with key >= target.
func (it *Iterator) SeekGE(key []byte) {
	if len(it.table.index) == 0 {
		it.valid = false
		return
	}

	// Find the block that could contain key
	idx := sort.Search(len(it.table.index), func(i int) bool {
		return bytes.Compare(it.table.index[i].lastKey, key) >= 0
	})
	if idx >= len(it.table.index) {
		it.releaseActive()
		it.valid = false
		return
	}

	if !it.loadBlock(idx) {
		return
	}

	for it.parseCurrent() {
		if bytes.Compare(it.currKey, key) >= 0 {
			it.valid = true
			return
		}
	}

	// If not found in this block, advance to next block
	it.Next()
}

// Next advances the iterator to the next entry.
func (it *Iterator) Next() {
	if !it.valid {
		return
	}

	if it.offset < len(it.payload) {
		if it.parseCurrent() {
			return
		}
	}

	// Move to next block
	nextBlock := it.blockIdx + 1
	for nextBlock < len(it.table.index) {
		if it.loadBlock(nextBlock) && it.parseCurrent() {
			return
		}
		nextBlock++
	}

	it.releaseActive()
	it.valid = false
}

// Valid returns true if the iterator currently points to a valid key-record pair.
func (it *Iterator) Valid() bool {
	return it.valid
}

// Key returns the current entry's key (valid until iterator advances).
func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.currKey
}

// Rec returns the current entry's record.
func (it *Iterator) Rec() record.Record {
	if !it.valid {
		return record.Record{}
	}
	return it.currRec
}

// Close releases any allocated buffers held by the iterator.
func (it *Iterator) Close() {
	it.releaseActive()
	it.valid = false
}
