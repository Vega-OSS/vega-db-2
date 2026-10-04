package lsm

import (
	"bytes"
	"container/heap"

	"vegadb/internal/record"
)

// Iterator defines the common interface for memtable, table, and merge iterators.
type Iterator interface {
	First()
	SeekGE(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Rec() record.Record
	Close()
}

type heapItem struct {
	childIdx int
	key      []byte
	rec      record.Record
}

type iterHeap []*heapItem

func (h iterHeap) Len() int      { return len(h) }
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h iterHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	// Equal keys: lower child index has higher priority
	return h[i].childIdx < h[j].childIdx
}
func (h *iterHeap) Push(x any) { *h = append(*h, x.(*heapItem)) }
func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeIterator combines multiple child iterators in priority order.
// When identical keys appear across multiple children, the highest priority child wins,
// and older duplicates are skipped.
type MergeIterator struct {
	children []Iterator
	h        iterHeap
	currKey  []byte
	currRec  record.Record
	valid    bool
}

// NewMergeIterator creates a merge iterator over children ordered from highest to lowest priority.
func NewMergeIterator(children ...Iterator) *MergeIterator {
	return &MergeIterator{
		children: children,
	}
}

// First positions all children at their first item and builds the heap.
func (m *MergeIterator) First() {
	m.h = m.h[:0]
	for i, child := range m.children {
		child.First()
		if child.Valid() {
			m.h = append(m.h, &heapItem{
				childIdx: i,
				key:      bytes.Clone(child.Key()),
				rec:      child.Rec(),
			})
		}
	}
	heap.Init(&m.h)
	m.advance()
}

// SeekGE positions all children at or after key and rebuilds the heap.
func (m *MergeIterator) SeekGE(key []byte) {
	m.h = m.h[:0]
	for i, child := range m.children {
		child.SeekGE(key)
		if child.Valid() {
			m.h = append(m.h, &heapItem{
				childIdx: i,
				key:      bytes.Clone(child.Key()),
				rec:      child.Rec(),
			})
		}
	}
	heap.Init(&m.h)
	m.advance()
}

func (m *MergeIterator) advance() {
	if len(m.h) == 0 {
		m.valid = false
		m.currKey = nil
		return
	}

	// Pop winning entry
	top := heap.Pop(&m.h).(*heapItem)
	m.currKey = top.key
	m.currRec = top.rec
	m.valid = true

	// Advance winning child
	child := m.children[top.childIdx]
	child.Next()
	if child.Valid() {
		heap.Push(&m.h, &heapItem{
			childIdx: top.childIdx,
			key:      bytes.Clone(child.Key()),
			rec:      child.Rec(),
		})
	}

	// Skip any other items in heap with the exact same key (lower priority versions)
	for len(m.h) > 0 && bytes.Equal(m.h[0].key, m.currKey) {
		dup := heap.Pop(&m.h).(*heapItem)
		dupChild := m.children[dup.childIdx]
		dupChild.Next()
		if dupChild.Valid() {
			heap.Push(&m.h, &heapItem{
				childIdx: dup.childIdx,
				key:      bytes.Clone(dupChild.Key()),
				rec:      dupChild.Rec(),
			})
		}
	}
}

// Next advances to the next distinct key.
func (m *MergeIterator) Next() {
	if !m.valid {
		return
	}
	m.advance()
}

// Valid returns true if the iterator is positioned at a valid entry.
func (m *MergeIterator) Valid() bool {
	return m.valid
}

// Key returns the winning key for the current position.
func (m *MergeIterator) Key() []byte {
	if !m.valid {
		return nil
	}
	return m.currKey
}

// Rec returns the winning record for the current position.
func (m *MergeIterator) Rec() record.Record {
	if !m.valid {
		return record.Record{}
	}
	return m.currRec
}

// Close closes all underlying child iterators.
func (m *MergeIterator) Close() {
	for _, child := range m.children {
		child.Close()
	}
	m.h = nil
	m.valid = false
}
