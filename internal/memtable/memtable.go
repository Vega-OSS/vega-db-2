package memtable

import (
	"bytes"
	"sync"
	"unsafe"

	"github.com/Vega-OSS/vega-db/internal/arena"
	"github.com/Vega-OSS/vega-db/internal/record"
)

const (
	maxHeight = 12
	slabSize  = 4096
)

type node struct {
	key    []byte // points into the arena
	rec    record.Record
	height uint8
	next   [maxHeight]*node
}

// Entry is a key-record pair returned during active memtable range copies.
type Entry struct {
	Key []byte
	Rec record.Record
}

type xorshift32 uint32

func (x *xorshift32) next() uint32 {
	v := *x
	v ^= v << 13
	v ^= v >> 17
	v ^= v << 5
	*x = v
	return uint32(v)
}

// Memtable represents an ordered skip list mapping keys to 17-byte records.
type Memtable struct {
	mu         sync.RWMutex
	arena      *arena.Arena
	head       *node
	height     int
	count      int
	slabs      [][]node
	slabOffset int
	rng        xorshift32
	frozen     bool
}

// New creates an initialized Memtable with its own arena and head node.
func New() *Memtable {
	m := &Memtable{
		arena:      arena.New(arena.DefaultChunkSize),
		head:       &node{height: maxHeight},
		height:     1,
		rng:        xorshift32(0x12345678),
		slabs:      [][]node{make([]node, slabSize)},
		slabOffset: 0,
	}
	return m
}

func (m *Memtable) randomHeight() int {
	h := 1
	for h < maxHeight && (m.rng.next()&3) == 0 {
		h++
	}
	return h
}

func (m *Memtable) allocNode() *node {
	if m.slabOffset == slabSize {
		m.slabs = append(m.slabs, make([]node, slabSize))
		m.slabOffset = 0
	}
	n := &m.slabs[len(m.slabs)-1][m.slabOffset]
	m.slabOffset++
	return n
}

// Put inserts or overwrites key with the given record.
// Key bytes are copied into the arena.
func (m *Memtable) Put(key []byte, rec record.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.frozen {
		panic("memtable: cannot Put into a frozen memtable")
	}

	var prev [maxHeight]*node
	curr := m.head
	for i := m.height - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
		prev[i] = curr
	}

	if curr.next[0] != nil && bytes.Equal(curr.next[0].key, key) {
		// Overwrite existing record without re-allocating key
		curr.next[0].rec = rec
		return
	}

	h := m.randomHeight()
	if h > m.height {
		for i := m.height; i < h; i++ {
			prev[i] = m.head
		}
		m.height = h
	}

	arenaKey := m.arena.Alloc(len(key))
	copy(arenaKey, key)

	n := m.allocNode()
	n.key = arenaKey
	n.rec = rec
	n.height = uint8(h)

	for i := 0; i < h; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	m.count++
}

// Get retrieves the record associated with key, returning false if not found.
func (m *Memtable) Get(key []byte) (record.Record, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	curr := m.head
	for i := m.height - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]
	if curr != nil && bytes.Equal(curr.key, key) {
		return curr.rec, true
	}
	return record.Record{}, false
}

// ApproxSize returns estimated memory usage: arena reserved bytes plus allocated node memory.
func (m *Memtable) ApproxSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodeBytes := len(m.slabs) * slabSize * int(unsafe.Sizeof(node{}))
	return m.arena.Size() + nodeBytes
}

// Len returns the number of active entries in the memtable.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// Freeze marks the memtable read-only.
func (m *Memtable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen = true
}

// CopyRange returns a snapshot slice of entries within [start, end) up to limit.
func (m *Memtable) CopyRange(start, end []byte, limit int) []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	curr := m.head
	for i := m.height - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, start) < 0 {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]

	var res []Entry
	count := 0
	for curr != nil {
		if len(end) > 0 && bytes.Compare(curr.key, end) >= 0 {
			break
		}
		keyCopy := make([]byte, len(curr.key))
		copy(keyCopy, curr.key)
		res = append(res, Entry{
			Key: keyCopy,
			Rec: curr.rec,
		})
		count++
		if limit > 0 && count >= limit {
			break
		}
		curr = curr.next[0]
	}
	return res
}
