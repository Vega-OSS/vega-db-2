package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"vegadb/internal/bloom"
	"vegadb/internal/bufpool"
	"vegadb/internal/encoding"
	"vegadb/internal/record"
)

// Table represents an open, readable SSTable file.
type Table struct {
	path       string
	id         uint64
	file       *os.File
	index      []blockIndexEntry
	bloom      []byte
	entryCount uint64
	smallest   []byte
	largest    []byte
	fileSize   int64

	refs     atomic.Int32
	obsolete atomic.Bool
}

// OpenTable opens an SSTable file, loads its index and Bloom filter, and verifies footer integrity.
func OpenTable(path string, id uint64) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	fileSize := info.Size()
	if fileSize < FooterSize {
		_ = f.Close()
		return nil, ErrCorruptFooter
	}

	// Read 48-byte footer
	var footBuf [FooterSize]byte
	if _, err := f.ReadAt(footBuf[:], fileSize-FooterSize); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}

	foot, err := decodeFooter(footBuf[:])
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	// Read index block
	var indexEntries []blockIndexEntry
	if foot.indexLen > 0 {
		indexRaw := make([]byte, foot.indexLen)
		if _, err := f.ReadAt(indexRaw, int64(foot.indexOffset)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("sstable: read index block: %w", err)
		}

		offset := 0
		for offset < len(indexRaw) {
			k, val, next, err := encoding.DecodeEntry(indexRaw, offset)
			if err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("sstable: decode index entry: %w", err)
			}
			offset = next
			if len(val) < 12 {
				_ = f.Close()
				return nil, errors.New("sstable: invalid index entry size")
			}
			blkOffset := binary.BigEndian.Uint64(val[0:8])
			blkLen := binary.BigEndian.Uint32(val[8:12])
			kCopy := make([]byte, len(k))
			copy(kCopy, k)
			indexEntries = append(indexEntries, blockIndexEntry{
				lastKey: kCopy,
				offset:  blkOffset,
				length:  blkLen,
			})
		}
	}

	// Read bloom block
	var bloomRaw []byte
	if foot.bloomLen > 0 {
		bloomRaw = make([]byte, foot.bloomLen)
		if _, err := f.ReadAt(bloomRaw, int64(foot.bloomOffset)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("sstable: read bloom block: %w", err)
		}
	}

	t := &Table{
		path:       path,
		id:         id,
		file:       f,
		index:      indexEntries,
		bloom:      bloomRaw,
		entryCount: foot.entryCount,
		fileSize:   fileSize,
	}
	t.refs.Store(1)

	if len(indexEntries) > 0 {
		t.largest = indexEntries[len(indexEntries)-1].lastKey
	}

	return t, nil
}

// ID returns the table's unique identifier.
func (t *Table) ID() uint64 {
	return t.id
}

// Size returns the file size of the SSTable.
func (t *Table) Size() int64 {
	return t.fileSize
}

// Smallest returns the smallest key in the table.
func (t *Table) Smallest() []byte {
	return t.smallest
}

// Largest returns the largest key in the table.
func (t *Table) Largest() []byte {
	return t.largest
}

// Ref increments the reference count of the Table.
func (t *Table) Ref() {
	t.refs.Add(1)
}

// Unref decrements the reference count. When it reaches 0, the table file is closed,
// and if marked obsolete, deleted from disk.
func (t *Table) Unref() {
	if t.refs.Add(-1) == 0 {
		if t.file != nil {
			_ = t.file.Close()
			t.file = nil
		}
		if t.obsolete.Load() {
			_ = os.Remove(t.path)
		}
	}
}

// MarkObsolete marks the table for deletion when its reference count drops to zero.
func (t *Table) MarkObsolete() {
	t.obsolete.Store(true)
}

// Get performs a point lookup for key:
// 1. Checks bloom filter.
// 2. Binary searches index for data block.
// 3. Reads block into a pooled buffer, verifies CRC, scans entries, and returns record.
func (t *Table) Get(key []byte) (record.Record, bool, error) {
	if len(t.index) == 0 {
		return record.Record{}, false, nil
	}

	// 1. Bloom filter exclusion
	if len(t.bloom) > 0 && !bloom.MayContain(t.bloom, key) {
		return record.Record{}, false, nil
	}

	// 2. Binary search index for block where lastKey >= key
	idx := sort.Search(len(t.index), func(i int) bool {
		return bytes.Compare(t.index[i].lastKey, key) >= 0
	})
	if idx >= len(t.index) {
		return record.Record{}, false, nil
	}

	blk := t.index[idx]

	// 3. Read block into pooled buffer
	buf := bufpool.Get(int(blk.length))
	defer buf.Release()

	if _, err := t.file.ReadAt(buf.B, int64(blk.offset)); err != nil {
		return record.Record{}, false, fmt.Errorf("sstable: read block %d: %w", idx, err)
	}

	if blk.length < 4 {
		return record.Record{}, false, errors.New("sstable: corrupt short block")
	}

	payload := buf.B[:blk.length-4]
	expectedCRC := binary.BigEndian.Uint32(buf.B[blk.length-4 : blk.length])
	actualCRC := encoding.ChecksumC(payload)
	if expectedCRC != actualCRC {
		return record.Record{}, false, fmt.Errorf("sstable: block %d crc mismatch: want %x, got %x", idx, expectedCRC, actualCRC)
	}

	// Scan entries in data block
	offset := 0
	for offset < len(payload) {
		k, val, next, err := encoding.DecodeEntry(payload, offset)
		if err != nil {
			return record.Record{}, false, fmt.Errorf("sstable: decode block entry: %w", err)
		}
		offset = next

		cmp := bytes.Compare(k, key)
		if cmp == 0 {
			rec, err := record.Decode(val)
			if err != nil {
				return record.Record{}, false, err
			}
			return rec, true, nil
		}
		if cmp > 0 {
			// Keys are sorted; stop early
			return record.Record{}, false, nil
		}
	}

	return record.Record{}, false, nil
}
