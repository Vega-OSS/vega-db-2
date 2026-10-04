package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"vegadb/internal/bloom"
	"vegadb/internal/encoding"
	"vegadb/internal/record"
)

// DefaultBlockSize is 4 KiB.
const DefaultBlockSize = 4096

// Meta describes summary metadata for a built SSTable.
type Meta struct {
	Smallest []byte
	Largest  []byte
	Size     int64
	Count    uint64
}

type blockIndexEntry struct {
	lastKey []byte
	offset  uint64
	length  uint32
}

// Builder constructs an SSTable file on disk.
type Builder struct {
	path       string
	file       *os.File
	blockSize  int
	bitsPerKey int
	fileOffset uint64
	blockBuf   []byte
	blockKeys  [][]byte
	allKeys    [][]byte
	index      []blockIndexEntry
	smallest   []byte
	largest    []byte
	count      uint64
	closed     bool
}

// NewBuilder creates a new SSTable Builder writing to path.
func NewBuilder(path string, blockSize int, bitsPerKey int) (*Builder, error) {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}

	return &Builder{
		path:       path,
		file:       f,
		blockSize:  blockSize,
		bitsPerKey: bitsPerKey,
	}, nil
}

// Add adds a key and record to the SSTable.
// Keys must be strictly increasing.
func (b *Builder) Add(key []byte, rec record.Record) error {
	if b.closed {
		return errors.New("sstable: builder closed")
	}

	if len(b.largest) > 0 && bytes.Compare(key, b.largest) <= 0 {
		return errors.New("sstable: keys must be strictly increasing")
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	if len(b.smallest) == 0 {
		b.smallest = keyCopy
	}
	b.largest = keyCopy

	var recBuf [record.Size]byte
	rec.Put(recBuf[:])

	b.blockBuf = encoding.EncodeEntry(b.blockBuf, keyCopy, recBuf[:])
	b.blockKeys = append(b.blockKeys, keyCopy)
	b.allKeys = append(b.allKeys, keyCopy)
	b.count++

	if len(b.blockBuf) >= b.blockSize {
		if err := b.flushDataBlock(); err != nil {
			return err
		}
	}

	return nil
}

func (b *Builder) flushDataBlock() error {
	if len(b.blockBuf) == 0 {
		return nil
	}

	crc := encoding.ChecksumC(b.blockBuf)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)

	blockLen := uint32(len(b.blockBuf) + 4)
	if _, err := b.file.Write(b.blockBuf); err != nil {
		return fmt.Errorf("sstable: write data block: %w", err)
	}
	if _, err := b.file.Write(crcBuf[:]); err != nil {
		return fmt.Errorf("sstable: write data block crc: %w", err)
	}

	lastKey := b.blockKeys[len(b.blockKeys)-1]
	b.index = append(b.index, blockIndexEntry{
		lastKey: lastKey,
		offset:  b.fileOffset,
		length:  blockLen,
	})

	b.fileOffset += uint64(blockLen)
	b.blockBuf = b.blockBuf[:0]
	b.blockKeys = b.blockKeys[:0]
	return nil
}

// Finish flushes remaining data, writes index, bloom filter, and footer, and fsyncs the file.
func (b *Builder) Finish() (Meta, error) {
	if b.closed {
		return Meta{}, errors.New("sstable: builder closed")
	}
	b.closed = true

	// Flush any pending data in blockBuf
	if err := b.flushDataBlock(); err != nil {
		_ = b.file.Close()
		return Meta{}, err
	}

	// Write index block
	indexOffset := b.fileOffset
	var indexBuf []byte
	var entryBuf [12]byte
	for _, entry := range b.index {
		binary.BigEndian.PutUint64(entryBuf[0:8], entry.offset)
		binary.BigEndian.PutUint32(entryBuf[8:12], entry.length)
		indexBuf = encoding.EncodeEntry(indexBuf, entry.lastKey, entryBuf[:])
	}

	indexLen := uint32(len(indexBuf))
	if _, err := b.file.Write(indexBuf); err != nil {
		_ = b.file.Close()
		return Meta{}, fmt.Errorf("sstable: write index: %w", err)
	}
	b.fileOffset += uint64(indexLen)

	// Write bloom filter block
	bloomOffset := b.fileOffset
	var bloomBuf []byte
	if len(b.allKeys) > 0 {
		bloomBuf = bloom.Build(b.allKeys, b.bitsPerKey)
	}
	bloomLen := uint32(len(bloomBuf))
	if bloomLen > 0 {
		if _, err := b.file.Write(bloomBuf); err != nil {
			_ = b.file.Close()
			return Meta{}, fmt.Errorf("sstable: write bloom: %w", err)
		}
		b.fileOffset += uint64(bloomLen)
	}

	// Write 48-byte fixed footer
	var foot footer
	foot.indexOffset = indexOffset
	foot.indexLen = indexLen
	foot.bloomOffset = bloomOffset
	foot.bloomLen = bloomLen
	foot.entryCount = b.count
	foot.magic = TableMagic

	var footBuf [FooterSize]byte
	encodeFooter(footBuf[:], foot)
	if _, err := b.file.Write(footBuf[:]); err != nil {
		_ = b.file.Close()
		return Meta{}, fmt.Errorf("sstable: write footer: %w", err)
	}
	b.fileOffset += FooterSize

	if err := b.file.Sync(); err != nil {
		_ = b.file.Close()
		return Meta{}, fmt.Errorf("sstable: fsync: %w", err)
	}
	if err := b.file.Close(); err != nil {
		return Meta{}, fmt.Errorf("sstable: close: %w", err)
	}

	return Meta{
		Smallest: b.smallest,
		Largest:  b.largest,
		Size:     int64(b.fileOffset),
		Count:    b.count,
	}, nil
}
