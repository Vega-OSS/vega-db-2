package sstable

import (
	"encoding/binary"
	"errors"

	"vegadb/internal/encoding"
)

// Table constants.
const (
	// FooterSize is the fixed size of the SSTable footer.
	FooterSize = 48

	// TableMagic identifies a valid VegaDB SSTable file ("VEGASST1").
	TableMagic uint64 = 0x5645474153535431
)

var (
	// ErrBadMagic indicates the footer magic did not match TableMagic.
	ErrBadMagic = errors.New("sstable: invalid table magic")

	// ErrCorruptFooter indicates corrupted footer checksum or length.
	ErrCorruptFooter = errors.New("sstable: corrupt footer")
)

type footer struct {
	indexOffset uint64
	indexLen    uint32
	bloomOffset uint64
	bloomLen    uint32
	entryCount  uint64
	magic       uint64
}

func encodeFooter(dst []byte, f footer) {
	if len(dst) < FooterSize {
		panic("sstable: footer slice too small")
	}

	binary.BigEndian.PutUint64(dst[0:8], f.indexOffset)
	binary.BigEndian.PutUint32(dst[8:12], f.indexLen)
	binary.BigEndian.PutUint64(dst[12:20], f.bloomOffset)
	binary.BigEndian.PutUint32(dst[20:24], f.bloomLen)
	binary.BigEndian.PutUint64(dst[24:32], f.entryCount)
	binary.BigEndian.PutUint64(dst[32:40], TableMagic)

	// Compute CRC32c over bytes 0:40
	crc := encoding.ChecksumC(dst[0:40])
	binary.BigEndian.PutUint32(dst[40:44], crc)
	// 4 bytes padding
	dst[44] = 0
	dst[45] = 0
	dst[46] = 0
	dst[47] = 0
}

func decodeFooter(src []byte) (footer, error) {
	if len(src) < FooterSize {
		return footer{}, ErrCorruptFooter
	}

	expectedCRC := binary.BigEndian.Uint32(src[40:44])
	actualCRC := encoding.ChecksumC(src[0:40])
	if expectedCRC != actualCRC {
		return footer{}, ErrCorruptFooter
	}

	magic := binary.BigEndian.Uint64(src[32:40])
	if magic != TableMagic {
		return footer{}, ErrBadMagic
	}

	return footer{
		indexOffset: binary.BigEndian.Uint64(src[0:8]),
		indexLen:    binary.BigEndian.Uint32(src[8:12]),
		bloomOffset: binary.BigEndian.Uint64(src[12:20]),
		bloomLen:    binary.BigEndian.Uint32(src[20:24]),
		entryCount:  binary.BigEndian.Uint64(src[24:32]),
		magic:       magic,
	}, nil
}
