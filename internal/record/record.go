package record

import (
	"encoding/binary"
	"errors"
)

// Record constants defining sizes and kinds.
const (
	// Size is the fixed byte length of an encoded Record.
	Size = 17

	// KindTombstone represents a deleted key.
	KindTombstone uint8 = 0

	// KindPointer represents a pointer to a vLog frame.
	KindPointer uint8 = 1
)

// ErrInvalidRecord indicates that the byte slice cannot be decoded into a valid Record.
var ErrInvalidRecord = errors.New("record: invalid record format")

// Record represents a pointer to a vLog frame or a tombstone stored in memtable nodes and SSTables.
type Record struct {
	Kind uint8
	File uint32
	Off  uint64
	Len  uint32
}

// Put writes the 17-byte binary representation of the Record into dst.
// dst must have length of at least Size (17 bytes).
func (r Record) Put(dst []byte) {
	if len(dst) < Size {
		panic("record: destination slice too small")
	}
	dst[0] = r.Kind
	if r.Kind == KindTombstone {
		// All other fields must be zero for tombstones
		for i := 1; i < Size; i++ {
			dst[i] = 0
		}
		return
	}
	binary.BigEndian.PutUint32(dst[1:5], r.File)
	binary.BigEndian.PutUint64(dst[5:13], r.Off)
	binary.BigEndian.PutUint32(dst[13:17], r.Len)
}

// Decode parses a Record from a 17-byte slice.
func Decode(src []byte) (Record, error) {
	if len(src) < Size {
		return Record{}, ErrInvalidRecord
	}
	kind := src[0]
	if kind != KindTombstone && kind != KindPointer {
		return Record{}, ErrInvalidRecord
	}
	if kind == KindTombstone {
		return Record{Kind: KindTombstone}, nil
	}
	return Record{
		Kind: kind,
		File: binary.BigEndian.Uint32(src[1:5]),
		Off:  binary.BigEndian.Uint64(src[5:13]),
		Len:  binary.BigEndian.Uint32(src[13:17]),
	}, nil
}
