package vlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"vegadb/internal/encoding"
)

// Flag constants for vLog frames.
const (
	FlagPut    byte = 0
	FlagDelete byte = 1
)

var (
	// ErrCRC indicates a frame failed CRC32c verification.
	ErrCRC = errors.New("vlog: crc mismatch")

	// ErrCorruptFrame indicates frame header or payload length is invalid.
	ErrCorruptFrame = errors.New("vlog: corrupt frame")

	// ErrFrameTooSmall indicates frame buffer is smaller than minimum header size.
	ErrFrameTooSmall = errors.New("vlog: frame too small")
)

// FrameSize computes the exact encoded byte length for a frame given key and value lengths.
func FrameSize(keyLen, valueLen int) int {
	var buf [binary.MaxVarintLen64]byte
	kVar := binary.PutUvarint(buf[:], uint64(keyLen))
	vVar := binary.PutUvarint(buf[:], uint64(valueLen))
	return 4 + 1 + kVar + vVar + keyLen + valueLen
}

// EncodeFrame writes an encoded vLog frame into dst and returns the total bytes written.
// dst must have at least FrameSize(len(key), len(value)) capacity.
func EncodeFrame(dst []byte, flags byte, key, value []byte) int {
	needed := FrameSize(len(key), len(value))
	if len(dst) < needed {
		panic("vlog: destination buffer too small for frame")
	}

	// [crc32c 4B BE][flags 1B][klen uvarint][vlen uvarint][key][val]
	dst[4] = flags
	idx := 5

	var vbuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(vbuf[:], uint64(len(key)))
	copy(dst[idx:], vbuf[:n])
	idx += n

	n = binary.PutUvarint(vbuf[:], uint64(len(value)))
	copy(dst[idx:], vbuf[:n])
	idx += n

	copy(dst[idx:], key)
	idx += len(key)

	copy(dst[idx:], value)
	idx += len(value)

	// Compute CRC32c over everything after the 4-byte CRC field
	crc := encoding.ChecksumC(dst[4:idx])
	binary.BigEndian.PutUint32(dst[0:4], crc)

	return idx
}

// DecodeFrame decodes and verifies a vLog frame.
// The returned key and value are sub-slices of frame and are valid until frame is released or modified.
func DecodeFrame(frame []byte) (flags byte, key, value []byte, err error) {
	if len(frame) < 5 {
		return 0, nil, nil, ErrFrameTooSmall
	}

	expectedCRC := binary.BigEndian.Uint32(frame[0:4])
	actualCRC := encoding.ChecksumC(frame[4:])
	if expectedCRC != actualCRC {
		return 0, nil, nil, fmt.Errorf("%w: expected %x, got %x", ErrCRC, expectedCRC, actualCRC)
	}

	flags = frame[4]
	idx := 5

	kLen, n := binary.Uvarint(frame[idx:])
	if n <= 0 {
		return 0, nil, nil, ErrCorruptFrame
	}
	idx += n

	vLen, n := binary.Uvarint(frame[idx:])
	if n <= 0 {
		return 0, nil, nil, ErrCorruptFrame
	}
	idx += n

	if idx > len(frame) {
		return 0, nil, nil, ErrCorruptFrame
	}

	rem := len(frame) - idx
	if rem < 0 || kLen > math.MaxInt || int(kLen) > rem {
		return 0, nil, nil, ErrCorruptFrame
	}
	keyLen := int(kLen)
	keyStart := idx
	idx += keyLen
	rem -= keyLen

	if rem < 0 || vLen > math.MaxInt || int(vLen) > rem {
		return 0, nil, nil, ErrCorruptFrame
	}
	valLen := int(vLen)
	valStart := idx
	idx += valLen

	if idx != len(frame) {
		return 0, nil, nil, ErrCorruptFrame
	}

	return flags, frame[keyStart:valStart], frame[valStart:idx], nil
}
