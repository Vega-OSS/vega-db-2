package encoding

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	// ErrBufferTooSmall is returned when a buffer does not contain enough bytes to decode a field.
	ErrBufferTooSmall = errors.New("encoding: buffer too small")

	// ErrVarintOverflow is returned when a varint value overflows uint64 (more than 10 bytes).
	ErrVarintOverflow = errors.New("encoding: varint overflows 64-bit integer")

	// ErrChecksumMismatch is returned when data CRC32 checksum does not match expected checksum.
	ErrChecksumMismatch = errors.New("encoding: checksum mismatch (corrupted data)")
)

// PutUvarint encodes a uint64 into buf using variable-length encoding (varint)
// and returns the number of bytes written.
func PutUvarint(buf []byte, v uint64) int {
	return binary.PutUvarint(buf, v)
}

// Uvarint decodes a uint64 from buf using variable-length encoding.
// It returns the decoded value, the number of bytes read, and an error
// if the buffer is empty or the varint is malformed/truncated.
func Uvarint(buf []byte) (uint64, int, error) {
	if len(buf) == 0 {
		return 0, 0, ErrBufferTooSmall
	}
	val, n := binary.Uvarint(buf)
	if n == 0 {
		return 0, 0, ErrBufferTooSmall
	}
	if n < 0 {
		return 0, 0, ErrVarintOverflow
	}
	return val, n, nil
}

// UvarintSize returns the number of bytes needed to encode v as a uvarint.
func UvarintSize(v uint64) int {
	i := 0
	for v >= 0x80 {
		v >>= 7
		i++
	}
	return i + 1
}

// PutUint32 encodes a uint32 into buf using big-endian byte order.
func PutUint32(buf []byte, v uint32) {
	binary.BigEndian.PutUint32(buf, v)
}

// PutUint64 encodes a uint64 into buf using big-endian byte order.
func PutUint64(buf []byte, v uint64) {
	binary.BigEndian.PutUint64(buf, v)
}

// Uint32 decodes a uint32 from buf using big-endian byte order.
func Uint32(buf []byte) (uint32, error) {
	if len(buf) < 4 {
		return 0, ErrBufferTooSmall
	}
	return binary.BigEndian.Uint32(buf), nil
}

// Uint64 decodes a uint64 from buf using big-endian byte order.
func Uint64(buf []byte) (uint64, error) {
	if len(buf) < 8 {
		return 0, ErrBufferTooSmall
	}
	return binary.BigEndian.Uint64(buf), nil
}

// Checksum calculates the CRC32 checksum of data using the IEEE polynomial.
func Checksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// VerifyChecksum verifies that the CRC32 checksum of data matches expected.
func VerifyChecksum(data []byte, expected uint32) error {
	if Checksum(data) != expected {
		return ErrChecksumMismatch
	}
	return nil
}
