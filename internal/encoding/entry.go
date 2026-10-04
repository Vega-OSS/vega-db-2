package encoding

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	// ErrCorrupt indicates that an encoded entry has invalid or truncated data.
	ErrCorrupt = errors.New("encoding: corrupt entry")

	// ErrShort indicates that the buffer is too small to contain the entry header.
	ErrShort = errors.New("encoding: buffer too short")
)

// EncodedEntrySize returns the byte size required to encode an entry with keyLen and valueLen.
func EncodedEntrySize(keyLen, valueLen int) int {
	return UvarintSize(uint64(keyLen)) + UvarintSize(uint64(valueLen)) + keyLen + valueLen
}

// EncodeEntryTo serializes key and value into dst and returns bytes written.
func EncodeEntryTo(dst, key, value []byte) int {
	offset := 0
	offset += PutUvarint(dst[offset:], uint64(len(key)))
	offset += PutUvarint(dst[offset:], uint64(len(value)))
	copy(dst[offset:], key)
	offset += len(key)
	copy(dst[offset:], value)
	offset += len(value)
	return offset
}

// EncodeEntry appends an entry formatted as [key len uvarint][val len uvarint][key][val] to dst.
func EncodeEntry(dst, key, value []byte) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(key)))
	dst = append(dst, buf[:n]...)
	n = binary.PutUvarint(buf[:], uint64(len(value)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, key...)
	dst = append(dst, value...)
	return dst
}

// DecodeEntry parses an entry from data at the given offset.
// The returned key and value are sub-slices of data and are valid until data is reused or released.
func DecodeEntry(data []byte, offset int) (key, value []byte, nextOffset int, err error) {
	if offset < 0 || offset > len(data) {
		return nil, nil, 0, ErrShort
	}
	kLen, n := binary.Uvarint(data[offset:])
	if n <= 0 {
		return nil, nil, 0, ErrCorrupt
	}
	offset += n

	vLen, n := binary.Uvarint(data[offset:])
	if n <= 0 {
		return nil, nil, 0, ErrCorrupt
	}
	offset += n

	if offset > len(data) {
		return nil, nil, 0, ErrCorrupt
	}
	rem := len(data) - offset
	if rem < 0 || kLen > math.MaxInt || int(kLen) > rem {
		return nil, nil, 0, ErrCorrupt
	}
	keyLen := int(kLen)
	keyStart := offset
	offset += keyLen
	rem -= keyLen

	if rem < 0 || vLen > math.MaxInt || int(vLen) > rem {
		return nil, nil, 0, ErrCorrupt
	}
	valLen := int(vLen)
	valStart := offset
	nextOffset = offset + valLen

	key = data[keyStart:valStart]
	value = data[valStart:nextOffset]
	return key, value, nextOffset, nil
}
