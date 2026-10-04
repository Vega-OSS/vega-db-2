package encoding_test

import (
	"testing"

	"vegadb/internal/encoding"
)

func TestBinaryUvarint(t *testing.T) {
	testVals := []uint64{0, 1, 127, 128, 255, 16383, 16384, 1<<32 - 1, 1<<64 - 1}

	var buf [16]byte
	for _, v := range testVals {
		n := encoding.PutUvarint(buf[:], v)
		sz := encoding.UvarintSize(v)
		if n != sz {
			t.Fatalf("UvarintSize mismatch for %d: got %d, want %d", v, sz, n)
		}

		decoded, readN, err := encoding.Uvarint(buf[:n])
		if err != nil {
			t.Fatalf("unexpected error for %d: %v", v, err)
		}
		if readN != n || decoded != v {
			t.Fatalf("decoded %d != original %d (read %d, wrote %d)", decoded, v, readN, n)
		}
	}
}

func TestBinaryFixedIntegers(t *testing.T) {
	var buf32 [4]byte
	encoding.PutUint32(buf32[:], 0x12345678)
	val32, err := encoding.Uint32(buf32[:])
	if err != nil || val32 != 0x12345678 {
		t.Fatalf("uint32 roundtrip failed: %v, got %x", err, val32)
	}

	var buf64 [8]byte
	encoding.PutUint64(buf64[:], 0x123456789abcdef0)
	val64, err := encoding.Uint64(buf64[:])
	if err != nil || val64 != 0x123456789abcdef0 {
		t.Fatalf("uint64 roundtrip failed: %v, got %x", err, val64)
	}
}

func TestBinaryChecksum(t *testing.T) {
	data := []byte("vega-db-test-data")
	crc := encoding.Checksum(data)
	if err := encoding.VerifyChecksum(data, crc); err != nil {
		t.Fatalf("checksum verify failed: %v", err)
	}
	if err := encoding.VerifyChecksum(data, crc^0x1); err == nil {
		t.Fatal("expected error on mismatched checksum")
	}
}
