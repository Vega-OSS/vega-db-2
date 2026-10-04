package encoding_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/Vega-OSS/vega-db/internal/encoding"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	testCases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{"empty", []byte{}, []byte{}},
		{"simple", []byte("user:100"), []byte("alice")},
		{"binary", []byte{0x00, 0xff, 0xfe}, []byte{0xde, 0xad, 0xbe, 0xef}},
		{"long_key_val", bytes.Repeat([]byte("k"), 1024), bytes.Repeat([]byte("v"), 4096)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := encoding.EncodeEntry(nil, tc.key, tc.value)
			k, v, next, err := encoding.DecodeEntry(buf, 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if next != len(buf) {
				t.Fatalf("expected next %d, got %d", len(buf), next)
			}
			if !bytes.Equal(k, tc.key) {
				t.Fatalf("key mismatch: got %v, want %v", k, tc.key)
			}
			if !bytes.Equal(v, tc.value) {
				t.Fatalf("value mismatch: got %v, want %v", v, tc.value)
			}
		})
	}
}

func TestDecodeEntryHardenedLengths(t *testing.T) {
	lengths := []uint64{
		1 << 63,
		math.MaxUint64,
		math.MaxInt64 + 1,
		1 << 62,
	}

	for _, l := range lengths {
		var buf [binary.MaxVarintLen64 * 2]byte
		n := binary.PutUvarint(buf[:], l)
		binary.PutUvarint(buf[n:], 10)

		_, _, _, err := encoding.DecodeEntry(buf[:], 0)
		if err == nil {
			t.Fatalf("expected error for extreme length %d, got nil", l)
		}
	}
}

func FuzzDecodeEntry(f *testing.F) {
	f.Add([]byte{})
	f.Add(encoding.EncodeEntry(nil, []byte("key"), []byte("val")))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})

	f.Fuzz(func(_ *testing.T, data []byte) {
		// Must never panic
		_, _, _, _ = encoding.DecodeEntry(data, 0)
	})
}
