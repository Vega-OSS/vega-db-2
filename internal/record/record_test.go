package record_test

import (
	"testing"

	"vegadb/internal/record"
)

func TestRecordRoundTrip(t *testing.T) {
	testCases := []struct {
		name string
		rec  record.Record
	}{
		{
			name: "tombstone",
			rec:  record.Record{Kind: record.KindTombstone},
		},
		{
			name: "pointer_basic",
			rec:  record.Record{Kind: record.KindPointer, File: 1, Off: 1024, Len: 64},
		},
		{
			name: "pointer_max_values",
			rec:  record.Record{Kind: record.KindPointer, File: 0xFFFFFFFF, Off: 0xFFFFFFFFFFFFFFFF, Len: 0xFFFFFFFF},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf [record.Size]byte
			tc.rec.Put(buf[:])

			decoded, err := record.Decode(buf[:])
			if err != nil {
				t.Fatalf("unexpected decode error: %v", err)
			}
			if decoded != tc.rec {
				t.Fatalf("record mismatch: got %+v, want %+v", decoded, tc.rec)
			}
		})
	}
}

func TestRecordDecodeErrors(t *testing.T) {
	// Too short
	short := make([]byte, 16)
	if _, err := record.Decode(short); err == nil {
		t.Fatal("expected error for short buffer, got nil")
	}

	// Invalid kind
	var invalidKind [record.Size]byte
	invalidKind[0] = 2
	if _, err := record.Decode(invalidKind[:]); err == nil {
		t.Fatal("expected error for invalid kind, got nil")
	}
}
