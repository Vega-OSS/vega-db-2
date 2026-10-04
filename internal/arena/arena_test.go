package arena_test

import (
	"bytes"
	"testing"

	"github.com/Vega-OSS/vega-db/internal/arena"
)

func TestArenaAlloc(t *testing.T) {
	a := arena.New(1024)

	// Small allocs
	b1 := a.Alloc(100)
	copy(b1, []byte("hello"))
	b2 := a.Alloc(200)
	copy(b2, []byte("world"))

	if !bytes.Equal(b1[:5], []byte("hello")) {
		t.Fatal("b1 corrupted")
	}
	if !bytes.Equal(b2[:5], []byte("world")) {
		t.Fatal("b2 corrupted")
	}

	// Large alloc (> chunkSize/4 = 256)
	b3 := a.Alloc(500)
	copy(b3, []byte("dedicated"))
	if !bytes.Equal(b3[:9], []byte("dedicated")) {
		t.Fatal("b3 corrupted")
	}

	if a.Size() < 1024+500 {
		t.Fatalf("expected arena size >= 1524, got %d", a.Size())
	}
}
