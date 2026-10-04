package vlog_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Vega-OSS/vega-db/internal/vlog"
)

func TestFrameEncodeDecode(t *testing.T) {
	testCases := []struct {
		name  string
		flags byte
		key   []byte
		value []byte
	}{
		{"empty", vlog.FlagPut, []byte{}, []byte{}},
		{"simple", vlog.FlagPut, []byte("key1"), []byte("val1")},
		{"delete_flag", vlog.FlagDelete, []byte("deleted_key"), []byte{}},
		{"binary_data", vlog.FlagPut, []byte{0x00, 0xff, 0xca, 0xfe}, []byte{0xde, 0xad, 0xbe, 0xef}},
		{"large_value_1mb", vlog.FlagPut, []byte("huge"), bytes.Repeat([]byte("v"), 1024*1024)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			needed := vlog.FrameSize(len(tc.key), len(tc.value))
			buf := make([]byte, needed)
			n := vlog.EncodeFrame(buf, tc.flags, tc.key, tc.value)
			if n != needed {
				t.Fatalf("expected written %d, got %d", needed, n)
			}

			flags, k, v, err := vlog.DecodeFrame(buf[:n])
			if err != nil {
				t.Fatalf("unexpected decode error: %v", err)
			}
			if flags != tc.flags {
				t.Fatalf("flags mismatch: got %d, want %d", flags, tc.flags)
			}
			if !bytes.Equal(k, tc.key) {
				t.Fatalf("key mismatch")
			}
			if !bytes.Equal(v, tc.value) {
				t.Fatalf("value mismatch")
			}
		})
	}
}

func TestVlogSegmentRollAndRead(t *testing.T) {
	dir := t.TempDir()
	opts := vlog.Options{
		Dir:         dir,
		SegmentSize: 64 * 1024, // 64 KiB tiny segments
	}

	lg, err := vlog.Open(opts)
	if err != nil {
		t.Fatalf("open error: %v", err)
	}
	defer lg.Close()

	type item struct {
		pos vlog.Position
		len uint32
		k   []byte
		v   []byte
	}

	const count = 5000 // test 5k entries across tiny segments
	items := make([]item, count)

	for i := range count {
		k := []byte(fmt.Sprintf("key_%06d", i))
		v := []byte(fmt.Sprintf("value_data_%06d", i))
		sz := vlog.FrameSize(len(k), len(v))
		frame := make([]byte, sz)
		vlog.EncodeFrame(frame, vlog.FlagPut, k, v)

		pos, err := lg.AppendBatch(frame)
		if err != nil {
			t.Fatalf("append error: %v", err)
		}
		items[i] = item{pos: pos, len: uint32(sz), k: k, v: v}
	}

	// Verify tail advanced across segments
	tail := lg.Tail()
	if tail.File <= 1 {
		t.Fatalf("expected segments to roll, got tail file %d", tail.File)
	}

	// Read each back by pointer
	for i, it := range items {
		buf := make([]byte, it.len)
		if err := lg.ReadFrame(it.pos.File, it.pos.Off, it.len, buf); err != nil {
			t.Fatalf("read frame %d error: %v", i, err)
		}
		_, k, v, err := vlog.DecodeFrame(buf)
		if err != nil {
			t.Fatalf("decode frame %d error: %v", i, err)
		}
		if !bytes.Equal(k, it.k) || !bytes.Equal(v, it.v) {
			t.Fatalf("frame %d data mismatch", i)
		}
	}
}

func TestVlogReplayAndTornTailTruncation(t *testing.T) {
	dir := t.TempDir()
	opts := vlog.Options{
		Dir:         dir,
		SegmentSize: 32 * 1024,
	}

	lg, err := vlog.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const count = 200
	for i := range count {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte(fmt.Sprintf("v%d", i))
		buf := make([]byte, vlog.FrameSize(len(k), len(v)))
		vlog.EncodeFrame(buf, vlog.FlagPut, k, v)
		if _, err := lg.AppendBatch(buf); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	lastTail := lg.Tail()
	_ = lg.Close()

	// Corrupt the end of the last segment by appending 10 garbage bytes (torn write)
	lastSegPath := filepath.Join(dir, "vlog", fmt.Sprintf("%08d.vlog", lastTail.File))
	f, err := os.OpenFile(lastSegPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("torn_trash"))
	_ = f.Close()

	// Reopen and replay
	lg2, err := vlog.Open(opts)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	defer lg2.Close()

	replayed := 0
	err = lg2.Replay(vlog.Position{File: 1, Off: 0}, func(pos vlog.Position, frame []byte) error {
		replayed++
		return nil
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if replayed != count {
		t.Fatalf("expected %d replayed frames, got %d", count, replayed)
	}

	// Verify file was truncated to exact lastGoodOffset
	info, err := os.Stat(lastSegPath)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(info.Size()) != lastTail.Off {
		t.Fatalf("expected truncated size %d, got %d", lastTail.Off, info.Size())
	}
}

func TestVlogCRCFailure(t *testing.T) {
	dir := t.TempDir()
	lg, err := vlog.Open(vlog.Options{Dir: dir, SegmentSize: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()

	k := []byte("key")
	v := []byte("val")
	sz := vlog.FrameSize(len(k), len(v))
	buf := make([]byte, sz)
	vlog.EncodeFrame(buf, vlog.FlagPut, k, v)
	pos, _ := lg.AppendBatch(buf)

	// Read frame
	readBuf := make([]byte, sz)
	if err := lg.ReadFrame(pos.File, pos.Off, uint32(sz), readBuf); err != nil {
		t.Fatal(err)
	}

	// Flip a byte
	readBuf[sz-1] ^= 0xFF
	if _, _, _, err := vlog.DecodeFrame(readBuf); err == nil {
		t.Fatal("expected CRC error on corrupted frame, got nil")
	}
}

func TestVlogConcurrentReadersAndWriter(t *testing.T) {
	dir := t.TempDir()
	lg, err := vlog.Open(vlog.Options{Dir: dir, SegmentSize: 64 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()

	var positions []vlog.Position
	var lengths []uint32
	var mu sync.Mutex

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 16 readers
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					mu.Lock()
					n := len(positions)
					if n == 0 {
						mu.Unlock()
						continue
					}
					idx := rand.Intn(n)
					p := positions[idx]
					l := lengths[idx]
					mu.Unlock()

					buf := make([]byte, l)
					if err := lg.ReadFrame(p.File, p.Off, l, buf); err == nil {
						_, _, _, _ = vlog.DecodeFrame(buf)
					}
				}
			}
		}()
	}

	// Single writer
	for i := range 1000 {
		k := []byte(fmt.Sprintf("key_%04d", i))
		v := []byte(fmt.Sprintf("val_%04d", i))
		sz := vlog.FrameSize(len(k), len(v))
		buf := make([]byte, sz)
		vlog.EncodeFrame(buf, vlog.FlagPut, k, v)
		pos, err := lg.AppendBatch(buf)
		if err != nil {
			t.Fatalf("append error: %v", err)
		}
		mu.Lock()
		positions = append(positions, pos)
		lengths = append(lengths, uint32(sz))
		mu.Unlock()
	}

	close(stop)
	wg.Wait()
}
