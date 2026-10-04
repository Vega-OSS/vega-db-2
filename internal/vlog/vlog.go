package vlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultSegmentSize is 1 GiB.
const DefaultSegmentSize int64 = 1024 * 1024 * 1024

// Options configures the segmented value log.
type Options struct {
	Dir         string
	SegmentSize int64
}

// Position identifies a byte offset within a specific vLog segment file.
type Position struct {
	File uint32
	Off  uint64
}

// Log represents a segmented append-only value log.
type Log struct {
	dir         string
	segmentSize int64

	// Writer state (owned by single writer goroutine)
	activeID     uint32
	activeOffset int64
	activeFile   *os.File

	// Reader cache
	readersMu sync.RWMutex
	readers   map[uint32]*os.File

	// Metrics
	BytesWritten atomic.Int64

	closed atomic.Bool
}

func segmentPath(dir string, fileID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%08d.vlog", fileID))
}

// Open opens or initializes a segmented value log in opts.Dir/vlog.
func Open(opts Options) (*Log, error) {
	vlogDir := filepath.Join(opts.Dir, "vlog")
	if err := os.MkdirAll(vlogDir, 0755); err != nil {
		return nil, fmt.Errorf("vlog: mkdir %s: %w", vlogDir, err)
	}

	segSize := opts.SegmentSize
	if segSize <= 0 {
		segSize = DefaultSegmentSize
	}

	entries, err := os.ReadDir(vlogDir)
	if err != nil {
		return nil, fmt.Errorf("vlog: readdir %s: %w", vlogDir, err)
	}

	var segIDs []uint32
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".vlog") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".vlog")
		id64, err := strconv.ParseUint(base, 10, 32)
		if err == nil && id64 > 0 {
			segIDs = append(segIDs, uint32(id64))
		}
	}

	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	var activeID uint32 = 1
	if len(segIDs) > 0 {
		activeID = segIDs[len(segIDs)-1]
	}

	activePath := segmentPath(vlogDir, activeID)
	f, err := os.OpenFile(activePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("vlog: open active segment %s: %w", activePath, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("vlog: stat active segment %s: %w", activePath, err)
	}

	l := &Log{
		dir:          vlogDir,
		segmentSize:  segSize,
		activeID:     activeID,
		activeOffset: info.Size(),
		activeFile:   f,
		readers:      make(map[uint32]*os.File),
	}

	return l, nil
}

// AppendBatch writes a contiguous block of frames to the active segment.
// If appending would exceed SegmentSize and the segment is not empty, rolls to a new segment first.
// A batch never spans two segments.
func (l *Log) AppendBatch(frames []byte) (Position, error) {
	if l.closed.Load() {
		return Position{}, errors.New("vlog: closed")
	}

	batchLen := int64(len(frames))
	if l.activeOffset > 0 && l.activeOffset+batchLen > l.segmentSize {
		if err := l.rollSegment(); err != nil {
			return Position{}, err
		}
	}

	pos := Position{
		File: l.activeID,
		Off:  uint64(l.activeOffset),
	}

	n, err := l.activeFile.Write(frames)
	if err != nil {
		return Position{}, fmt.Errorf("vlog: write segment %d: %w", l.activeID, err)
	}
	l.activeOffset += int64(n)
	l.BytesWritten.Add(int64(n))

	return pos, nil
}

func (l *Log) rollSegment() error {
	if err := l.activeFile.Sync(); err != nil {
		return fmt.Errorf("vlog: sync before roll %d: %w", l.activeID, err)
	}
	if err := l.activeFile.Close(); err != nil {
		return fmt.Errorf("vlog: close before roll %d: %w", l.activeID, err)
	}

	l.activeID++
	newPath := segmentPath(l.dir, l.activeID)
	f, err := os.OpenFile(newPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("vlog: open new segment %s: %w", newPath, err)
	}
	l.activeFile = f
	l.activeOffset = 0
	return nil
}

// Sync fsyncs the active segment file.
func (l *Log) Sync() error {
	if l.closed.Load() {
		return errors.New("vlog: closed")
	}
	return l.activeFile.Sync()
}

// Tail returns the current end position of the value log.
func (l *Log) Tail() Position {
	return Position{
		File: l.activeID,
		Off:  uint64(l.activeOffset),
	}
}

func (l *Log) getReader(fileID uint32) (*os.File, error) {
	l.readersMu.RLock()
	f, ok := l.readers[fileID]
	l.readersMu.RUnlock()
	if ok {
		return f, nil
	}

	l.readersMu.Lock()
	defer l.readersMu.Unlock()
	if f, ok := l.readers[fileID]; ok {
		return f, nil
	}

	path := segmentPath(l.dir, fileID)
	newF, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("vlog: open reader %s: %w", path, err)
	}
	l.readers[fileID] = newF
	return newF, nil
}

// ReadFrame reads exactly length bytes from segment file at off into dst.
// Safe for concurrent callers.
func (l *Log) ReadFrame(file uint32, off uint64, length uint32, dst []byte) error {
	if len(dst) < int(length) {
		return errors.New("vlog: destination slice too small for ReadFrame")
	}
	f, err := l.getReader(file)
	if err != nil {
		return err
	}

	n, err := f.ReadAt(dst[:length], int64(off))
	if err != nil && (errors.Is(err, io.EOF) || n < int(length)) {
		return fmt.Errorf("vlog: short read at %d/%d (read %d of %d): %w", file, off, n, length, err)
	}
	if err != nil {
		return fmt.Errorf("vlog: read error at %d/%d: %w", file, off, err)
	}
	return nil
}

// Replay reads sequentially from position 'from' through all segments to the tail.
// A short read or CRC mismatch in the last segment truncates the file at the last good frame.
// In earlier segments, any corruption returns an error.
func (l *Log) Replay(from Position, fn func(pos Position, frame []byte) error) error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}

	var segIDs []uint32
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".vlog") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".vlog")
		id64, err := strconv.ParseUint(base, 10, 32)
		if err == nil && id64 > 0 {
			segIDs = append(segIDs, uint32(id64))
		}
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	for segIdx, segID := range segIDs {
		if segID < from.File {
			continue
		}

		isLastSegment := segIdx == len(segIDs)-1
		path := segmentPath(l.dir, segID)
		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("vlog: replay open %s: %w", path, err)
		}

		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}

		fileSize := info.Size()
		var currOff int64
		if segID == from.File {
			currOff = int64(from.Off)
		}

		lastGoodOffset := currOff
		var replayErr error

		for currOff < fileSize {
			// Read frame header: minimum 5 bytes: 4 bytes CRC + 1 byte flag
			var hdr [5]byte
			if _, err := f.ReadAt(hdr[:], currOff); err != nil {
				if isLastSegment {
					break // Torn tail
				}
				replayErr = fmt.Errorf("vlog: read header error in segment %d at %d: %w", segID, currOff, err)
				break
			}

			// Read varints to determine full frame length
			var varintBuf [32]byte
			nVar, err := f.ReadAt(varintBuf[:], currOff+5)
			if err != nil && !errors.Is(err, io.EOF) {
				if isLastSegment {
					break
				}
				replayErr = err
				break
			}
			if nVar == 0 {
				if isLastSegment {
					break
				}
				replayErr = fmt.Errorf("vlog: missing varints in segment %d at %d", segID, currOff)
				break
			}

			kLen, n1 := binary.Uvarint(varintBuf[:nVar])
			if n1 <= 0 {
				if isLastSegment {
					break
				}
				replayErr = fmt.Errorf("vlog: corrupt key len in segment %d at %d", segID, currOff)
				break
			}

			vLen, n2 := binary.Uvarint(varintBuf[n1:nVar])
			if n2 <= 0 {
				if isLastSegment {
					break
				}
				replayErr = fmt.Errorf("vlog: corrupt val len in segment %d at %d", segID, currOff)
				break
			}

			totalFrameLen := 5 + n1 + n2 + int(kLen) + int(vLen)
			if currOff+int64(totalFrameLen) > fileSize {
				if isLastSegment {
					break // Incomplete frame at tail
				}
				replayErr = fmt.Errorf("vlog: frame extends past EOF in segment %d at %d", segID, currOff)
				break
			}

			frameBuf := make([]byte, totalFrameLen)
			if _, err := f.ReadAt(frameBuf, currOff); err != nil {
				if isLastSegment {
					break
				}
				replayErr = err
				break
			}

			// Verify frame CRC and structure
			if _, _, _, err := DecodeFrame(frameBuf); err != nil {
				if isLastSegment {
					break
				}
				replayErr = fmt.Errorf("vlog: frame decode error in segment %d at %d: %w", segID, currOff, err)
				break
			}

			framePos := Position{File: segID, Off: uint64(currOff)}
			if err := fn(framePos, frameBuf); err != nil {
				_ = f.Close()
				return err
			}

			currOff += int64(totalFrameLen)
			lastGoodOffset = currOff
		}

		if isLastSegment && lastGoodOffset < fileSize {
			// Torn tail detected: truncate file to last good frame
			if err := f.Truncate(lastGoodOffset); err != nil {
				_ = f.Close()
				return fmt.Errorf("vlog: truncate torn tail: %w", err)
			}
			_ = f.Sync()
		}

		_ = f.Close()

		if replayErr != nil {
			return replayErr
		}

		if isLastSegment {
			// Update active file to match replay state
			_ = l.activeFile.Close()
			activeF, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				return err
			}
			l.activeID = segID
			l.activeFile = activeF
			l.activeOffset = lastGoodOffset
		}
	}

	return nil
}

// Close closes all writer and reader file descriptors.
func (l *Log) Close() error {
	if l.closed.Swap(true) {
		return nil
	}

	var errs []error
	if l.activeFile != nil {
		if err := l.activeFile.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := l.activeFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	l.readersMu.Lock()
	for _, f := range l.readers {
		if err := f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	l.readers = nil
	l.readersMu.Unlock()

	return errors.Join(errs...)
}
