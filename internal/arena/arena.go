package arena

// DefaultChunkSize is the default chunk allocation size (1 MiB).
const DefaultChunkSize = 1024 * 1024

// Arena is a monotonic bump allocator.
// Not safe for concurrent use by itself; caller must synchronize.
type Arena struct {
	chunkSize int
	chunks    [][]byte
	curr      []byte
	offset    int
	totalSize int
}

// New creates a new Arena with the specified chunk size.
func New(chunkSize int) *Arena {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Arena{
		chunkSize: chunkSize,
	}
}

// Alloc bump-allocates n bytes.
// Requests larger than chunkSize/4 are allocated into their own dedicated chunk.
func (a *Arena) Alloc(n int) []byte {
	if n <= 0 {
		return nil
	}

	if n > a.chunkSize/4 {
		chunk := make([]byte, n)
		a.chunks = append(a.chunks, chunk)
		a.totalSize += n
		return chunk
	}

	if a.curr == nil || len(a.curr)-a.offset < n {
		a.curr = make([]byte, a.chunkSize)
		a.chunks = append(a.chunks, a.curr)
		a.totalSize += a.chunkSize
		a.offset = 0
	}

	buf := a.curr[a.offset : a.offset+n]
	a.offset += n
	return buf
}

// Size returns total bytes reserved by all chunks in the arena.
func (a *Arena) Size() int {
	return a.totalSize
}
