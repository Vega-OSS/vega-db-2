package bufpool

import "sync"

// Size class limits: 512 B, 4 KiB, 64 KiB, 1 MiB, 16 MiB.
var sizeClasses = [...]int{
	512,
	4 * 1024,
	64 * 1024,
	1024 * 1024,
	16 * 1024 * 1024,
}

var pools = [...]sync.Pool{
	{New: func() any { return &Buf{B: make([]byte, 0, sizeClasses[0]), class: 0} }},
	{New: func() any { return &Buf{B: make([]byte, 0, sizeClasses[1]), class: 1} }},
	{New: func() any { return &Buf{B: make([]byte, 0, sizeClasses[2]), class: 2} }},
	{New: func() any { return &Buf{B: make([]byte, 0, sizeClasses[3]), class: 3} }},
	{New: func() any { return &Buf{B: make([]byte, 0, sizeClasses[4]), class: 4} }},
}

// Buf is a pooled byte buffer. B has len = requested size and cap = class size.
type Buf struct {
	B        []byte
	class    int8 // -1 means "not pooled" (larger than the largest class)
	released bool
}

// Get returns a pooled byte buffer of length n.
// Contents of the buffer are undefined.
func Get(n int) *Buf {
	if n <= 0 {
		return &Buf{B: nil, class: -1}
	}

	classIdx := -1
	for i, sz := range sizeClasses {
		if sz >= n {
			classIdx = i
			break
		}
	}

	if classIdx == -1 {
		// Larger than largest pool size (16 MiB)
		return &Buf{
			B:     make([]byte, n),
			class: -1,
		}
	}

	buf := pools[classIdx].Get().(*Buf)
	buf.B = buf.B[:n]
	buf.resetDebug()
	return buf
}
