//go:build !bufdebug

package bufpool

func (b *Buf) resetDebug() {
	b.released = false
}

// Release returns the buffer to its size-class pool if it was pooled.
// Releasing twice is a bug.
func (b *Buf) Release() {
	if b == nil || b.class < 0 {
		return
	}
	b.B = b.B[:0]
	pools[b.class].Put(b)
}
