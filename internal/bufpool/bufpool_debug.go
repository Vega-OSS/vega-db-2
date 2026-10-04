//go:build bufdebug

package bufpool

func (b *Buf) resetDebug() {
	b.released = false
}

// Release returns the buffer to its pool, poisoning the byte slice with 0xDE and guarding against double release.
func (b *Buf) Release() {
	if b == nil {
		return
	}
	if b.released {
		panic("bufpool: double release detected")
	}
	b.released = true

	// Poison memory to expose use-after-release bugs
	c := cap(b.B)
	poisoned := b.B[:c]
	for i := range poisoned {
		poisoned[i] = 0xDE
	}

	if b.class < 0 {
		return
	}
	b.B = b.B[:0]
	pools[b.class].Put(b)
}
