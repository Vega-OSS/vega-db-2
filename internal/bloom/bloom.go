package bloom

import (
	"github.com/cespare/xxhash/v2"
)

// Build constructs a serialized Bloom filter for the given list of keys.
// Serialization format: bit array followed by 1 byte storing k (the probe count).
func Build(keys [][]byte, bitsPerKey int) []byte {
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}

	numBits := len(keys) * bitsPerKey
	if numBits < 64 {
		numBits = 64
	}

	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8

	k := int(float64(bitsPerKey) * 0.69314718056) // ~ ln(2)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}

	// Allocate bit array + 1 byte for k
	filter := make([]byte, numBytes+1)
	filter[numBytes] = byte(k)

	bitCount := uint32(numBits)
	for _, key := range keys {
		h := xxhash.Sum64(key)
		h1 := uint32(h)
		h2 := uint32(h >> 32)
		for i := 0; i < k; i++ {
			bit := (h1 + uint32(i)*h2) % bitCount
			filter[bit/8] |= (1 << (bit % 8))
		}
	}

	return filter
}

// MayContain checks if key could be in the set represented by filter.
// Returns false if key is definitely absent, or true if it might be present.
func MayContain(filter, key []byte) bool {
	if len(filter) < 2 {
		return true
	}

	numBytes := len(filter) - 1
	k := int(filter[numBytes])
	if k > 30 || k < 1 {
		return true
	}

	bitCount := uint32(numBytes * 8)
	h := xxhash.Sum64(key)
	h1 := uint32(h)
	h2 := uint32(h >> 32)

	for i := 0; i < k; i++ {
		bit := (h1 + uint32(i)*h2) % bitCount
		if (filter[bit/8] & (1 << (bit % 8))) == 0 {
			return false
		}
	}

	return true
}
