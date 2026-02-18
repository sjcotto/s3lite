// Package bloom implements a space-efficient Bloom filter for s3lite.
// Bloom filters are stored per-chunk in the manifest to allow
// skipping chunks that definitely don't contain a given key.
package bloom

import (
	"encoding/base64"
	"encoding/binary"
	"math"
)

const (
	// DefaultFPRate is the target false positive rate.
	DefaultFPRate = 0.01
)

// Filter is a Bloom filter.
type Filter struct {
	bits    []byte
	numBits uint64
	numHash int
}

// New creates a new Bloom filter sized for n expected items.
func New(n int) *Filter {
	return NewWithFPRate(n, DefaultFPRate)
}

// NewWithFPRate creates a Bloom filter with a custom false positive rate.
func NewWithFPRate(n int, fpRate float64) *Filter {
	if n <= 0 {
		n = 1
	}
	// Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
	m := int(math.Ceil(-float64(n) * math.Log(fpRate) / (math.Log(2) * math.Log(2))))
	// Round up to nearest byte
	m = ((m + 7) / 8) * 8
	if m < 64 {
		m = 64
	}
	// Optimal number of hash functions: k = (m/n) * ln(2)
	k := int(math.Ceil(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	return &Filter{
		bits:    make([]byte, m/8),
		numBits: uint64(m),
		numHash: k,
	}
}

// Add inserts a key into the filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := hash(key)
	for i := 0; i < f.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % f.numBits
		f.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain returns true if the key might be in the set.
// Returns false if the key is definitely not in the set.
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := hash(key)
	for i := 0; i < f.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % f.numBits
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the bloom filter to a base64 string for storage in manifests.
func (f *Filter) Encode() string {
	// Format: [numHash(1 byte)][numBits(8 bytes)][bits...]
	buf := make([]byte, 1+8+len(f.bits))
	buf[0] = byte(f.numHash)
	binary.LittleEndian.PutUint64(buf[1:9], f.numBits)
	copy(buf[9:], f.bits)
	return base64.StdEncoding.EncodeToString(buf)
}

// Decode deserializes a bloom filter from a base64 string.
func Decode(s string) (*Filter, error) {
	buf, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	f := &Filter{
		numHash: int(buf[0]),
		numBits: binary.LittleEndian.Uint64(buf[1:9]),
		bits:    make([]byte, len(buf)-9),
	}
	copy(f.bits, buf[9:])
	return f, nil
}

// hash produces two independent 64-bit hashes using a variant of
// Murmur-inspired mixing. We use double hashing: h(i) = h1 + i*h2.
func hash(key []byte) (uint64, uint64) {
	// FNV-1a inspired, but split into two halves
	var h1, h2 uint64
	h1 = 14695981039346656037 // FNV offset basis
	h2 = 17316873913966636673 // different seed

	for _, b := range key {
		h1 ^= uint64(b)
		h1 *= 1099511628211 // FNV prime
		h2 ^= uint64(b)
		h2 *= 6364136223846793005
	}

	// Final mix
	h1 ^= h1 >> 33
	h1 *= 0xff51afd7ed558ccd
	h1 ^= h1 >> 33

	h2 ^= h2 >> 33
	h2 *= 0xc4ceb9fe1a85ec53
	h2 ^= h2 >> 33

	return h1, h2
}

// EstimateCount estimates the number of items in the filter using
// the number of set bits (useful for diagnostics).
func (f *Filter) EstimateCount() int {
	setBits := 0
	for _, b := range f.bits {
		for b != 0 {
			setBits++
			b &= b - 1
		}
	}
	if setBits == 0 || setBits == int(f.numBits) {
		return 0
	}
	m := float64(f.numBits)
	k := float64(f.numHash)
	x := float64(setBits)
	return int(-m / k * math.Log(1-x/m))
}
