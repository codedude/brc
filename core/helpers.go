package brc

import (
	"encoding/binary"
	"math/bits"
)

// getHashFromBytes is a simple 64 bits FNV for speed: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func getHashFromBytes(data []byte) uint64 {
	var p uint64 = 1099511628211
	var hash uint64 = 14695981039346656037
	for i := range data {
		hash = (hash ^ uint64(data[i])) * p
	}
	return hash
}

// Best naive option
// for i := range haystack {
// 	if haystack[i] == needle {
// 		return i
// 	}
// }
// return -1
// Almost equivalent
// return slices.Index(haystack, needle)
// return bytes.IndexByte(haystack, needle)

// findIndexOf is optimize for power of 2 buffer size
func findIndexOf(haystack []byte, pattern uint64) int {
	// Best solution
	var i int
	hLen := len(haystack)
	for i = 0; i < hLen/8*8; i += 8 {
		if index := firstInstance(
			binary.BigEndian.Uint64(haystack[i:i+8]), pattern); index != 8 {
			return i + index
		}
	}
	if hLen%8 == 0 {
		return -1
	}
	sliceToUint := uint64(0)
	switch hLen % 8 {
	case 7:
		sliceToUint |= (uint64(haystack[i+6]) << 8)
		fallthrough
	case 6:
		sliceToUint |= (uint64(haystack[i+5]) << 16)
		fallthrough
	case 5:
		sliceToUint |= (uint64(haystack[i+4]) << 24)
		fallthrough
	case 4:
		sliceToUint |= (uint64(haystack[i+3]) << 32)
		fallthrough
	case 3:
		sliceToUint |= (uint64(haystack[i+2]) << 40)
		fallthrough
	case 2:
		sliceToUint |= (uint64(haystack[i+1]) << 48)
		fallthrough
	case 1:
		sliceToUint |= (uint64(haystack[i]) << 56)
	}
	if index := firstInstance(sliceToUint, pattern); index != 8 {
		return i + index
	}
	return -1
}

// https://richardstartin.github.io/posts/finding-bytes
func compilePattern(byteToFind byte) uint64 {
	var pattern uint64 = uint64(byteToFind & 0xFF)
	return pattern |
		(pattern << 8) |
		(pattern << 16) |
		(pattern << 24) |
		(pattern << 32) |
		(pattern << 40) |
		(pattern << 48) |
		(pattern << 56)
}

func firstInstance(word, pattern uint64) int {
	var input uint64 = word ^ pattern
	var tmp uint64 = (input & 0x7F7F7F7F7F7F7F7F) + 0x7F7F7F7F7F7F7F7F
	tmp = ^(tmp | input | 0x7F7F7F7F7F7F7F7F)
	return bits.LeadingZeros64(tmp) >> 3
}

// ParseF64, over simplified. We only need to parse -99.9 to 99.9 float, input is always valid
// Taken from https://github.com/valyala/fastjson/blob/6dae91c8e11a7fa6a257a550b75cba53ab81693e/fastfloat/parse.go#L203
// Faster than std strconv.ParseFloat
func ParseF64(s []byte) float64 {
	var f float64
	i := 0
	d := 0
	minus := s[0] == '-'
	if minus {
		i++
	}
	if s[i] >= '0' && s[i] <= '9' {
		d = d*10 + int(s[i]-'0')
		i++
	}
	if s[i] >= '0' && s[i] <= '9' {
		d = d*10 + int(s[i]-'0')
		i++
	}
	if s[i] == '.' {
		i++
		k := i
		if s[i] >= '0' && s[i] <= '9' {
			d = d*10 + int(s[i]-'0')
			i++
		}
		// Convert the entire mantissa to a float at once to avoid rounding errors.
		f = float64(d) / float64pow10[i-k]
	} else {
		f = float64(d)
	}
	if minus {
		f = -f
	}
	return f
}

// Exact powers of 10.
// This works faster than math.Pow10, since it avoids additional multiplication.
var float64pow10 = [...]float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
}
