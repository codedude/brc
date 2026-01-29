// Taken from https://github.com/valyala/fastjson/blob/6dae91c8e11a7fa6a257a550b75cba53ab81693e/fastfloat/parse.go#L203
// Faster than std strconv.ParseFloat
package main

// ParseF32 is ParseF64, but 32bit instead of 64 bits, and remove any error checking to speed things up
func ParseF32(s []byte) float32 {
	i := uint(0)
	minus := s[0] == '-'
	if minus {
		i++
	}
	d := uint32(0)
	for i < uint(len(s)) {
		if s[i] >= '0' && s[i] <= '9' {
			d = d*10 + uint32(s[i]-'0')
			i++
			continue
		}
		break
	}

	var f float32
	if s[i] == '.' {
		// Parse fractional part.
		i++
		k := i
		if s[i] >= '0' && s[i] <= '9' {
			d = d*10 + uint32(s[i]-'0')
			i++
		}
		// Convert the entire mantissa to a float at once to avoid rounding errors.
		f = float32(d) / float32pow10[i-k]
		// Fast path - parsed fractional number.
		if minus {
			f = -f
		}
		return f
	}
	return 0
}

// Exact powers of 10.
// This works faster than math.Pow10, since it avoids additional multiplication.
var float32pow10 = [...]float32{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
}
