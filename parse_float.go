// Taken from https://github.com/valyala/fastjson/blob/6dae91c8e11a7fa6a257a550b75cba53ab81693e/fastfloat/parse.go#L203
// Faster than std strconv.ParseFloat
package main

// ParseF64, over simplified. We only need to parse -99.9 to 99.9 float, input is always valid
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
