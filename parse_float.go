// Taken from https://github.com/valyala/fastjson/blob/6dae91c8e11a7fa6a257a550b75cba53ab81693e/fastfloat/parse.go#L203
// Faster than std strconv.ParseFloat
package main

import (
	"fmt"
	"math"
)

func ParseF64(s []byte) (float64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("cannot parse float64 from empty string")
	}
	i := uint(0)
	minus := s[0] == '-'
	if minus {
		i++
		if i >= uint(len(s)) {
			return 0, fmt.Errorf("cannot parse float64 from %q", s)
		}
	}

	d := uint64(0)
	j := i
	for i < uint(len(s)) {
		if s[i] >= '0' && s[i] <= '9' {
			d = d*10 + uint64(s[i]-'0')
			i++
			// EDIT no need for our case
			// if i > 18 {
			// 	// The integer part may be out of range for uint64.
			// 	// Fall back to slow parsing.
			// 	f, err := strconv.ParseFloat(s, 64)
			// 	if err != nil && !math.IsInf(f, 0) {
			// 		return 0, err
			// 	}
			// 	return f, nil
			// }
			continue
		}
		break
	}
	if i <= j {
		ss := s[i:]
		// EDIT no need for our case
		// if strings.HasPrefix(ss, "+") {
		// 	ss = ss[1:]
		// }
		// EDIT no need for our case
		// "infinity" is needed for OpenMetrics support.
		// See https://github.com/OpenObservability/OpenMetrics/blob/master/OpenMetrics.md
		// if strings.EqualFold(ss, "inf") || strings.EqualFold(ss, "infinity") {
		// 	if minus {
		// 		return -inf, nil
		// 	}
		// 	return inf, nil
		// }
		// if strings.EqualFold(ss, "nan") {
		// 	return nan, nil
		// }
		return 0, fmt.Errorf("unparsed tail left after parsing float64 from %q: %q", s, ss)
	}
	f := float64(d)
	if i >= uint(len(s)) {
		// Fast path - just integer.
		if minus {
			f = -f
		}
		return f, nil
	}

	if s[i] == '.' {
		// Parse fractional part.
		i++
		if i >= uint(len(s)) {
			return 0, fmt.Errorf("cannot parse fractional part in %q", s)
		}
		k := i
		for i < uint(len(s)) {
			if s[i] >= '0' && s[i] <= '9' {
				d = d*10 + uint64(s[i]-'0')
				i++
				// EDIT no need for our case
				// if i-j >= uint(len(float64pow10)) {
				// 	// The mantissa is out of range. Fall back to standard parsing.
				// 	f, err := strconv.ParseFloat(s, 64)
				// 	if err != nil && !math.IsInf(f, 0) {
				// 		return 0, fmt.Errorf("cannot parse mantissa in %q: %s", s, err)
				// 	}
				// 	return f, nil
				// }
				continue
			}
			break
		}
		if i < k {
			return 0, fmt.Errorf("cannot find mantissa in %q", s)
		}
		// Convert the entire mantissa to a float at once to avoid rounding errors.
		f = float64(d) / float64pow10[i-k]
		if i >= uint(len(s)) {
			// Fast path - parsed fractional number.
			if minus {
				f = -f
			}
			return f, nil
		}
	}
	if s[i] == 'e' || s[i] == 'E' {
		// Parse exponent part.
		i++
		if i >= uint(len(s)) {
			return 0, fmt.Errorf("cannot parse exponent in %q", s)
		}
		expMinus := false
		if s[i] == '+' || s[i] == '-' {
			expMinus = s[i] == '-'
			i++
			if i >= uint(len(s)) {
				return 0, fmt.Errorf("cannot parse exponent in %q", s)
			}
		}
		exp := int16(0)
		j := i
		for i < uint(len(s)) {
			if s[i] >= '0' && s[i] <= '9' {
				exp = exp*10 + int16(s[i]-'0')
				i++
				// EDIT no need for our case
				// if exp > 300 {
				// 	// The exponent may be too big for float64.
				// 	// Fall back to standard parsing.
				// 	f, err := strconv.ParseFloat(s, 64)
				// 	if err != nil && !math.IsInf(f, 0) {
				// 		return 0, fmt.Errorf("cannot parse exponent in %q: %s", s, err)
				// 	}
				// 	return f, nil
				// }
				continue
			}
			break
		}
		if i <= j {
			return 0, fmt.Errorf("cannot parse exponent in %q", s)
		}
		if expMinus {
			exp = -exp
		}
		f *= math.Pow10(int(exp))
		if i >= uint(len(s)) {
			if minus {
				f = -f
			}
			return f, nil
		}
	}
	return 0, fmt.Errorf("cannot parse float64 from %q", s)
}

// Exact powers of 10.
//
// This works faster than math.Pow10, since it avoids additional multiplication.
var float64pow10 = [...]float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
}
