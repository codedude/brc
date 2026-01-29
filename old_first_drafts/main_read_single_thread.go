// TO IMPROVE
// map access

// Steps to optimize:
// Use 1 map and sync read/write instead of merging lots of map
// Map contains pointer to struct, so faster read/write and merge
// Remove merge thread and do it in compute ?
// multithread read with syscall.Pread => hudge upgrade

package main

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"math"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"
)

// Rules and limits
// No external library dependencies may be used
// Implementations must be provided as a single source file
// The computation must happen at application runtime, i.e. you cannot process the measurements file at build time (for instance, when using GraalVM) and just bake the result into the binary
// Input value ranges are as follows:
// 	Station name: non null UTF-8 string of min length 1 character and max length 100 bytes, containing neither ; nor \n characters. (i.e. this could be 100 one-byte characters, or 50 two-byte characters, etc.)
// 	Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
// There is a maximum of 10,000 unique station names
// Line endings in the file are \n characters on all platforms
// Implementations must not rely on specifics of a given data set, e.g. any valid station name as per the constraints above and any data distribution (number of measurements per station) must be supported
// The rounding of output values must be done using the semantics of IEEE 754 rounding-direction "roundTowardPositive"

// Input: 1b lines of "STATION;TEMP\n"
// CITY = non null UTF-8 char (1 or 2 bytes)
// TEMP = -99.9 <= t <= 99.9

// Output: "{STATION=MIN/MEAN/MAX, ...}"

// Must be at least the maximum size of a line + 1 for NL
// So 107 bytes at minimum
const FILE_CHUNK_SIZE = 1024 * 1024 * 32
const MAX_NUMBER_OF_KEYS = 10000

type HashType = uint64

type BlockData struct {
	Name     []byte
	Min      float64
	Max      float64
	Sum      float64
	Mean     float64
	Quantity int
}

func NewBlockData(name []byte) BlockData {
	return BlockData{
		Name:     name,
		Min:      999.9,
		Max:      -999.9,
		Sum:      0.0,
		Quantity: 0,
	}
}

type StationMap = map[HashType]BlockData

func Solve(input, output string) error {
	inFs, _ := os.OpenFile(input, os.O_RDONLY, 0o764)
	defer inFs.Close()
	outFs, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o764)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer outFs.Close()
	dataMap := make(StationMap, MAX_NUMBER_OF_KEYS)
	err = start1BRC(inFs, outFs, dataMap)
	return err
}

func start1BRC(inFs, outFs *os.File, dataMap StationMap) error {
	var wg sync.WaitGroup
	// Keep one thread for main/writeOutput
	numThreads := runtime.NumCPU()
	reader := make(chan []byte, numThreads)
	writer := make(chan StationMap, numThreads)
	stop := make(chan bool)

	go mergeBlocks(writer, stop, dataMap)
	for range numThreads - 1 {
		wg.Add(1)
		go compute(&wg, reader, writer)
	}
	// main thread = start, read, wait, write (cannot be parallelized)
	// 1 thread for merge (merge as soon as a block has been treated)
	// 1 to (MAX_THREAD-1) for compute (compute as soon values comes from the reader)
	// using 2 thread (main + 1x merge + 1x compute) = 1min
	// using 12 thread (main + 1x merge + 1x compute) = 8sec
	// 8300ms
	// read only = 2600ms
	// without merge = 7800ms
	// other part (merge + compute) = 5700ms
	time1 := time.Now().UnixMicro() / 1000
	_ = readInput(inFs, reader)
	time2 := time.Now().UnixMicro() / 1000
	fmt.Println(time2 - time1)
	// 40ms
	wg.Wait()
	close(writer)
	<-stop
	// 70ms
	writeOutput(outFs, dataMap)
	//
	return nil
}

func writeOutput(outFs *os.File, dataMap StationMap) {
	strings := make([]string, 0, MAX_NUMBER_OF_KEYS)
	for _, v := range dataMap {
		mean := math.Ceil(v.Sum/float64(v.Quantity)*(math.Pow10(1))) / math.Pow10(1)
		strings = append(strings, fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", v.Name, v.Min, mean, v.Max))
	}
	// for 10k uniques, sort number is wrong?? i1000 is before i1;, it should not
	var buffer bytes.Buffer
	if len(strings) > 0 {
		slices.Sort(strings)
		strings[len(strings)-1] = strings[len(strings)-1][:len(strings[len(strings)-1])-2]
		buffer.Grow(1024 * 1024 * 3) // enough to hold all test cases
		buffer.WriteByte('{')
		for _, str := range strings {
			buffer.WriteString(str)
		}
	}
	buffer.WriteByte('}')
	buffer.WriteByte('\n')
	outFs.Write(buffer.Bytes())
}

func compute(wg *sync.WaitGroup, reader chan []byte, writer chan StationMap) {
	defer wg.Done()
	var h maphash.Hash
	for data := range reader {
		localMap := make(StationMap, MAX_NUMBER_OF_KEYS)
		// iterate through the block
		for i := 0; i < len(data); i++ {
			// iterate one line
			lineSplitPos := bytes.IndexByte(data[i:min(len(data), i+101)], ';') + i
			lineEndPos := bytes.IndexByte(data[lineSplitPos+1:min(len(data), lineSplitPos+1+6)], '\n')
			lineEndPos += lineSplitPos + 1
			h.Write(data[i:lineSplitPos])
			station := h.Sum64()
			h.Reset()
			temp, _ := ParseF64(data[lineSplitPos+1 : lineEndPos])
			// if err != nil {
			// 	fmt.Println(err)
			// }
			// Get or create blockdata for this station
			e, ok := localMap[station]
			if !ok {
				e = NewBlockData(data[i:lineSplitPos])
			}
			// update value
			if temp < e.Min {
				e.Min = temp
			}
			if temp > e.Max {
				e.Max = temp
			}
			e.Sum += temp
			e.Quantity += 1
			localMap[station] = e
			i = lineEndPos
		}
		writer <- localMap
	}
}

func mergeBlocks(writer chan StationMap, stop chan bool, dataMap StationMap) {
	// merge each divided data to main dataMap
	for localMap := range writer {
		for k, v := range localMap {
			// lock on write
			e, ok := dataMap[k]
			// lock on write
			if !ok {
				dataMap[k] = v
			} else {
				dataMap[k] = BlockData{
					Name:     v.Name,
					Min:      math.Min(e.Min, v.Min),
					Max:      math.Max(e.Max, v.Max),
					Sum:      e.Sum + v.Sum,
					Quantity: e.Quantity + v.Quantity,
				}
			}
		}
	}
	stop <- true
}

// Read input file by chunk and send block of full lines to the compute thread
func readInput(inFs *os.File, reader chan []byte) int {
	buffer := make([]byte, FILE_CHUNK_SIZE)
	bufferOffset := 0
	nOfBLocks := 0
	for {
		n, err := inFs.Read(buffer[bufferOffset:])
		if err != nil || n == 0 {
			break
		}
		endOfByteRead := bufferOffset + n
		lastNlOffset := 0
		// here compute lines
		for i := endOfByteRead - 1; i >= 0; i-- {
			if buffer[i] == '\n' {
				lastNlOffset = i
				break
			}
		}
		cpyBuff := make([]byte, lastNlOffset+1) // include NL for optimization
		copy(cpyBuff, buffer[:lastNlOffset+1])
		reader <- cpyBuff
		nOfBLocks += 1 // stats
		// copy not used data at then end to the beginning of the buffer for next round
		copy(buffer, buffer[lastNlOffset+1:endOfByteRead])
		bufferOffset = endOfByteRead - lastNlOffset - 1
	}
	close(reader)
	return nOfBLocks
}

func run(args []string) int {
	if len(args) < 2 {
		fmt.Println("Usage: ./exe input output")
		return 0
	}
	input, output := args[1], args[2]
	err := Solve(input, output)
	if err != nil {
		return -1
	} else {
		return 0
	}
}

func main() {
	ret := run(os.Args)
	os.Exit(ret)
}

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
	// EDIT no need for our case
	// f := float64(d)
	// if i >= uint(len(s)) {
	// 	// Fast path - just integer.
	// 	if minus {
	// 		f = -f
	// 	}
	// 	return f, nil
	// }
	var f float64
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
