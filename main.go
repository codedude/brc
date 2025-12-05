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
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"os"
	"runtime"
	"slices"
	"sync"
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
const FILE_CHUNK_SIZE = 1024 * 1024
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

type StationMap = map[HashType]*BlockData

func Solve(input, output string) error {
	inFs, _ := os.OpenFile(input, os.O_RDONLY, 0o764)
	defer inFs.Close()
	var inSize int64
	if info, err := inFs.Stat(); err == nil {
		inSize = info.Size()
	} else {
		return errors.New("Cannot Stat file")
	}
	fmt.Println(inSize)
	outFs, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o764)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer outFs.Close()
	dataMap := make(StationMap, MAX_NUMBER_OF_KEYS)

	var wg sync.WaitGroup
	numThreads := runtime.NumCPU()
	for range numThreads {
		wg.Add(1)
		go compute(&wg, dataMap)
	}
	wg.Wait()
	writeOutput(outFs, dataMap)
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
