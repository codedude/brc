package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
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
const FILE_CHUNK_SIZE = 1024 * 1024 * 4
const WORK_LINE_SIZE = 4096

type BlockData struct {
	Id       int
	Min      float32
	Max      float32
	Sum      float32
	Quantity int
}

type StationMap = map[string]int

func Solve(input, output string) error {
	inFs, _ := os.OpenFile(input, os.O_RDONLY, 0o764)
	defer inFs.Close()
	outFs, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o764)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer outFs.Close()
	dataMap := make(StationMap, 4096)
	outFs.Write([]byte{'{'})
	err = start1BRC(inFs, outFs, dataMap)
	outFs.Write([]byte{'}', '\n'})
	fmt.Println(dataMap)
	return err
}

func start1BRC(inFs, outFs *os.File, dataMap StationMap) error {
	numThreads := runtime.NumCPU()
	var wg sync.WaitGroup
	reader := make(chan string, numThreads)
	writer := make(chan BlockData, numThreads)

	go readInput(inFs, reader)
	go writeOutput(writer, dataMap)
	for range numThreads {
		wg.Add(1)
		go compute(&wg, reader, writer)
	}
	wg.Wait()
	close(writer)
	return nil
}

func compute(wg *sync.WaitGroup, reader chan string, writer chan BlockData) {
	defer wg.Done()
	for data := range reader {
		// Format: xxx;-xx.x
		// FOREACH line -> dont split, linear walk + slices
		// 1) Find ';'
		// 2) convert name to int hash
		// 3) convert end of line to float (from ;+1 to \n or 0)
		// 4) get entry for xxx if existing, else empty data
		// 5) compute
		// 6) store new value
		nOfLines := strings.Count(data, "\n") + 1
		// fmt.Println("line=", data)
		writer <- BlockData{Id: 0, Min: 0, Max: 0, Sum: 0, Quantity: nOfLines}
	}
}

func writeOutput(writer chan BlockData, dataMap StationMap) {
	total := 0
	dataMap["ok"] = 0
	for data := range writer {
		total += int(data.Quantity)
		dataMap["ok"] += int(data.Quantity)
	}
	fmt.Println("Total=", total)
}

// Read input file by chunk and send block of full lines to the compute thread
func readInput(inFs *os.File, reader chan string) {
	fmt.Println("Start of reader")
	buffer := make([]byte, FILE_CHUNK_SIZE)
	lastNlOffset := 0
	bufferOffset := 0
	for {
		n, err := inFs.Read(buffer[bufferOffset:])
		if err != nil || n == 0 {
			break
		}
		endOfByteRead := bufferOffset + n
		// here compute lines
		for i := endOfByteRead - 1; i >= 0; i-- {
			if buffer[i] == '\n' {
				lastNlOffset = i
				break
			}
		}
		reader <- string(buffer[:lastNlOffset])
		copy(buffer, buffer[lastNlOffset+1:endOfByteRead])
		bufferOffset = endOfByteRead - lastNlOffset - 1
	}
	close(reader)
	fmt.Println("End of reader")
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
