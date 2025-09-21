package main

import (
	"fmt"
	"os"
	"runtime"
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

func main() {
	ret := run(os.Args)
	os.Exit(ret)
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

type BlockData struct {
	Min      float32
	Max      float32
	Sum      float32
	Quantity int32
}

func Solve(input, output string) error {
	inFs, _ := os.OpenFile(input, os.O_RDONLY, 0o764)
	defer inFs.Close()
	outFs, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o764)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer outFs.Close()
	outFs.Write([]byte{'{'})
	err = start1BRC(inFs, outFs)
	outFs.Write([]byte{'}', '\n'})
	return err
}

func start1BRC(inFs, outFs *os.File) error {
	numThreads := runtime.NumCPU()
	reader := make(chan BlockData, numThreads)
	go readInput(inFs, reader)

	var buffer = make([]BlockData, numThreads)
	for range numThreads {
		data := <-reader
		buffer = append(buffer, data)
	}

	return nil
}

func readInput(inFs *os.File, reader chan BlockData) {
	for {
		reader <- BlockData{}
	}
}
