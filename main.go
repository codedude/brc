// Macbook pro M2 Pro 12 cores: 4.7sec, 24 threads, 2Mo read chunk, 56Mo of RAM used
package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path"
	"runtime"
)

func Solve(file_in, file_out string, chunkSize, nThreads int) error {
	var allStationMaps []MapStation = nil
	err := ReadFileFast(file_in, chunkSize, nThreads, &allStationMaps)
	if err != nil {
		return err
	}
	// estimate the final number of stations to limit allocation during loop
	// quick and dirty but works: 877 => 1024, 1024 => 2048, 8191 => 8192
	totalKeySize := 0
	for _, m := range allStationMaps {
		totalKeySize += len(m)
	}
	totalKeySize = (totalKeySize/nThreads/1024 + 1) * 1024
	stationLst := make([]*StationData, 0, totalKeySize)
	mergeMaps(allStationMaps, &stationLst)
	err = writeData(file_out, stationLst)
	return err
}

func usageAndExit(msg string) {
	fmt.Fprintf(os.Stderr, "error: %s\n", msg)
	flag.Usage()
	fmt.Println("Default output: ./output/input_name.out")
	os.Exit(1)
}

func stderrAndExit(msg string) {
	fmt.Fprintf(os.Stderr, "error: %s\n", msg)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 1 {
		usageAndExit("Not enough argument")
	}
	inputPath := flag.String("input", "", "Input file path")
	nThreads := flag.Int("n_threads", runtime.NumCPU(), "Max number of threads to use [1-1024]")
	chunkSize := flag.Int("chunk_size", 1024*1024*1, fmt.Sprintf("Chunk size per read [128-%d]", math.MaxInt32))
	verbose := flag.Bool("verbose", false, "If off, not output on stdout")
	flag.Parse()
	if len(*inputPath) == 0 {
		usageAndExit("input is empty")
	}
	if nThreads != nil && (*nThreads < 1 || *nThreads > 1024) {
		usageAndExit("n_threads out of bound")
	}
	if chunkSize != nil && *chunkSize < 128 {
		usageAndExit("chunk_size out of bound")
	}
	err := os.Mkdir("output", 0o764)
	if err != nil && !os.IsExist(err) {
		stderrAndExit(fmt.Sprintf("Cannot create output folder: %s", err.Error()))
	}
	input_file := *inputPath
	output_file := path.Join("./output", path.Base(input_file)) + ".out"
	if _, err := os.Stat(input_file); errors.Is(err, os.ErrNotExist) {
		stderrAndExit(fmt.Sprintf("Input file does not exists or is not accessible: %s", err.Error()))
	}
	err = Solve(input_file, output_file, *chunkSize, *nThreads)
	if err != nil {
		stderrAndExit(err.Error())
	}
	if *verbose {
		fmt.Fprintf(os.Stdout, "Output file: %s\n", output_file)
	}
}
