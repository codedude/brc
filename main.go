// Macbook pro M2 Pro 12 cores: 4.7sec, 24 threads, 2Mo read chunk, 56Mo of RAM used
package main

import (
	brc "brc/core"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"slices"
	"time"
)

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
		usageAndExit("not enough argument")
	}
	inputPath := flag.String("input", "", "Input file path")
	nThreads := flag.Int("threads", runtime.NumCPU(), "Max number of threads to use (default=number of cores)")
	chunkSize := flag.Int("chunk", (1024*1024)/os.Getpagesize(), fmt.Sprintf("Chunk size per read (a factor of pagesize=%db, default=1Mb)", os.Getpagesize()))
	readerMode := flag.String("reader", string(brc.BrcReaderDisk), "Read from disk or mmap the file first [disk,mmap]")
	strategy := flag.String("mode", string(brc.BrcStrategyLazyRead), "Pre read all file or read as needed [preload,lazy]")
	verbose := flag.Bool("v", false, "If off, not output on stdout")
	profiling := flag.Bool("p", false, "Activate incode pprof CPU profiling")
	flag.Parse()
	if len(*inputPath) == 0 {
		usageAndExit("input is empty")
	}
	if nThreads != nil && *nThreads < 1 {
		usageAndExit("threads out of bound")
	}
	if chunkSize != nil && *chunkSize < 1 {
		usageAndExit("chunk out of bound")
	}
	if !slices.Contains(brc.BrcStrategyList, brc.BrcStrategyType(*strategy)) {
		usageAndExit("strategy unknown")
	}
	if !slices.Contains(brc.BrcReaderList, brc.BrcReaderType(*readerMode)) {
		usageAndExit("mode unknown")
	}
	input_file := *inputPath
	if _, err := os.Stat(input_file); errors.Is(err, os.ErrNotExist) {
		stderrAndExit(fmt.Sprintf("Input file does not exists or is not accessible: %s", err.Error()))
	}
	err := os.Mkdir("output", 0o764)
	if err != nil && !os.IsExist(err) {
		stderrAndExit(fmt.Sprintf("Cannot create output folder: %s", err.Error()))
	}
	output_file := path.Join("./output", path.Base(input_file)) + ".out"
	opts := brc.BrcOptions{
		NThreads:        *nThreads,
		ReadChunkFactor: *chunkSize,
		Strategy:        brc.BrcStrategyType(*strategy),
		ReaderType:      brc.BrcReaderType(*readerMode),
		Verbose:         *verbose,
	}
	var fileReader brc.FileReader
	switch opts.ReaderType {
	case brc.BrcReaderMmap:
		fileReader = brc.NewFileMmapReader()
	case brc.BrcReaderDisk:
		fileReader = brc.NewFileDiskReader()
	default:
		stderrAndExit("unknown reader")
	}
	err = fileReader.Open(input_file)
	if err != nil {
		stderrAndExit(err.Error())
	}
	defer fileReader.Close()
	if *profiling {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			stderrAndExit(err.Error())
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	timeBefore := time.Now()
	err = brc.Solve(fileReader, output_file, opts)
	timeAfter := time.Since(timeBefore)
	if opts.Verbose {
		fmt.Printf("Time taken total: %s\n", timeAfter.String())
	}
	if err != nil {
		stderrAndExit(err.Error())
	}
	if opts.Verbose {
		fmt.Fprintf(os.Stdout, "Output file: %s\n", output_file)
	}
}
