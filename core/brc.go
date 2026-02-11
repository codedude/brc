package brc

import (
	"fmt"
	"time"
)

type BrcStrategyType string

const (
	BrcStrategyPreRead  BrcStrategyType = "preload"
	BrcStrategyLazyRead BrcStrategyType = "lazy"
)

var BrcStrategyList = []BrcStrategyType{BrcStrategyPreRead, BrcStrategyLazyRead}

type BrcReaderType string

const (
	BrcReaderDisk BrcReaderType = "disk"
	BrcReaderMmap BrcReaderType = "mmap"
)

var BrcReaderList = []BrcReaderType{BrcReaderDisk, BrcReaderMmap}

type BrcOptions struct {
	ReadChunkFactor int             // factor of pagesize, size of read chunks
	NThreads        int             // number of thread to use (at most, can be lowered)
	Strategy        BrcStrategyType // load data upfront or lazyload
	ReaderType      BrcReaderType   // read on disk or mmap file
	Verbose         bool            // print things in Solve(...) or not
}

func Solve(fileReader FileReader, file_out string, opts BrcOptions) error {
	var err error
	if opts.Strategy == BrcStrategyPreRead {
		if _, err = fileReader.Read(); err != nil {
			return err
		}
	}
	timeBefore := time.Now()
	var allStationMaps []MapStation = nil
	if err := parseFile(fileReader, opts, &allStationMaps); err != nil {
		return err
	}
	// estimate the final number of stations to limit allocation during loop
	// quick and dirty but works: 877 => 1024, 1023 => 2048, 1024 => 2048
	totalKeySize := 0
	for _, m := range allStationMaps {
		totalKeySize += len(m)
	}
	totalKeySize = (totalKeySize/len(allStationMaps)/1024 + 1) * 1024
	stationLst := make([]*StationData, 0, totalKeySize)
	mergeMaps(allStationMaps, &stationLst)
	timeAfter := time.Since(timeBefore)
	if opts.Verbose {
		fmt.Printf("Time taken parse only: %s\n", timeAfter.String())
	}
	return writeData(file_out, stationLst)
}
