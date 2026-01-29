// Macbook pro M2 Pro 12 threads: 3.6sec, 24Mo of RAM
package main

import (
	"log"
	"os"
	"runtime"
)

func Solve(file_in, file_out string, chunkSize, nThreads int) error {
	var allStationMaps []MapStation = nil
	err := ReadFileFast(file_in, chunkSize, nThreads, &allStationMaps)
	if err != nil {
		return err
	}
	stationLst := make([]*StationData, 0, 8926) // size of the 1 billion samples
	mergeMaps(allStationMaps, &stationLst)
	// fmt.Println(counter)   //  1000000000
	// fmt.Println(len(data)) //8926
	err = writeData(file_out, stationLst)
	return err
}

func main() {
	err := os.Mkdir("output", 0o764)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	nThreads := 1 * runtime.NumCPU() // number of real core
	chunkSize := 1024 * 512          // in byte
	err = Solve("samples/data-1b.txt", "output/data-1b.out", chunkSize, nThreads)
	if err != nil {
		log.Fatal(err)
	}
}
