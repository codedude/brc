// Macbook pro M2 Pro 12 threads: 3.6sec, 24Mo of RAM
package main

import (
	"fmt"
	"runtime"
)

const MAX_NUMBER_OF_KEYS = 10000

// for file reading => 15909705968 bytes
// chunk_size = 512ko is fine, less is not enough, more not better or even worse
// n_threads = # of core is a good, 2x is 25% better, more is useless
// multithreaded read is 2 times faster
func main() {
	var allStationMaps []MapStation = nil
	// mergeChan := make(chan MapStation) // chan to get data read from file
	// stopChan := make(chan bool)
	// stationMap := make(MapStation, MAX_NUMBER_OF_KEYS)
	n_threads := 1 * runtime.NumCPU() // number of real core
	// n_threads := 1 // number of real core
	// chunkSize := 128 // in byte
	chunkSize := 1024 * 512 // in byte
	// err := ReadFileFast("samples/measurements-10000-unique-keys.txt", chunkSize, n_threads, &allStationMaps)
	// err := ReadFileFast("samples/measurements-20.txt", chunkSize, n_threads, &allStationMaps)
	// err := ReadFileFast("samples/measurements-rounding.txt", chunkSize, n_threads, &allStationMaps)
	err := ReadFileFast("samples/data-1b.txt", chunkSize, n_threads, &allStationMaps)
	// close(mergeChan)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println(counter)                        //  1000000000
	stationLst := make([]*StationData, 0, 8926) // size of the 1 billion samples
	mergeMaps(allStationMaps, &stationLst)
	// fmt.Println(len(data)) //8926
	writeData("output/out.txt", stationLst)
}
