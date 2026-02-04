package main

import (
	"fmt"
	"os"
	"sync"
	"syscall"
)

const MAX_LINE_SIZE = 128
const REAL_MAX_LINE_SIZE = 107 // label=100, ;=1, temp=5,\n=1 => 107, round to 128
const MIN_LINE_SIZE = 6        // label=1, ;=1, temp=3,\n=1 => 6

// calcChunkAndThreadSize adapt parameters for multithreaded read
// chunkSize and nThreads can be lowered (never below 107 he minimum), but never raised
// try to keep the maximum number of threads ask (arbitrary choice)
func calcChunkAndThreadSize(size int64, chunkSize, nThreads int) (int64, int, int) {
	thChunkSize := size / int64(nThreads)
	if size%int64(nThreads) != 0 {
		thChunkSize += 1
	}
	// update chunk sizes to fit in size
	if int64(chunkSize) > size {
		chunkSize = max(int(size), REAL_MAX_LINE_SIZE)
	}
	if int64(chunkSize) > thChunkSize {
		if int(thChunkSize) >= 107 {
			chunkSize = int(thChunkSize)
		} else {
			chunkSize = REAL_MAX_LINE_SIZE
			thChunkSize = REAL_MAX_LINE_SIZE
		}
	}
	// update nThreads with updated data
	nThreads = int(size / thChunkSize)
	if size%thChunkSize != 0 {
		nThreads += 1
	}
	return thChunkSize, chunkSize, nThreads
}

func ReadFileFast(filename string, chunkSize int, nThreads int, allStationMaps *[]MapStation) error {
	if chunkSize < REAL_MAX_LINE_SIZE {
		return fmt.Errorf("chunk_size must be greater than %d", REAL_MAX_LINE_SIZE)
	}
	if nThreads < 1 {
		return fmt.Errorf("n_threads must be greater than 1")
	}
	file, err := os.OpenFile(filename, os.O_RDONLY, 0o764)
	if err != nil {
		return err
	}
	defer file.Close()
	var size int64
	if info, err := file.Stat(); err == nil {
		size = info.Size()
	} else {
		return err
	}
	fd := int(file.Fd())
	t_chunk_size, chunkSize, nThreads := calcChunkAndThreadSize(size, chunkSize, nThreads)
	*allStationMaps = make([]MapStation, nThreads)
	for i := range *allStationMaps {
		(*allStationMaps)[i] = make(MapStation, 1024)
	}
	var wg sync.WaitGroup
	for i := range nThreads {
		wg.Go(func() {
			asyncRead(fd, int64(chunkSize), size, int64(i), t_chunk_size, (*allStationMaps)[i])
		})
	}
	wg.Wait()
	return nil
}

// read all "t_chunk_size" per thread, "chunk_size" chunk at a time, +MAX_SIZE_LINE at the end
func asyncRead(fd int, chunk_size, size, t_i, t_chunk_size int64, stationMap MapStation) {
	t_offset_start := t_i * t_chunk_size
	buff := make([]byte, max(chunk_size*2, MAX_LINE_SIZE*2))
	var totalRead int64 = 0
	if t_i != 0 { // only if thread starts in the middle, start next line
		n, _ := syscall.Pread(fd, buff[:chunk_size], t_offset_start)
		totalRead = int64(findIndexOf(buff[:n], '\n')) + 1
		if totalRead == 0 { // can happen for high nTthreads vs low chunkSize
			return
		}
		if t_offset_start+totalRead >= size { // useless thread
			return
		}
	}
	var buff_offset int64 = 0 // keeps track of remaining data after each read
	for totalRead < t_chunk_size {
		// ajust buffer to only read what we need
		buff_end_offset := buff_offset + min(chunk_size, t_chunk_size-totalRead)
		_n, _ := syscall.Pread(fd, buff[buff_offset:buff_end_offset], t_offset_start+totalRead)
		if _n == 0 {
			break
		}
		n := int64(_n)
		if n < buff_end_offset-buff_offset {
			// if we read less than expected (end of file or end of t_chunk)
			// we need to adjust slice, else we can find NL from previous lines
			buff_end_offset = buff_offset + n
		}
		totalRead += n
		// we know buff starts after NL, find the next NL
		var pos int64
		for pos = buff_end_offset - 1; pos >= 0; pos-- {
			if buff[pos] == '\n' {
				break
			}
		}
		if pos >= MIN_LINE_SIZE-1 {
			ParseLines(buff[:pos+1], stationMap)
		} // else we are at end of t_chunk_size, treated after the loop
		buff_offset = buff_end_offset - (pos + 1)
		copy(buff, buff[pos+1:buff_end_offset])
	}
	if t_offset_start+totalRead < size-1 { // all but last thread when not at end of file
		// For the last line always read a MAX_LINE_SIZE up to the next \n,
		// even if we are on a \n. This way, we know each line will be parsed once,
		// and threads can be independant
		_, _ = syscall.Pread(fd, buff[buff_offset:min(buff_offset+MAX_LINE_SIZE, int64(len(buff)))], t_offset_start+totalRead)
		lastNl := int64(findIndexOf(buff, '\n')) + 1
		if lastNl > MIN_LINE_SIZE-1 {
			ParseLines(buff[:lastNl], stationMap)
		}
	}
}
