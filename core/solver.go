package brc

import (
	"fmt"
	"os"
	"sync"
)

const MAX_LINE_SIZE = 128 // label=100, ;=1, temp=5,\n=1 => 107, round to 128
const MIN_LINE_SIZE = 6   // label=1, ;=1, temp=3,\n=1 => 6

// calcChunkAndThreadSize adapt parameters for multithreaded read
// Does not modify nThread or initial thChunkSize, only the chunkSize parameter is adapted
func calcChunkAndThreadSize(size int64, chunkSize, nThreads int) (int64, int, int) {
	thChunkSize := size / int64(nThreads)
	if size%int64(nThreads) != 0 {
		thChunkSize += 1
	}
	pageSize := os.Getpagesize()
	chunkSize *= pageSize // chunkSize is a factor of pagesize
	if int64(chunkSize) > thChunkSize {
		thChunkSize = min(int64(chunkSize), size) // grow thChunkSize
		nThreads = int(size / thChunkSize)        // nThreads can be lowered
		if size%thChunkSize != 0 {
			nThreads += 1
		}
	}
	return thChunkSize, chunkSize, nThreads
}

func parseFile(fileReader FileReader, opts BrcOptions, allStationMaps *[]MapStation) error {
	if opts.ReadChunkFactor < 1 {
		return fmt.Errorf("chunk_size must be greater than 0")
	}
	if opts.NThreads < 1 {
		return fmt.Errorf("n_threads must be greater than 1")
	}
	t_chunk_size, chunkSize, nThreads := calcChunkAndThreadSize(
		fileReader.GetSize(), opts.ReadChunkFactor, opts.NThreads)
	*allStationMaps = make([]MapStation, nThreads)
	for i := range *allStationMaps {
		// arbitrary value, better too much than future allocation needed
		(*allStationMaps)[i] = make(MapStation, 1024)
	}
	var wg sync.WaitGroup
	for i := range nThreads {
		wg.Go(func() {
			switch opts.Strategy {
			case BrcStrategyPreRead:
				asyncPreRead(fileReader, int64(chunkSize), int64(i), t_chunk_size, (*allStationMaps)[i])
			case BrcStrategyLazyRead:
				asyncLazyRead(fileReader, int64(chunkSize), int64(i), t_chunk_size, (*allStationMaps)[i])
			default:
				return
			}
		})
	}
	wg.Wait()
	return nil
}

func asyncLazyRead(fileReader FileReader, chunk_size, t_i, t_chunk_size int64, stationMap MapStation) {
	t_offset_start := t_i * t_chunk_size
	buff := make([]byte, max(chunk_size*2, MAX_LINE_SIZE*2))
	var totalRead int64 = 0
	if t_i != 0 { // only if thread starts in the middle, start next line
		n, _ := fileReader.ReadChunk(buff[:chunk_size], t_offset_start)
		totalRead = int64(findIndexOf(buff[:n], patternNl)) + 1
		if totalRead == 0 { // can happen for high nTthreads vs low chunkSize
			return
		}
		if t_offset_start+totalRead >= fileReader.GetSize() { // useless thread
			return
		}
	}
	var buff_offset int64 = 0 // keeps track of remaining data after each read
	for totalRead < t_chunk_size {
		// ajust buffer to only read what we need
		buff_end_offset := buff_offset + min(chunk_size, t_chunk_size-totalRead)
		_n, _ := fileReader.ReadChunk(buff[buff_offset:buff_end_offset], t_offset_start+totalRead)
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
		pos += 1
		if pos > MIN_LINE_SIZE-1 {
			ParseLines(buff[:pos], stationMap)
		} // else we are at end of t_chunk_size, treated after the loop
		buff_offset = buff_end_offset - pos
		copy(buff, buff[pos:buff_end_offset])
	}
	if t_offset_start+totalRead < fileReader.GetSize()-1 { // all but last thread when not at end of file
		// For the last line always read a MAX_LINE_SIZE up to the next \n,
		// even if we are on a \n. This way, we know each line will be parsed once,
		// and threads can be independant
		_, _ = fileReader.ReadChunk(buff[buff_offset:min(buff_offset+MAX_LINE_SIZE, int64(len(buff)))], t_offset_start+totalRead)
		lastNl := int64(findIndexOf(buff, patternNl)) + 1
		if lastNl > MIN_LINE_SIZE {
			ParseLines(buff[:lastNl], stationMap)
		}
	}
}

func asyncPreRead(fileReader FileReader, chunk_size, t_i, t_chunk_size int64, stationMap MapStation) {
	t_offset_start := t_i * t_chunk_size
	// buffLen := max(chunk_size * 2)
	var buff []byte
	var pos int64
	buff_offset := t_offset_start // where to start in the file
	if t_offset_start != 0 {      // only if thread starts in the middle, start next line
		buff, n := fileReader.GetChunk(buff_offset, MAX_LINE_SIZE)
		totalRead := int64(findIndexOf(buff[:n], patternNl)) + 1
		if totalRead == 0 { // can happen for high nTthreads vs low chunkSize
			return
		}
		if t_offset_start+totalRead >= fileReader.GetSize() { // useless thread
			return
		}
		buff_offset += totalRead
	}
	for {
		// ajust buffer to only read what we need
		sizeToRead := min(chunk_size, t_chunk_size-(buff_offset-t_offset_start))
		if sizeToRead < MIN_LINE_SIZE {
			break
		}
		buff, n := fileReader.GetChunk(buff_offset, sizeToRead)
		if n < sizeToRead {
			// if we read less than expected (end of file or end of t_chunk)
			// we need to adjust slice, else we can find NL from previous lines
			sizeToRead = n
		}
		// we know buff starts after NL, find the next NL
		for pos = sizeToRead - 1; pos >= 0; pos-- {
			if buff[pos] == '\n' {
				break
			}
		}
		pos += 1
		if pos > MIN_LINE_SIZE {
			ParseLines(buff[:pos], stationMap)
			buff_offset += pos
		} else {
			break
		}
	}
	if buff_offset < fileReader.GetSize()-1 { // all but last thread when not at end of file
		// For the last line always read a MAX_LINE_SIZE up to the next \n,
		// even if we are on a \n. This way, we know each line will be parsed once,
		// and threads can be independant
		buff, _ = fileReader.GetChunk(buff_offset, buff_offset+MAX_LINE_SIZE)
		lastNl := int64(findIndexOf(buff, patternNl)) + 1
		if lastNl > MIN_LINE_SIZE-1 {
			ParseLines(buff[:lastNl], stationMap)
		}
	}
}
