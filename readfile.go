package main

import (
	"fmt"
	"os"
	"slices"
	"sync"
	"syscall"
)

const MAX_LINE_SIZE = 128 // label=100, ;=1, temp=5,\n=1 => 107, round to 128
const MIN_LINE_SIZE = 6   // label=1, ;=1, temp=3,\n=1 => 6

func ReadFileFast(filename string, chunk_size int, n_threads int, allStationMaps *[]MapStation) error {
	if chunk_size < MAX_LINE_SIZE {
		return fmt.Errorf("chunk_size must be greater than MAX_LINE_SIZE %d", MAX_LINE_SIZE)
	}
	if n_threads < 1 {
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
	t_chunk_size := size/int64(n_threads) + 1
	if chunk_size > int(t_chunk_size) { // asking to read more per read than the whole thread chunk
		t_chunk_size = int64(chunk_size)
		n_threads = int(size/t_chunk_size) + 1
		//fmt.Printf("n_threads reduced to: %d, t_chunk augmented to: %d\n", n_threads, t_chunk_size)
	}
	if t_chunk_size == 1 { // to test small values, like 100 thread for a 80 bytes file
		n_threads = int(size)
		//fmt.Printf("n_threads reduced to: %d\n", n_threads)
	}
	// fmt.Println(chunk_size, t_chunk_size, t_chunk_size/int64(chunk_size), size)
	*allStationMaps = make([]MapStation, n_threads)
	for i := range *allStationMaps {
		(*allStationMaps)[i] = make(MapStation, 1024)
	}
	var wg sync.WaitGroup
	for i := 0; i < n_threads; i++ {
		wg.Go(func() {
			asyncRead(fd, int64(chunk_size), size, int64(i), t_chunk_size, (*allStationMaps)[i])
		})
	}
	wg.Wait()
	return nil
}

// read all "t_chunk_size" per thread, "chunk_size" chunk at a time, +MAX_SIZE_LINE at the end
func asyncRead(fd int, chunk_size, size, t_i, t_chunk_size int64, stationMap MapStation) {
	t_offset_start := t_i * t_chunk_size
	buff := make([]byte, chunk_size*2)
	var totalRead int64 = 0
	if t_i != 0 { // only if thread starts in the middle, start next line
		n, _ := syscall.Pread(fd, buff[:MAX_LINE_SIZE], t_offset_start)
		totalRead = nextNlInBuff(buff[:n]) + 1
		if totalRead == 0 { // should never happen with valid input
			fmt.Println("nextNlOffset did not find a \\n in first line, cannot happen")
			return
		}
		if t_offset_start+totalRead >= size {
			return
		}
	}
	var buff_offset int64 = 0 // keeps track of remaining data after each read
	for totalRead < t_chunk_size {
		// ajust buffer to only read what we need
		buff_end_offset := buff_offset + min(chunk_size, t_chunk_size-totalRead)
		_n, _ := syscall.Pread(fd, buff[buff_offset:buff_end_offset], t_offset_start+totalRead)
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
			ParseLines(buff[:pos], stationMap)
		} // else we are at end of t_chunk_size, treated after the loop
		buff_offset = buff_end_offset - (pos + 1)
		copy(buff, buff[pos+1:buff_end_offset])
		if n < chunk_size { // last chunk read, we stop
			break
		}
	}
	if t_offset_start+totalRead < size-1 { // all but last thread when not at end of file
		// For the last line always read a MAX_LINE_SIZE up to the next \n,
		// even if we are on a \n. This way, we know each line will be parsed once,
		// and threads can be independant
		_, _ = syscall.Pread(fd, buff[buff_offset:buff_offset+MAX_LINE_SIZE], t_offset_start+totalRead)
		lastNl := nextNlInBuff(buff)
		if lastNl >= MIN_LINE_SIZE-1 {
			ParseLines(buff[:lastNl], stationMap)
		}
	}
}

func nextNlInBuff(buff []byte) int64 {
	return int64(slices.Index(buff, '\n'))
}
