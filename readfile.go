package main

import (
	"fmt"
	"os"
	"slices"
	"sync"
	"syscall"
)

const MAX_LINE_SIZE = 128 // label=100, :=1, temp=5,\n=1 => 107, round to 128

func ReadFileFast(filename string, mergeChan chan MapStation, chunk_size int, n_threads int) error {
	if chunk_size < MAX_LINE_SIZE {
		return fmt.Errorf("chunk_size must be greater than MAX_LINE_SIZE %d", MAX_LINE_SIZE)
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
	t_chunk_size := size / int64(n_threads)
	if size%int64(n_threads) != 0 { // last incomplete part
		t_chunk_size += 1
	}
	if chunk_size > int(t_chunk_size) { // asking to read more per read than the whole thread chunk
		n_threads = int(size / int64(chunk_size))
		if size%int64(chunk_size) != 0 {
			n_threads += 1
		}
		t_chunk_size = int64(chunk_size)
		fmt.Printf("n_threads reduced to: %d, t_chunk augmented to: %d\n", n_threads, t_chunk_size)
	}
	if t_chunk_size == 1 { // to test small values, like 100 thread for a 80 bytes file
		n_threads = int(size)
		fmt.Printf("n_threads reduced to: %d\n", n_threads)
	}
	// fmt.Println(chunk_size, t_chunk_size, t_chunk_size/int64(chunk_size), size)
	var wg sync.WaitGroup
	for i := 0; i < n_threads; i++ {
		wg.Go(func() {
			asyncRead(mergeChan, fd, chunk_size, size, int64(i), t_chunk_size)
		})
	}
	wg.Wait()
	return nil
}

// read all "t_chunk_size" per thread, "chunk_size" chunk at a time, +MAX_SIZE_LINE at the end
func asyncRead(mergeChan chan MapStation, fd int, chunk_size int, size, t_i, t_chunk_size int64) {
	t_offset_start := int64(t_i) * t_chunk_size
	buff := make([]byte, chunk_size*2)
	var totalRead int64 = 0
	if t_i != 0 { // only if thread starts in the middle, start next line
		n, _ := syscall.Pread(fd, buff[:MAX_LINE_SIZE], t_offset_start)
		totalRead = nextNlInBuff(buff[:n]) + 1
		if totalRead == 0 { // should never happen with valid input
			fmt.Println("nextNlOffset did not find a \\n in first line, cannot happen")
			return
		}
	}
	var buff_offset int64 = 0 // keeps track of remaining data after each read
	for totalRead < t_chunk_size {
		// ajust buffer to only read what we need
		buff_end_offset := buff_offset + min(int64(chunk_size), t_chunk_size-totalRead)
		n, _ := syscall.Pread(fd, buff[buff_offset:buff_end_offset], t_offset_start+totalRead)
		if int64(n) < buff_end_offset-buff_offset {
			// if we read less than expected (end of file or end of t_chunk)
			// we need to adjust slice, else we can find NL from previous lines
			buff_end_offset = buff_offset + int64(n)
		}
		totalRead += int64(n)
		// we know buff starts after NL, find the next NL
		var pos int64
		for pos = buff_end_offset - 1; pos >= 0; pos-- {
			if buff[pos] == '\n' {
				break
			}
		}
		ParseLines(mergeChan, buff[:pos])
		buff_offset = buff_end_offset - (pos + 1)
		if buff_offset != 0 {
			copy(buff, buff[pos+1:buff_end_offset])
		} // else no data to copy
		if n < chunk_size { // last chunk read, we stop
			break
		}
	}
	if t_offset_start+totalRead < size-1 { // all but last thread when not at end of file
		// For the last line always read a MAX_LINE_SIZE up to the next \n,
		// even if we are on a \n. This way, we know each line will be parsed once,
		// and threads can be independant
		_, _ = syscall.Pread(fd, buff[buff_offset:MAX_LINE_SIZE], t_offset_start+totalRead)
		// fmt.Println("#", string(buff[:buff_offset+MAX_LINE_SIZE]), "#\n")
		lastNl := nextNlInBuff(buff)
		// cpyBuff := make([]byte, lastNl)
		// copy(cpyBuff, buff[:lastNl])
		// chanOut <- cpyBuff
		ParseLines(mergeChan, buff[:lastNl])
	}
}

func nextNlInBuff(buff []byte) int64 {
	return int64(slices.Index(buff, '\n'))
}

// monothread read
// func ReadFile(filename string, chanOut chan []byte, chunk_size int) error {
// 	file, err := os.OpenFile(filename, os.O_RDONLY, 0o764)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()
// 	buff := make([]byte, chunk_size)
// 	var total_read int64
// 	for {
// 		n, _ := file.Read(buff)
// 		total_read += int64(n)
// 		cpyBuff := make([]byte, n)
// 		copy(cpyBuff, buff)
// 		chanOut <- cpyBuff
// 		if n < chunk_size {
// 			break
// 		}
// 	}
// 	return nil
// }

// if we want to split each line here, but it's not effective at all
// iterate over all \n, break when out of data
// for {
// 	if buff[buff_offset] == '\n' { // skip current '\n'
// 		buff_offset += 1
// 		if buff_offset >= buff_end_offset {
// 			break // we need to read more data
// 		}
// 	}
// 	//panic: runtime error: slice bounds out of range [524289:524288]
// 	if buff_offset >= buff_end_offset {
// 		break // we need to read more data
// 	}
// 	nextNl := nextNlOffsetBuff(buff[buff_offset:buff_end_offset])
// 	if nextNl == -1 { // we need to read more data
// 		break
// 	}
// 	// cpyBuff := make([]byte, nextNl)
// 	// copy(cpyBuff, buff[buff_offset:buff_offset+nextNl])
// 	// chanOut <- cpyBuff
// 	Calc(buff[buff_offset : buff_offset+nextNl])
// 	buff_offset += int64(nextNl) + 1
// }
// if t_offset_start+totalRead >= size { // not if end of file
// 	break
// }
// if buff_offset >= buff_end_offset {
// 	buff_offset = 0
// } else {
// 	copy(buff, buff[buff_offset:buff_end_offset]) // copy remaining data ot the start
// 	buff_offset = buff_end_offset - buff_offset
// }
