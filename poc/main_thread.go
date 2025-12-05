// Try to read file with multiple thread using seek
// result => not better, sometimes even worse
package main

import (
	"fmt"
	"os"
	"sync"
	"syscall"
)

// looks like the best settings for parallel pread of big files
const FILE_CHUNK_SIZE = 1024 * 1024
const NUM_THREADS = 12

func run(args []string) int {
	if len(args) < 2 {
		fmt.Println("Usage: ./exe input")
		return 0
	}
	input := args[1]

	f, _ := os.OpenFile(input, os.O_RDONLY, 0o764)
	var size int64
	if info, err := f.Stat(); err == nil {
		size = info.Size()
	} else {
		return -1
	}
	fmt.Println("File size: ", size)
	f.Close()

	// f, _ = os.OpenFile(input, os.O_RDONLY, 0o764)
	// buffer := make([]byte, FILE_CHUNK_SIZE)
	// for {
	// 	n, err := f.Read(buffer)
	// 	if err != nil || n == 0 {
	// 		break
	// 	}
	// }

	var wg sync.WaitGroup
	for i := range NUM_THREADS {
		wg.Add(1)
		go func(offset int, blockSize int64) {
			defer wg.Done()
			inFs, _ := os.OpenFile(input, os.O_RDONLY, 0o764)
			defer inFs.Close()
			buff := make([]byte, FILE_CHUNK_SIZE)
			// inFs.Seek(int64(offset)*blockSize, 0)
			var totalRead int64 = 0
			for totalRead < blockSize-1 {
				// n, err := inFs.Read(buff)
				n, err := syscall.Pread(int(inFs.Fd()), buff, int64(offset)*blockSize)
				if err != nil {
					fmt.Println(err)
				}
				totalRead += int64(n)
				// fmt.Printf("T%d = %d\n", offset, totalRead)
			}
		}(i, size/NUM_THREADS)
	}
	wg.Wait()
	return 0
}

func main() {
	ret := run(os.Args)
	os.Exit(ret)
}
