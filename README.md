# One Billion Row Challenge in Go

[One billion row challenge](https://github.com/gunnarmorling/1brc/tree/main)

## Result

> On my MacbookPro M2 pro, 32Go of RAM, 12 cores, it uses only 55Mo of RAM (chunk_size=2Mb, n_threads=24), and takes only 3.9 seconds (15Go file size, 8926 unique stations)

## Usage

```bash
Usage of ./core:
  -chunk_size int
        Chunk size per read [128-2147483647] (default 1048576)
  -input string
        Input file path
  -n_threads int
        Max number of threads to use [1-1024] (default 12)
  -verbose
        If off, not output on stdout
Default output: ./output/[input].out
```

## Test & Profile

```bash
# The acceptance test
go test ./core -run TestSamples
# You need to generate the 1 billion input + solution first for the big one
go test ./core -run TestBigOnly
go test ./core -run TestPerfLazy
go test ./core -run TestPerfPreload
# Profiling (must be call with the 1 billion row file, else it's too fast)
go build . && go test ./core -cpuprofile cpu.pprof -memprofile mem.pprof -bench ./core -benchmem
pprof -web brc cpu.pprof
pprof -web brc mem.pprof
# With incode profiling
go build . && ./brc -input samples/measurements-10000-unique-keys.txt
go tool pprof -http=":8000" brc cpu.pprof
```
