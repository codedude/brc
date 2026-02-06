# One Billion Row Challenge in Go

[One billion row challenge](https://github.com/gunnarmorling/1brc/tree/main)

## Result

> On my MacbookPro M2 pro, 32Go of RAM, 12 cores, it uses only 55Mo of RAM (chunk_size=2Mb, n_threads=24), and takes only 3.9 seconds (15Go file size, 8926 unique stations)

## Usage

```bash
Usage of ./brc:
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
# Generate the 1 billion line file (14.8Go)
git clone https://github.com/gunnarmorling/1brc.git
cd 1brc/src/main/python
# You must have a "python" in your path, it takes some minutes
./create_measurements.py
# Copy the output file in 1brc/data/measurments.txt to brc/samples/data1b.txt
```

```bash
# Some commands to tests and benchmarks
go test . -run TestSamples
# You need to generate the 1 billion input + solution first for the big one
go test . -run TestBigProf
go test -cpuprofile cpu.prof -memprofile mem.prof -bench .
pprof -web brc cpu.prof
pprof -web brc mem.prof
# Build + dry run first
go build . && ./brc -input samples/measurements-20.txt
go test . -cpuprofile cpu.prof -memprofile mem.prof -bench . && pprof -web brc cpu.prof
```
