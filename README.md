# One Billion Row Challenge in Go

[1brc](https://github.com/gunnarmorling/1brc/tree/main)

## Result

> On my MacbookPro M2, 32Go of RAM, 12 cores, it uses only 25Mo of RAM, and takes only 4 seconds (15Go file size, 1 billion rows, 8926 unique stations)

## Test & Profile

```bash
# Some commands to tests and benchmarks
go test -run TestSamples
# You need to generate the 1 billion input + solution first for the big one
go test -run TestBig
go test -cpuprofile cpu.prof -memprofile mem.prof -bench .
pprof -web brc cpu.prof
pprof -web brc mem.prof

go build . && ./brc && go test -cpuprofile cpu.prof -memprofile mem.prof -bench . && pprof -web brc cpu.prof
```
