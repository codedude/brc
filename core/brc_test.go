package brc

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const samplesRootDir = "../samples"

// hashFile return the sha1 of a file
func hashFile(filepath string) (string, error) {
	f, err := os.OpenFile(
		filepath,
		os.O_RDONLY,
		0o764)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// getSamples retrieve test cases (starting with measure and ending with .txt) in path folder
func getSamples(path string) []string {
	fs, _ := os.Open(path)
	fileInfos, _ := fs.Readdir(128)
	filesStr := make([]string, 0, 64)
	for _, file := range fileInfos {
		filename := file.Name()
		if len(filename) < 7 || filename[:7] != "measure" {
			continue
		}
		if filename[len(filename)-4:] == ".txt" {
			filesStr = append(filesStr, filepath.Join(path, filename))
		}
	}
	return filesStr
}

func testFile(tmpDirPath string, fileReader FileReader, file string, opts BrcOptions) error {
	expectedOutput := strings.Replace(file, ".txt", ".out", 1)
	filename := filepath.Base(file)
	output := filepath.Join(tmpDirPath, filename+".out")
	// compute samples/x.txt into tmp/x.out
	if err := Solve(fileReader, output, opts); err != nil {
		return err
	}
	// compare samples/x.out with tmp/x.out
	expected, err := hashFile(expectedOutput)
	if err != nil {
		return err
	}
	computed, err := hashFile(output)
	if err != nil {
		return err
	}
	if expected != computed {
		return fmt.Errorf("Wrong output")
	}
	return nil
}

// TestSamples test all test cases in samples directory
func TestSamples(t *testing.T) {
	files := getSamples(samplesRootDir)
	tmpDirPath := t.TempDir()
	for _, file := range files {
		for _, fileReaderFactory := range [](func() FileReader){NewFileDiskReader, NewFileMmapReader} {
			fileReader := fileReaderFactory()
			if err := fileReader.Open(file); err != nil {
			}
			defer fileReader.Close()
			if _, err := fileReader.Read(); err != nil {
				t.Fatalf("File=%s: %s", file, err.Error())
			}
			for _, strategy := range BrcStrategyList {
				for _, mode := range BrcReaderList {
					for _, chunkSize := range []int{1, 2, 3, 4, 5, 11, 64, 128} {
						for _, nThreads := range []int{1, 2, 3, 4, 5, 7, 12, 64} {
							opts := BrcOptions{
								ReadChunkFactor: chunkSize,
								NThreads:        nThreads,
								Strategy:        strategy,
								ReaderType:      mode,
								Verbose:         false,
							}
							if opts.Strategy == BrcStrategyPreRead {
								if _, err := fileReader.Read(); err != nil {
									t.Fatalf("File=%s: %s", file, err.Error())
								}
							}
							t.Run(fmt.Sprintf("File=%s, chunk=%d, threads=%d, mode=%s, strategy=%s",
								file, opts.ReadChunkFactor, opts.NThreads, string(opts.ReaderType), string(opts.Strategy)),
								func(t *testing.T) {
									if err := testFile(tmpDirPath, fileReader, file, opts); err != nil {
										t.Error(err.Error())
									}
								})
						}
					}
				}
			}
		}
	}
}

// TestBig test the 1b inputs file
func TestBigOnly(t *testing.T) {
	file := filepath.Join(samplesRootDir, "data-1b.txt")
	tmpDirPath := t.TempDir()
	fileReader := NewFileMmapReader()
	err := fileReader.Open(file)
	if err != nil {
		t.Fatalf("File=%s: %s", file, err.Error())
	}
	defer fileReader.Close()
	opts := BrcOptions{
		ReadChunkFactor: 64,
		NThreads:        runtime.NumCPU(),
		Strategy:        BrcStrategyLazyRead,
		ReaderType:      BrcReaderDisk,
		Verbose:         false,
	}
	if err := testFile(tmpDirPath, fileReader, file, opts); err != nil {
		t.Fatalf("%s", err.Error())
	}
}

// TestBig test the 1b inputs file without output check, for profiling
func TestPerfLazy(t *testing.T) {
	tmpDirPath := t.TempDir()
	file := "data-1b.txt"
	input := filepath.Join(samplesRootDir, file)
	output := filepath.Join(tmpDirPath, file+".out")
	fileReader := NewFileDiskReader()
	err := fileReader.Open(input)
	if err != nil {
		t.Fatalf("File=%s: %s", input, err.Error())
	}
	defer fileReader.Close()
	opts := BrcOptions{
		ReadChunkFactor: 64,
		NThreads:        runtime.NumCPU(),
		Strategy:        BrcStrategyLazyRead,
		ReaderType:      BrcReaderDisk,
		Verbose:         false,
	}
	if err := Solve(fileReader, output, opts); err != nil {
		t.Error(err)
	}
}

func TestPerfPreload(t *testing.T) {
	tmpDirPath := t.TempDir()
	file := "data-1b.txt"
	input := filepath.Join(samplesRootDir, file)
	output := filepath.Join(tmpDirPath, file+".out")
	fileReader := NewFileDiskReader()
	err := fileReader.Open(input)
	if err != nil {
		t.Fatalf("File=%s: %s", input, err.Error())
	}
	defer fileReader.Close()
	opts := BrcOptions{
		ReadChunkFactor: 64,
		NThreads:        runtime.NumCPU(),
		Strategy:        BrcStrategyPreRead,
		ReaderType:      BrcReaderDisk,
		Verbose:         false,
	}
	if err := Solve(fileReader, output, opts); err != nil {
		t.Error(err)
	}
}

func TestFindIndexOf(t *testing.T) {
	if id := findIndexOf([]byte{32, 48, 32, 47, 98, 99, ';', 10}, patternSemi); id != 6 {
		t.Errorf("fail 1: %d", id)
	}
	if id := findIndexOf([]byte{32, 48, 32, 47, 98, 99, 10, ';'}, patternSemi); id != 7 {
		t.Errorf("fail 2: %d", id)
	}
	if id := findIndexOf([]byte{';', 48, 32, 47, 98, 99, 10, 34}, patternSemi); id != 0 {
		t.Errorf("fail 3: %d", id)
	}
	if id := findIndexOf([]byte{67, 48, 32, 47, 98, 99, 10, 89}, patternSemi); id != -1 {
		t.Errorf("fail 4: %d", id)
	}
	if id := findIndexOf([]byte{67, 48, 32, 47, 98, 99, 10, 89, 67, 48, 32, 47, 98, ';', 10, 89}, patternSemi); id != 13 {
		t.Errorf("fail 5: %d", id)
	}
	if id := findIndexOf([]byte{67, 48, 32, 47, 98, 99, 10, 89, 67, 48, 32, 48, 32, 47, 98, ';', 10, 89}, patternSemi); id != 15 {
		t.Errorf("fail 6: %d", id)
	}
	if id := findIndexOf([]byte{67, 48, 32, 47, 98, ';'}, patternSemi); id != 5 {
		t.Errorf("fail 7: %d", id)
	}
	if id := findIndexOf([]byte{67, 48, 0, 47, 98, ';', ';', 45, 45, ';', 12}, patternSemi); id != 5 {
		t.Errorf("fail 8: %d", id)
	}
}

func TestFileDiskReader(t *testing.T) {
	fileName := filepath.Join(samplesRootDir, "measurements-10000-unique-keys.txt")
	fdr := NewFileDiskReader()
	err := fdr.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if fdr.GetFilename() != fileName {
		t.Fatal("Filename is empty")
	}
	if fdr.GetSize() == 0 {
		t.Fatal("Size is 0")
	}
	if !fdr.IsOpen() {
		t.Fatal("File should be opened")
	}
	buff := make([]byte, 32)
	n, err := fdr.ReadChunk(buff, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 32 {
		t.Fatalf("Should have read 32 bytes, got %d", n)
	}
	n, err = fdr.Read()
	if err != nil {
		t.Fatal(err)
	}
	if n < 32 {
		t.Fatal(err)
	}
	chunk, n := fdr.GetChunk(4, 32)
	if n == 0 {
		t.Fatalf("GetChunk should return 32 bytes, got %d", n)
	}
	if int64(len(chunk)) != n {
		t.Fatalf("GetChunk should return a %d bytes slices, got %d", n, len(chunk))
	}
	err = fdr.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !fdr.IsOpen() {
		t.Fatal("File should be closed")
	}
}

func TestFileMmapReader(t *testing.T) {
	fileName := filepath.Join(samplesRootDir, "measurements-10000-unique-keys.txt")
	fdr := NewFileDiskReader()
	err := fdr.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if fdr.GetFilename() != fileName {
		t.Fatal("Filename is empty")
	}
	if fdr.GetSize() == 0 {
		t.Fatal("Size is 0")
	}
	if !fdr.IsOpen() {
		t.Fatal("File should be opened")
	}
	buff := make([]byte, 32)
	n, err := fdr.ReadChunk(buff, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 32 {
		t.Fatalf("Should have read 32 bytes, got %d", n)
	}
	n, err = fdr.Read()
	if err != nil {
		t.Fatal(err)
	}
	if n < 32 {
		t.Fatal(err)
	}
	chunk, n := fdr.GetChunk(4, 32)
	if n == 0 {
		t.Fatalf("GetChunk should return 32 bytes, got %d", n)
	}
	if int64(len(chunk)) != n {
		t.Fatalf("GetChunk should return a %d bytes slices, got %d", n, len(chunk))
	}
	err = fdr.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !fdr.IsOpen() {
		t.Fatal("File should be closed")
	}
}
