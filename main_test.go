package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"
)

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
			filesStr = append(filesStr, filename[:len(filename)-4])
		}
	}
	return filesStr
}

func testFile(tmpDirPath, file string, chunkSize, nThreads int) error {
	input := "samples/" + file + ".txt"
	expectedOutput := "samples/" + file + ".out"
	output := tmpDirPath + "/" + file + ".out"
	// compute samples/X.txt into tmp/x.out
	if err := Solve(input, output, chunkSize, nThreads); err != nil {
		return err
	}
	// compare samples/X.out with tmp/X.out
	expected, err := hashFile(expectedOutput)
	if err != nil {
		return err
	}
	computed, err := hashFile(output)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if expected != computed {
		return fmt.Errorf("Wrong output")
	}
	return nil
}

// TestSamples test all test cases in samples directory
func TestSamples(t *testing.T) {
	files := getSamples("samples")
	tmpDirPath := t.TempDir()
	for _, file := range files {
		for _, chunkSize := range []int{128, 129, 512, 1024 * 1024} {
			for _, nThreads := range []int{1, 2, 5, 16} {
				t.Run(fmt.Sprintf("File=%s, chunkSize=%d, mThreads=%d", file, chunkSize, nThreads), func(t *testing.T) {
					if err := testFile(tmpDirPath, file, chunkSize, nThreads); err != nil {
						t.Errorf("File=%s, chunkSize=%d, mThreads=%d: %s", file, chunkSize, nThreads, err.Error())
					}
				})
			}
		}
	}
}

// TestBig test the 1b inputs file
func TestBig(t *testing.T) {
	tmpDirPath := t.TempDir()
	if err := testFile(tmpDirPath, "data-1b", 1024*1024, runtime.NumCPU()); err != nil {
		t.Fatalf("%s", err.Error())
	}
}

// TestBig test the 1b inputs file without output check, for profiling
func TestBigProf(t *testing.T) {
	tmpDirPath := t.TempDir()
	file := "data-1b"
	input := "samples/" + file + ".txt"
	output := tmpDirPath + "/" + file + ".out"
	if err := Solve(input, output, 1024*1024*2, 2*runtime.NumCPU()); err != nil {
		t.Error(err)
	}
}

func TestByte(t *testing.T) {
	if findIndexOf([]byte{32, 48, 32, 47, 98, 99, ';', 10}, ';') != 6 {
		t.Errorf("fail 1")
	}
	if findIndexOf([]byte{32, 48, 32, 47, 98, 99, 10, ';'}, ';') != 7 {
		t.Errorf("fail 2")
	}
	if findIndexOf([]byte{';', 48, 32, 47, 98, 99, 10, 34}, ';') != 0 {
		t.Errorf("fail 3")
	}
	if findIndexOf([]byte{67, 48, 32, 47, 98, 99, 10, 89}, ';') != -1 {
		t.Errorf("fail 4")
	}
	index := findIndexOf([]byte{67, 48, 32, 47, 98, 99, 10, 89, 67, 48, 32, 47, 98, ';', 10, 89}, ';')
	if index != 13 {
		t.Errorf("fail 5: %d", index)
	}
}
