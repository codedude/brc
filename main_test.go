package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
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

func testFile(tmpDirPath, file string) bool {
	input := "samples/" + file + ".txt"
	expectedOutput := "samples/" + file + ".out"
	output := tmpDirPath + "/" + file + ".out"
	// compute samples/X.txt into tmp/x.out
	if err := Solve(input, output); err != nil {
		return false
	}
	// compare samples/X.out with tmp/X.out
	expected, err := hashFile(expectedOutput)
	if err != nil {
		fmt.Println(err)
		return false
	}
	computed, err := hashFile(output)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return expected == computed
}

// TestSamples test all test cases in samples directory
func TestSamples(t *testing.T) {
	files := getSamples("samples")
	tmpDirPath := t.TempDir()
	for _, file := range files {
		if !testFile(tmpDirPath, file) {
			fmt.Println("Fail " + file)
			t.Fail()
		}
	}
}

// TestBig test the 1b inputs file
func TestBig(t *testing.T) {
	tmpDirPath := t.TempDir()
	if !testFile(tmpDirPath, "data-1b") {
		t.Fail()
	}
}
