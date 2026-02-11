package brc

import (
	"fmt"
	"os"
	"syscall"
)

var chunkReadByteSize int = os.Getpagesize()

type FileReader interface {
	Open(filename string) error
	Close() error
	IsOpen() bool
	GetFilename() string
	GetSize() int64
	ReadChunk(buffer []byte, offset int64) (int64, error)
	Read() (int64, error)
	GetChunk(offset, size int64) ([]byte, int64)
}

type _FileCommonReader struct {
	filename string
	size     int64
	data     []byte
}

type FileDiskReader struct {
	_FileCommonReader
	file *os.File
	fd   int
}

type FileMmapReader struct {
	_FileCommonReader
}

func NewFileDiskReader() FileReader {
	return &FileDiskReader{}
}

func (fileReader *FileDiskReader) Open(filename string) error {
	if len(filename) == 0 {
		return fmt.Errorf("Empty filename")
	}
	if fileReader.file != nil {
		return fmt.Errorf("File already open")
	}
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("Can't open file: %v", err)
	}
	fileReader.filename = filename
	fileReader.file = file
	fileReader.fd = int(file.Fd())
	file.Seek(0, 0)
	size, _ := file.Seek(0, 2)
	fileReader.size = size
	return nil
}

func (fileReader *FileDiskReader) IsOpen() bool {
	return fileReader.file != nil
}

func (fileReader *FileDiskReader) GetSize() int64 {
	return fileReader.size
}

func (fileReader *FileDiskReader) GetFilename() string {
	return fileReader.filename
}

func (fileReader *FileDiskReader) ReadChunk(buffer []byte, offset int64) (int64, error) {
	if offset >= int64(fileReader.size) || len(buffer) == 0 {
		return 0, nil
	}
	n, err := syscall.Pread(fileReader.fd, buffer, offset)
	return int64(n), err
}

func (fileReader *FileDiskReader) Read() (int64, error) {
	if !fileReader.IsOpen() {
		return 0, fmt.Errorf("File is not open")
	}
	if fileReader.data != nil {
		return fileReader.size, nil
	}
	fileReader.data = make([]byte, fileReader.size)
	total := int64(0)
	for total < fileReader.size {
		n, err := syscall.Pread(fileReader.fd,
			fileReader.data[total:min(total+int64(chunkReadByteSize*64), fileReader.size)], total)
		total += int64(n)
		if err != nil {
			clear(fileReader.data)
			return 0, err
		}
	}
	return total, nil
}

func (fileReader *FileDiskReader) GetChunk(offset, size int64) ([]byte, int64) {
	if offset >= int64(fileReader.size) || offset >= int64(len(fileReader.data)) || size == 0 {
		return nil, 0
	}
	sizeToRead := min(offset+size, int64(len(fileReader.data)))
	return fileReader.data[offset:sizeToRead], sizeToRead - offset
}

func (fileReader *FileDiskReader) Close() error {
	if fileReader.file == nil {
		return fmt.Errorf("File already closed")
	}
	err := fileReader.file.Close()
	fileReader.fd = 0
	fileReader.size = 0
	return err
}

func NewFileMmapReader() FileReader {
	return &FileMmapReader{}
}

func (fileReader *FileMmapReader) Open(filename string) error {
	if len(filename) == 0 {
		return fmt.Errorf("Empty filename")
	}
	if fileReader.data != nil {
		return fmt.Errorf("File already open")
	}
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("Can't open file: %v", err)
	}
	defer file.Close()
	file.Seek(0, 0)
	size, _ := file.Seek(0, 2)
	fileReader.filename = filename
	fileReader.size = size
	mmapFile, err := syscall.Mmap(
		int(file.Fd()), 0, int(fileReader.size),
		syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return fmt.Errorf("Cannot mmap file: %v", err)
	}
	fileReader.data = mmapFile[:]
	// defer runtime.AddCleanup(fileReader, func(mmapFile *[]byte) {
	// 	if mmapFile != nil {
	// 		syscall.Munmap(*mmapFile)
	// 	}
	// }, &fileReader.data)
	return nil
}

// TODO: manage empty file correctly
func (fileReader *FileMmapReader) IsOpen() bool {
	return fileReader.data != nil
}

func (fileReader *FileMmapReader) GetSize() int64 {
	return fileReader.size
}

func (fileReader *FileMmapReader) GetFilename() string {
	return fileReader.filename
}

func (fileReader *FileMmapReader) ReadChunk(buffer []byte, offset int64) (int64, error) {
	if offset >= int64(fileReader.size) || len(buffer) == 0 {
		return 0, nil
	}
	sizeToRead := min(offset+int64(len(buffer)), fileReader.size)
	copy(buffer, fileReader.data[offset:sizeToRead])
	return sizeToRead - offset, nil
}

func (fileReader *FileMmapReader) Read() (int64, error) {
	if !fileReader.IsOpen() {
		return 0, fmt.Errorf("File is not open")
	}
	if fileReader.data != nil {
		return fileReader.size, nil
	}
	fileReader.data = make([]byte, fileReader.size)
	total := int64(0)
	tmpBuff := make([]byte, chunkReadByteSize*64)
	for total < fileReader.size {
		copy(tmpBuff, fileReader.data[total:min(total+int64(chunkReadByteSize*64), fileReader.size)])
		total += int64(chunkReadByteSize * 64)
	}
	return total, nil
}

func (fileReader *FileMmapReader) GetChunk(offset, size int64) ([]byte, int64) {
	if offset >= int64(fileReader.size) || size == 0 {
		return nil, 0
	}
	sizeToRead := min(offset+size, fileReader.size)
	return fileReader.data[offset:sizeToRead], sizeToRead - offset
}

func (fileReader *FileMmapReader) Close() error {
	if fileReader.GetSize() == 0 {
		return fmt.Errorf("File already closed")
	}
	if fileReader.data != nil {
		err := syscall.Munmap(fileReader.data)
		fileReader.size = 0
		//clear(fileReader.data)
		fileReader.data = nil
		if err != nil {
			return err
		}
	}
	return nil
}
