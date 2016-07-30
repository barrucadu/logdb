package logdb

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

// Write the given value to the file using little-endian byte order.
// If the file doesn't exist, it is created. If the file does exist,
// it is truncated. The contents of the file are synced to disk after
// the write.
func writeFile(path string, data interface{}) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, data); err != nil {
		return err
	}

	fsync(file)

	return nil
}

// Read data into the given pointer from the file using little-endian
// byte order.
func readFile(path string, data interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	if err := binary.Read(file, binary.LittleEndian, data); err != nil {
		return err
	}

	return nil
}

// Memory-map the given file.
func mmap(path string) (*os.File, []byte, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	if fi.IsDir() {
		// Should never happen if this function is called correctly.
		return nil, nil, errors.New("tried to mmap a directory")
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}

	bytes, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return f, bytes, err
}

// Close and delete a file.
func closeAndRemove(file *os.File) error {
	if err := file.Close(); err != nil {
		return err
	}
	return os.Remove(file.Name())
}
