package logdb

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

// Write the given value to the file using little-endian byte order.
// If 'create' is true, the file is created/truncated.
func writeFile(path string, data interface{}, create bool) error {
	flags := os.O_RDWR
	if create {
		flags = flags | os.O_CREATE | os.O_TRUNC
	}

	file, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, data); err != nil {
		return err
	}

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

	f, err := os.OpenFile(fi.Name(), syscall.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}

	bytes, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return f, bytes, err
}
