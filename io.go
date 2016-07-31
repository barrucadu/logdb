package logdb

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

// Create a new file with 0644 permissions and the given size,
// truncating it if it already exists.
func createFile(path string, size uint32) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	return syscall.Ftruncate(int(file.Fd()), int64(size))
}

// Write the given value to the file using little-endian byte order.
// If the file doesn't exist, it is created. If the file does exist,
// it is truncated. The contents of the file are synced to disk after
// the write.
func writeFile(path string, data interface{}) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.LittleEndian, data); err != nil {
		return err
	}

	return fsync(file)
}

// Read data into the given pointer from the file using little-endian
// byte order.
func readFile(path string, data interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return binary.Read(file, binary.LittleEndian, data)
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

// Open and lock a file.
func flock(path string) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return f, syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// Unlock and close a file.
func funlock(file *os.File) error {
	// No need to do a flock(LOCK_UN) call, as closing the fd also
	// releases the lock.
	return file.Close()
}
