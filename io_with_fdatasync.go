// +build linux

package logdb

import (
	"os"
	"syscall"
)

// Synchronise writes to a file descriptor.
func fsync(file *os.File) error {
	fd := int(file.Fd())
	return syscall.Fdatasync(fd)
}
