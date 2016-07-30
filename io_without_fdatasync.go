// +build !linux

package logdb

import "os"

// Synchronise writes to a file descriptor.
func fsync(file os.File) error {
	return file.Sync()
}
