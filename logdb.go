// Package logdb provides an efficient log-structured database
// supporting efficient insertion of new entries, and efficient
// removal from either end of the log.
package logdb

import "errors"

var (
	// ErrIDOutOfRange means that the requested ID is not present
	// in the log.
	ErrIDOutOfRange = errors.New("log ID out of range")

	// ErrUnknownVersion means that the requested disk format
	// version is unknown.
	ErrUnknownVersion = errors.New("unknown disk format version")

	// ErrPathExists means that the path given to 'Create' or
	// 'Clone' already exists.
	ErrPathExists = errors.New("database directory already exists")

	// ErrPathDoesntExist means that the path given to 'Open' does
	// not exist.
	ErrPathDoesntExist = errors.New("database directory does not exist")

	// ErrCorrupt means that the database files are invalid.
	ErrCorrupt = errors.New("database corrupted")

	// Versions is a slice of all the supported disk format
	// versions. Generally the highest version should be used.
	Versions = []uint16{0}
)

// ReadError means that a read failed. It wraps the actual error.
type ReadError struct{ Err error }

func (e *ReadError) Error() string          { return e.Err.Error() }
func (e *ReadError) WrappedErrors() []error { return []error{e.Err} }

// WriteError means that a write failed. It wraps the actual error.
type WriteError struct{ Err error }

func (e *WriteError) Error() string          { return e.Err.Error() }
func (e *WriteError) WrappedErrors() []error { return []error{e.Err} }

// PathError means that a directory could not be created. It wraps the
// actual error.
type PathError struct{ Err error }

func (e *PathError) Error() string          { return e.Err.Error() }
func (e *PathError) WrappedErrors() []error { return []error{e.Err} }

// A LogDB is a reference to an efficient log-structured database
// providing ACID consistency guarantees.
type LogDB interface {
	// Append writes a new entry to the log.
	//
	// Returns a 'WriteError' value if the database files could
	// not be written to.
	Append(entry []byte) error

	// Get looks up an entry by ID.
	//
	// Returns 'ErrIDOutOfRange' if the requested ID is lesser
	// than the oldest or greater than the newest.
	Get(id uint64) ([]byte, error)

	// Forget removes entries from the end of the log.
	//
	// Returns 'ErrIDOutOfRange' if the new oldest ID is lesser
	// than the current oldest.
	Forget(newOldestID uint64) error

	// Rollback removes entries from the head of the log.
	//
	// Returns 'ErrIDOutOfRange' if the new newest ID is greater
	// than the current newest.
	Rollback(newNewestID uint64) error

	// Clone copies a database to a new path, using the given
	// format version and chunk size. This may be more efficient
	// than simply copying every entry.
	//
	// Returns the same errors as 'Create'.
	Clone(path string, version uint16, chunkSize uint32) (LogDB, error)

	// OldestID gets the ID of the oldest log entry.
	OldestID() uint64

	// NewestID gets the ID of the newest log entry.
	NewestID() uint64
}

// Create makes a new database with the given format version.
//
// The log is stored on disk in fixed-size files, controlled by the
// 'chunkSize' parameter. If entries are a fixed size, the chunk size
// should be a multiple of that to avoid wasting space. There is a
// trade-off to be made: a chunk is only deleted when its entries do
// not overlap with the live entries at all (this happens through
// calls to 'Forget' and 'Rollback'), so a larger chunk size means
// fewer files, but longer persistence.
//
// Returns 'ErrUnknownVersion' if the version number is not valid, a
// 'PathError' value if the directory could not be created, and
// 'ErrPathExists' if the directory already exists.
func Create(path string, version uint16, chunkSize uint32) (LogDB, error) {
	panic("unimplemented")
}

// Open opens the database in the given path, detecting the format
// version automatically.
//
// Returns 'ErrPathDoesntExist' if the directory does not exist, a
// 'ReadError' value if the directory cannot be read,
// 'ErrUnknownVersion' if the detected version is not valid, and
// 'ErrCorrupt' if the database could be understood.
func Open(path string) (LogDB, error) {
	panic("unimplemented")
}
