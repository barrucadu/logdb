package logdb

import (
	"errors"
	"fmt"
)

var (
	// ErrIDOutOfRange means that the requested ID is not present in the log.
	ErrIDOutOfRange = errors.New("log ID out of range")

	// ErrUnknownVersion means that the disk format version of an opened database is unknown.
	ErrUnknownVersion = errors.New("unknown disk format version")

	// ErrNotDirectory means that the path given to 'Open' exists and is not a directory.
	ErrNotDirectory = errors.New("database path not a directory")

	// ErrPathDoesntExist means that the path given to 'Open' does not exist and the 'create' flag was
	// false.
	ErrPathDoesntExist = errors.New("database directory does not exist")

	// ErrTooBig means that an entry could not be appended because it is larger than the chunk size.
	ErrTooBig = errors.New("entry larger than chunksize")

	// ErrClosed means that the database handle is closed.
	ErrClosed = errors.New("database is closed")

	// ErrEmptyNonfinalChunk means that the metadata for a non-final chunk has zero entries.
	ErrEmptyNonfinalChunk = errors.New("metadata of non-final chunk contains no entries")
)

// ReadError means that a read failed. It wraps the actual error.
type ReadError struct{ Err error }

func (e *ReadError) Error() string          { return e.Err.Error() }
func (e *ReadError) WrappedErrors() []error { return []error{e.Err} }

// WriteError means that a write failed. It wraps the actual error.
type WriteError struct{ Err error }

func (e *WriteError) Error() string          { return e.Err.Error() }
func (e *WriteError) WrappedErrors() []error { return []error{e.Err} }

// PathError means that a directory could not be created. It wraps the actual error.
type PathError struct{ Err error }

func (e *PathError) Error() string          { return e.Err.Error() }
func (e *PathError) WrappedErrors() []error { return []error{e.Err} }

// SyncError means that a file could not be synced to disk. It wraps the actual error.
type SyncError struct{ Err error }

func (e *SyncError) Error() string          { return e.Err.Error() }
func (e *SyncError) WrappedErrors() []error { return []error{e.Err} }

// DeleteError means that a file could not be deleted from disk. It wraps the actual error.
type DeleteError struct{ Err error }

func (e *DeleteError) Error() string          { return e.Err.Error() }
func (e *DeleteError) WrappedErrors() []error { return []error{e.Err} }

// LockError means that the database files could not be locked. It wraps the actual error.
type LockError struct{ Err error }

func (e *LockError) Error() string          { return e.Err.Error() }
func (e *LockError) WrappedErrors() []error { return []error{e.Err} }

// AtomicityError means that an error occurred while appending an entry in an 'AppendEntries' call, and
// attempting to rollback also gave an error. It wraps the actual errors.
type AtomicityError struct {
	AppendErr   error
	RollbackErr error
}

func (e *AtomicityError) Error() string {
	return fmt.Sprintf("error rolling back after append error: %s (%s)", e.RollbackErr.Error(), e.AppendErr.Error())
}

func (e *AtomicityError) WrappedErrors() []error {
	return []error{e.AppendErr, e.RollbackErr}
}

// FormatError means that there is a problem with the database files. It wraps the actual error.
type FormatError struct {
	FilePath string
	Err      error
}

func (e *FormatError) Error() string {
	return fmt.Sprintf("error reading file %s: %s", e.FilePath, e.Err.Error())
}

func (e *FormatError) WrappedErrors() []error {
	return []error{e.Err}
}

// ChunkFileNameError means that a filename is not valid for a chunk file.
type ChunkFileNameError struct {
	FilePath string
}

func (e *ChunkFileNameError) Error() string {
	return fmt.Sprintf("invalid chunk file name: %s", e.FilePath)
}

// ChunkSizeError means that a chunk file is not the expected size.
type ChunkSizeError struct {
	ChunkFilePath string
	Expected      uint32
	Actual        uint32
}

func (e *ChunkSizeError) Error() string {
	return fmt.Sprintf("in chunk %s: incorrect chunk file size (expected %v, got %v)", e.ChunkFilePath, e.Expected, e.Actual)
}

// ChunkContinuityError means that two adjacent chunks do not contain a contiguous sequence of entries.
type ChunkContinuityError struct {
	ChunkFilePath string
	Expected      uint64
	Actual        uint64
}

func (e *ChunkContinuityError) Error() string {
	return fmt.Sprintf("in chunk %s: discontinuity in entry IDs (expected %v, got %v)", e.ChunkFilePath, e.Expected, e.Actual)
}

// ChunkMetaError means that the metadata for a chunk could not be read. It wraps the actual error.
type ChunkMetaError struct {
	ChunkFilePath string
	Err           error
}

func (e *ChunkMetaError) Error() string {
	return fmt.Sprintf("error reading metadata for %s: %s", e.ChunkFilePath, e.Err.Error())
}

func (e *ChunkMetaError) WrappedErrors() []error {
	return []error{e.Err}
}

// MetaContinuityError means that the metadata for a chunk does not contain a contiguous sequence of entries.
type MetaContinuityError struct {
	Expected int32
	Actual   int32
}

func (e *MetaContinuityError) Error() string {
	return fmt.Sprintf("entry index too large (expected <=%v, got %v)", e.Expected, e.Actual)
}

// MetaOffsetError means that the metadata for a chunk does not contain a monotonically increasing sequence
// of entry ending offsets.
type MetaOffsetError struct {
	Expected int32
	Actual   int32
}

func (e *MetaOffsetError) Error() string {
	return fmt.Sprintf("entry offsets not monotonically increasing (expected >=%v, got %v)", e.Expected, e.Actual)
}
