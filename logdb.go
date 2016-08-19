// Package logdb provides an efficient log-structured database supporting efficient insertion of new entries
// and removal from either end of the log.
//
// This provides a number of interfaces and types, and a lot of errors.
//
//  - 'LogDB' is the main interface for a log-structured database.
//  - 'PersistDB' is an interface for databases which can be persisted in some way.
//  - 'BoundedDB' is an interface for databases with a fixed maximum entry size.
//  - 'CloseDB' is an interface for databases which can be closed.
//
// The 'LockFreeChunkDB' and 'ChunkDB' types implement all of these interfaces, and are created with 'Open'
// and 'WrapForConcurrency' respectively. As the names suggest, the difference is the thread-safety. A
// 'LockFreeChunkDB' is only safe for single-threaded access, where a 'ChunkDB' wraps it and adds locking, for
// safe concurrent access. Additionally, the 'InMemDB' type implements the 'LogDB' interface using a purely
// in-memory store.
package logdb

// A LogDB is a log-structured database.
type LogDB interface {
	// Append writes a new entry to the log and returns its ID.
	//
	// Returns 'WriteError' value if the database files could not be written to.
	Append(entry []byte) (uint64, error)

	// AppendEntries atomically writes a collection of new entries to the log and returns the ID of the
	// first (the IDs are contiguous). If the slice is empty or nil, the returned ID is meaningless.
	//
	// Returns the same errors as 'Append', and an 'AtomicityError' value if any entry fails to
	// append and rolling back the log failed.
	AppendEntries(entries [][]byte) (uint64, error)

	// Get looks up an entry by ID.
	//
	// Returns 'ErrIDOutOfRange' if the requested ID is lesser than the oldest or greater than the
	// newest.
	Get(id uint64) ([]byte, error)

	// Forget removes entries from the end of the log.
	//
	// If the new "oldest" ID is older than the current, this is a no-op.
	//
	// Returns 'ErrIDOutOfRange' if the ID is newer than the "newest" ID.
	Forget(newOldestID uint64) error

	// Rollback removes entries from the head of the log.
	//
	// If the new "newest" ID is newer than the current, this is a no-op.
	//
	// Returns the same errors as 'Forget', with 'ErrIDOutOfRange' being returned if the ID is older
	// than the "oldest" ID.
	Rollback(newNewestID uint64) error

	// Truncate performs a 'Forget' followed by a 'Rollback' atomically. The semantics are that if
	// the 'Forget' fails, the 'Rollback' is not performed; but the 'Forget' is not undone either.
	//
	// Returns the same errors as 'Forget' and 'Rollback', and also an 'ErrIDOutOfRange' if the new
	// newest < the new oldest.
	Truncate(newOldestID, newNewestID uint64) error

	// OldestID gets the ID of the oldest log entry.
	//
	// For an empty database, this will return 0.
	OldestID() uint64

	// NewestID gets the ID of the newest log entry.
	//
	// For an empty database, this will return 0.
	NewestID() uint64
}

// A PersistDB is a database which can be persisted in some fashion. In addition to defining methods, a
// 'PersistDB' which is also a 'LogDB' changes some existing behaviours:
//
//  - 'Append', 'AppendEntries', 'Forget', 'Rollback', and 'Truncate' can now cause a 'Sync', if
//    'SetSync' has been called.
//
//  - The above may return a 'SyncError' value if a periodic synchronisation failed.
type PersistDB interface {
	// SetSync configures the database to synchronise the data after touching (appending, forgetting,
	// or rolling back) at most this many entries.
	//
	// <0 disables periodic syncing, and 'Sync' must be called instead. The default value is 256.
	// Both 0 and 1 cause a 'Sync' after every write.
	//
	// Returns a 'SyncError' value if this triggered an immediate synchronisation which failed, and
	// 'ErrClosed' if the handle is closed.
	SetSync(every int) error

	// Sync persists the data now.
	//
	// May return a SyncError value, and 'ErrClosed' if the handle is closed.
	Sync() error
}

// A BoundedDB has a maximum entry size. A 'BoundedDB' which is also a 'LogDB' changes the behaviour of
// 'Append' and 'AppendEntries': they now return 'ErrTooBig' if an entry appended is larger than the maximum
// size.
type BoundedDB interface {
	// The maximum size of an entry. It is an error to try to insert an entry larger than this.
	MaxEntrySize() uint64
}

// A CloseDB can be closed, which may perform some clean-up.
//
// If a 'CloseDB' is also a 'PersistDB', then 'Sync' should be called during 'Close'. In addition, all
// 'LogDB' and 'PersistDB' with an error return will fail after calling 'Close', with 'ErrClosed'.
type CloseDB interface {
	// Close performs some database-specific clean-up. It is an error to try to use a database after
	// closing it.
	//
	// Returns 'ErrClosed' if the database is already closed.
	Close() error
}
