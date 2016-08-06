// Package logdb provides an efficient log-structured database supporting efficient insertion of new entries,
// and efficient removal from either end of the log.
package logdb

import (
	"io/ioutil"
	"os"
	"sort"
	"sync"
)

const latestVersion = uint16(0)

////////// LOG-STRUCTURED DATABASE //////////

// A LogDB is a reference to an efficient log-structured database providing ACID consistency guarantees.
type LogDB struct {
	// Path to the database directory.
	path string

	// Lock file used to prevent multiple simultaneous open handles: concurrent use of one handle is fine,
	// multiple handles is not. This file is locked exclusive, not shared.
	lockfile *os.File

	// Flag indicating that the handle has been closed. This is used to give 'ErrClosed' errors.
	closed bool

	// Any number of goroutines can read simultaneously, but when entries are added or removed, the write
	// lock must be held.
	//
	// An alternative design point to consider is to have a separate 'RWMutex' in each chunk, and hold only
	// the necessary write locks. This would complicate locking but allow for more concurrent reading, and
	// so may be better under some work loads.
	rwlock sync.RWMutex

	// Concurrent syncing/reading is safe, but syncing/writing and syncing/syncing is not. To prevent the
	// first, syncing claims a read lock. To prevent the latter, a special sync lock is used. Claiming a
	// write lock would also work, but is far more heavyweight.
	slock sync.Mutex

	// Size of individual chunks. Entries are not split over chunks, and so they cannot be bigger than this.
	chunkSize uint32

	// Chunks, in order.
	chunks []*chunk

	// Oldest and entry IDs. The db oldest may be > the first chunk oldest if forgetting has happened.
	oldest uint64

	// Data syncing: 'syncEvery' is how many changes (entries appended/truncated) to allow before syncing,
	// 'sinceLastSync' keeps track of this, and 'syncDirty' is the set of chunks to sync. When syncing,
	// first chunks are deleted newest-first, then data is flushed oldest-first. This is to maintain
	// consistency,
	syncEvery     int
	sinceLastSync uint64
	syncDirty     map[*chunk]struct{}
}

// Open a database.
//
// It is not possible to have multiple open references to the same database, as the files are locked. Concurrent
// usage of one open handle in a single process is safe.
//
// The log is stored on disk in fixed-size files, controlled by the 'chunkSize' parameter. Entries are not split
// over chunks, and so if entries are a fixed size, the chunk size should be a multiple of that to avoid wasting
// space. Furthermore, no entry can be larger than the chunk size. There is a trade-off to be made: a chunk is
// only deleted when its entries do not overlap with the live entries at all (this happens through calls to
// 'Forget' and 'Rollback'), so a larger chunk size means fewer files, but longer persistence.
//
// If the 'create' flag is true and the database doesn't already exist, the database is created using the given
// chunk size. If the database does exist, the chunk size parameter is ignored, and detected automatically from
// the chunk files.
func Open(path string, chunkSize uint32, create bool) (*LogDB, error) {
	// Check if it already exists.
	if stat, _ := os.Stat(path); stat != nil {
		if !stat.IsDir() {
			return nil, ErrNotDirectory
		}
		return opendb(path)
	}
	if create {
		return createdb(path, chunkSize)
	}
	return nil, ErrPathDoesntExist
}

// Append writes a new entry to the log.
//
// Returns 'ErrTooBig' if the entry is larger than the chunk size, a 'WriteError' value if the database files
// could not be written to, a 'SyncError' value if a periodic synchronisation failed, and 'ErrClosed' if the
// handle is closed.
func (db *LogDB) Append(entry []byte) error {
	return db.AppendEntries([][]byte{entry})
}

// AppendEntries atomically writes a collection of new entries to the log.
//
// Returns the same errors as 'Append', and an 'AtomicityError' value if any entry fails to append and rolling
// back the log failed.
func (db *LogDB) AppendEntries(entries [][]byte) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	if db.closed {
		return ErrClosed
	}

	originalNewest := db.next() - 1

	var appended bool
	for _, entry := range entries {
		if err := db.append(entry); err != nil {
			// Rollback on error if we've already appended some entries.
			if appended {
				if rerr := db.rollback(originalNewest); rerr != nil {
					return &AtomicityError{AppendErr: err, RollbackErr: rerr}
				}
			}
			return err
		}
		appended = true
	}

	return db.periodicSync()
}

// Get looks up an entry by ID.
//
// Returns 'ErrIDOutOfRange' if the requested ID is lesser than the oldest or greater than the newest, and
// 'ErrClosed' if the handle is closed.
func (db *LogDB) Get(id uint64) ([]byte, error) {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Check ID is in range.
	if id < db.oldest || id >= db.next() || len(db.chunks) == 0 {
		return nil, ErrIDOutOfRange
	}

	// Binary search through chunks for the one containing the ID.
	lo := 0
	hi := len(db.chunks)
	mid := hi / 2
	for ; !(db.chunks[mid].oldest <= id && id < db.chunks[mid].next()); mid = (hi + lo) / 2 {
		if hi < lo {
			panic("hi < lo")
		}
		if db.chunks[mid].next() <= id {
			lo = mid + 1
		} else if db.chunks[mid].oldest > id {
			hi = mid - 1
		}
	}

	// Calculate the start and end offset, and return a copy of the relevant byte slice.
	chunk := db.chunks[mid]
	off := id - chunk.oldest
	start := int32(0)
	if off > 0 {
		start = chunk.ends[off-1]
	}
	end := chunk.ends[off]
	out := make([]byte, end-start)
	for i := start; i < end; i++ {
		out[i-start] = chunk.bytes[i]
	}
	return out, nil
}

// Forget removes entries from the end of the log.
//
// If the new "oldest" ID is older than the current, this is a no-op.
//
// Returns a 'SyncError' value if this caused a periodic sync which failed, 'ErrIDOutOfRange' if the ID is
// newer than the "newest" ID, and 'ErrClosed' if the handle is closed.
func (db *LogDB) Forget(newOldestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.forget(newOldestID)
}

// Rollback removes entries from the head of the log.
//
// If the new "newest" ID is newer than the current, this is a no-op.
//
// Returns the same errors as 'Forget', with 'ErrIDOutOfRange' being returned if the ID is older than the
// "oldest" ID.
func (db *LogDB) Rollback(newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.rollback(newNewestID)
}

// Truncate performs a 'Forget' followed by a 'Rollback' atomically. The semantics are that if the 'Forget'
// fails, the 'Rollback' is not performed; but the 'Forget' is not undone either.
//
// Returns the same errors as 'Forget' and 'Rollback', and also an 'ErrIDOutOfRange' if the new newest < the
// new oldest.
func (db *LogDB) Truncate(newOldestID, newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	if db.closed {
		return ErrClosed
	}
	if newNewestID < newOldestID {
		return ErrIDOutOfRange
	}
	if err := db.forget(newOldestID); err != nil {
		return err
	}
	return db.rollback(newNewestID)
}

// SetSync configures the database to synchronise the data to disk after touching (appending, forgetting, or
// rolling back) at most this many entries. Data is always synced if an entire chunk is forgotten or rolled
// back.
//
// <0 disables periodic syncing, and 'Sync' must be called instead. The default value is 256. Both 0 and 1 cause
// a 'Sync' after every write.
//
// Returns a 'SyncError' value if this triggered an immediate synchronisation which failed, and 'ErrClosed' if
// the handle is closed.
func (db *LogDB) SetSync(every int) error {
	db.syncEvery = every

	// Immediately perform a periodic sync.
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return db.periodicSync()
}

// Sync writes the data to disk now.
//
// May return a SyncError value, and 'ErrClosed' if the handle is closed.
func (db *LogDB) Sync() error {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return db.sync()
}

// OldestID gets the ID of the oldest log entry.
//
// For an empty database, this will return 0.
func (db *LogDB) OldestID() uint64 {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	return db.oldest
}

// NewestID gets the ID of the newest log entry.
//
// For an empty database, this will return 0.
func (db *LogDB) NewestID() uint64 {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	return db.next() - 1
}

// Close synchronises the database to disk and closes all open files. It is an error to try to use a database
// after closing it.
//
// May return a 'SyncError' value, and 'ErrClosed' if the handle is already closed.
func (db *LogDB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	if db.closed {
		return ErrClosed
	}

	// First sync everything
	err := db.sync()

	// Then close the open files
	for _, c := range db.chunks {
		_ = c.mmapf.Close()
	}

	// Then release the lock
	funlock(db.lockfile)

	// Mark the databse as closed, so any further attempts to use
	// this handle will be errored.
	db.closed = true

	return err
}

////////// HELPERS //////////

// Create a database. It is an error to call this function if the database directory already exists.
func createdb(path string, chunkSize uint32) (*LogDB, error) {
	// Create the directory.
	if err := os.MkdirAll(path, os.ModeDir|0755); err != nil {
		return nil, &PathError{err}
	}

	// Write the version file
	if err := writeFile(path+"/version", latestVersion); err != nil {
		return nil, &WriteError{err}
	}

	// Lock the "version" file.
	lockfile, err := flock(path + "/version")
	if err != nil {
		return nil, &LockError{err}
	}

	// Write the chunk size file
	if err := writeFile(path+"/chunk_size", chunkSize); err != nil {
		return nil, &WriteError{err}
	}

	// Write the "oldest" file.
	if err := writeFile(path+"/oldest", uint64(0)); err != nil {
		return nil, &WriteError{err}
	}

	return &LogDB{
		path:      path,
		closed:    false,
		lockfile:  lockfile,
		chunkSize: chunkSize,
		syncEvery: 256,
		syncDirty: make(map[*chunk]struct{}),
	}, nil
}

// Open an existing database. It is an error to call this function if the database directory does not exist.
func opendb(path string) (*LogDB, error) {
	// Read the "version" file.
	var version uint16
	if err := readFile(path+"/version", &version); err != nil {
		return nil, &ReadError{err}
	}

	// Check the version.
	if version != 0 {
		return nil, ErrUnknownVersion
	}

	// Lock the "version" file.
	lockfile, err := flock(path + "/version")
	if err != nil {
		return nil, &LockError{err}
	}

	// Read the "chunk_size" file.
	var chunkSize uint32
	if err := readFile(path+"/chunk_size", &chunkSize); err != nil {
		return nil, &ReadError{err}
	}

	// Get all the chunk files.
	var chunkFiles []os.FileInfo
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, &ReadError{err}
	}
	for _, fi := range fis {
		if !fi.IsDir() && isBasenameChunkDataFile(fi.Name()) {
			chunkFiles = append(chunkFiles, fi)
		}
	}

	sort.Sort(fileInfoSlice(chunkFiles))

	// Populate the chunk slice.
	chunks := make([]*chunk, len(chunkFiles))
	var prior *chunk
	var empty bool
	for i, fi := range chunkFiles {
		// Normally a chunk contains at least one entry. This may only false for the final chunk. So if
		// we have a chunk file to process and the 'empty' flag is set, then we have an error.
		if empty {
			return nil, &FormatError{
				FilePath: prior.metaFilePath(),
				Err:      ErrEmptyNonfinalChunk,
			}
		}

		c, err := openChunkFile(path, fi, prior, chunkSize)
		if err != nil {
			return nil, err
		}
		chunks[i] = &c
		prior = &c
		empty = len(c.ends) == 0
	}

	// If we cannot read the "oldest" file OR the oldest entry according to the metadata is older than the
	// oldest entry we actually have, bump it up to the newer one. This could happen if a chunk is forgotten
	// and then the program crashes before the "oldest" file gets rewritten.
	var oldest uint64
	if err := readFile(path+"/oldest", &oldest); err != nil || (len(chunks) > 0 && oldest < chunks[0].oldest) {
		oldest = chunks[0].oldest
	}

	return &LogDB{
		path:      path,
		closed:    false,
		lockfile:  lockfile,
		chunkSize: chunkSize,
		chunks:    chunks,
		oldest:    oldest,
		syncEvery: 100,
		syncDirty: make(map[*chunk]struct{}),
	}, nil
}

// Return the 'next' value of the last chunk. Assumes a read lock is held.
func (db *LogDB) next() uint64 {
	if len(db.chunks) == 0 {
		return 1
	}
	return db.chunks[len(db.chunks)-1].next()
}

// Append an entry to the database, creating a new chunk if necessary, and incrementing the dirty counter.
// Assumes a write lock is held.
func (db *LogDB) append(entry []byte) error {
	if uint32(len(entry)) > db.chunkSize {
		return ErrTooBig
	}

	// If there are no chunks, create a new one.
	if len(db.chunks) == 0 {
		if err := db.newChunk(); err != nil {
			return &WriteError{err}
		}
	}

	lastChunk := db.chunks[len(db.chunks)-1]

	// If the last chunk doesn't have the space for this entry, create a new one.
	if len(lastChunk.ends) > 0 {
		lastEnd := lastChunk.ends[len(lastChunk.ends)-1]
		if db.chunkSize-uint32(lastEnd) < uint32(len(entry)) {
			if err := db.newChunk(); err != nil {
				return &WriteError{err}
			}
			lastChunk = db.chunks[len(db.chunks)-1]
		}
	}

	// Add the entry to the last chunk
	var start int32
	if len(lastChunk.ends) > 0 {
		start = lastChunk.ends[len(lastChunk.ends)-1]
	}
	end := start + int32(len(entry))
	for i, b := range entry {
		lastChunk.bytes[start+int32(i)] = b
	}
	lastChunk.ends = append(lastChunk.ends, end)

	// If this is the first entry ever, set the oldest ID to 1 (IDs start from 1, not 0)
	if db.oldest == 0 {
		db.oldest = 1
	}

	// Mark the current chunk as dirty.
	db.sinceLastSync++
	db.syncDirty[lastChunk] = struct{}{}
	return nil
}

// Adds a new chunk to the database. Assumes a write lock is held.
//
// A chunk cannot be empty, so it is only valid to call this if an entry is going to be inserted into the chunk
// immediately.
func (db *LogDB) newChunk() error {
	// As the chunk oldest ID is stored in the filename, we need to sync the prior chunk before creating the
	// new one. Otherwise if the process dies before the next sync, there will be a chunk ID discontinuity.
	if len(db.chunks) > 0 {
		if err := db.syncOne(db.chunks[len(db.chunks)-1]); err != nil {
			return err
		}
	}

	chunkFile := db.path + "/" + initialChunkFile

	// Filename is "chunk-<1 + last chunk file name>_<next id>"
	if len(db.chunks) > 0 {
		chunkFile = db.path + "/" + db.chunks[len(db.chunks)-1].nextDataFileName(db.next())
	}

	// Create the files for a new chunk.
	err := createChunkFiles(chunkFile, db.chunkSize, db.next())
	if err != nil {
		return err
	}

	// Open the newly-created chunk file.
	fi, err := os.Stat(chunkFile)
	if err != nil {
		return err
	}
	var prior *chunk
	if len(db.chunks) > 0 {
		prior = db.chunks[len(db.chunks)-1]
	}
	c, err := openChunkFile(db.path, fi, prior, db.chunkSize)
	if err != nil {
		return err
	}
	db.chunks = append(db.chunks, &c)

	return nil
}

// Remove entries from the beginning of the log, performing a sync if necessary. Assumes a write lock is held.
func (db *LogDB) forget(newOldestID uint64) error {
	if newOldestID < db.oldest {
		return nil
	}

	if newOldestID >= db.next() {
		return ErrIDOutOfRange
	}

	db.sinceLastSync += newOldestID - db.oldest
	db.oldest = newOldestID

	// Mark too-old chunks for deletion.
	var first int
	for first = 0; first < len(db.chunks) && db.chunks[first].next() <= newOldestID; first++ {
		c := db.chunks[first]
		db.syncDirty[c] = struct{}{}
		c.delete = true
	}

	// If this deleted any chunks, perform a sync.
	if first > 0 {
		if err := db.sync(); err != nil {
			return err
		}
		db.chunks = db.chunks[first:]
	}

	// Perform a periodic sync.
	return db.periodicSync()
}

// Remove entries from the end of the log, performing a sync if necessary. Assumes a write lock is held.
func (db *LogDB) rollback(newNewestID uint64) error {
	newNextID := newNewestID + 1

	if newNextID > db.next() {
		return nil
	}

	if newNextID <= db.oldest {
		return ErrIDOutOfRange
	}

	db.sinceLastSync += db.next() - newNextID

	// Update chunk metadata and mark too-new chunks for deletion.
	var last int
	for last = len(db.chunks) - 1; last >= 0; last-- {
		c := db.chunks[last]
		db.syncDirty[c] = struct{}{}
		if newNextID <= c.oldest {
			c.ends = nil
			c.delete = true
		} else {
			toRemove := c.next() - newNextID
			c.ends = c.ends[0 : uint64(len(c.ends))-toRemove]
			if len(c.ends) < c.newFrom {
				// Force the new last entry to be written out again.
				c.newFrom = len(c.ends) - 1
			}
			break
		}
	}
	last++

	// If this deleted any chunks, perform a sync.
	if last < len(db.chunks) {
		if err := db.sync(); err != nil {
			return err
		}
		db.chunks = db.chunks[:last]
	}

	// Perform a periodic sync
	return db.periodicSync()
}

// Perform a sync only if needed. Assumes a lock (read or write) is held.
func (db *LogDB) periodicSync() error {
	if db.syncEvery >= 0 && db.sinceLastSync > uint64(db.syncEvery) {
		return db.sync()
	}
	return nil
}

// Perform a sync immediately. Assumes a lock (read or write) is held.
func (db *LogDB) sync() error {
	db.slock.Lock()
	defer db.slock.Unlock()

	// Produce a sorted list of chunks to sync.
	dirtyChunks := make([]*chunk, len(db.syncDirty))
	var i int
	for c, _ := range db.syncDirty {
		dirtyChunks[i] = c
		i++
	}
	sort.Sort(sort.Reverse(chunkSlice(dirtyChunks)))

	// First handle deletions. As the slice is sorted in reverse order, this will delete newest-first. This
	// avoids next/oldest inconsistencies: if chunk N+1 and some entries in chunk N are deleted, then some
	// smaller entries are written into chunk N, the "next" of chunk N might be greater than the "oldest" of
	// chunk N+1. By deleting first, we avoid this situation.
	var toSync []*chunk
	for _, c := range dirtyChunks {
		if c.delete {
			if err := c.closeAndRemove(); err != nil {
				return &SyncError{&DeleteError{err}}
			}
		} else {
			toSync = append([]*chunk{c}, toSync...)
		}
	}
	for _, c := range toSync {
		if err := c.sync(); err != nil {
			return &SyncError{err}
		}
	}

	// Write the oldest entry ID.
	if err := writeFile(db.path+"/oldest", db.oldest); err != nil {
		return &SyncError{err}
	}

	db.syncDirty = make(map[*chunk]struct{})
	db.sinceLastSync = 0

	return nil
}

// Sync a single chunk and remove it from the dirty map.
//
// This does not update the sinceLastSync parameter, so the next sync will be slightly too early (which
// shouldn't be a problem for correctness, but isn't so great for performance).
func (db *LogDB) syncOne(c *chunk) error {
	_, ok := db.syncDirty[c]
	if !ok {
		return nil
	}

	if err := c.sync(); err != nil {
		return &SyncError{err}
	}

	delete(db.syncDirty, c)

	return nil
}
