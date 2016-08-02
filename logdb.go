// Package logdb provides an efficient log-structured database
// supporting efficient insertion of new entries, and efficient
// removal from either end of the log.
package logdb

import (
	"errors"
	"io/ioutil"
	"os"
	"sort"
	"sync"
)

const latestVersion = uint16(0)

////////// LOG-STRUCTURED DATABASE //////////

// A LogDB is a reference to an efficient log-structured database
// providing ACID consistency guarantees.
type LogDB struct {
	path string

	lockfile *os.File

	// Any number of goroutines can read simultaneously, but when
	// entries are added or removed, the write lock must be held.
	//
	// An alternative design point to consider is to have a
	// separate 'RWMutex' in each chunk, and hold only the
	// necessary write locks. This would complicate locking but
	// allow for more concurrent reading, and so may be better
	// under some work loads.
	rwlock sync.RWMutex

	// Concurrent syncing/reading is safe, but syncing/writing and
	// syncing/syncing is not. To prevent the first, syncing
	// claims a read lock. To prevent the latter, a special sync
	// lock is used. Claiming a write lock would also work, but is
	// far more heavyweight.
	slock sync.Mutex

	chunkSize uint32

	chunks []*chunk

	oldest uint64

	next uint64

	syncEvery     int
	sinceLastSync uint64
	syncDirty     map[*chunk]struct{}
}

// Open a database.
//
// It is not safe to have multiple open references to the same
// database at the same time, across any number of processes.
// Concurrent usage of one open reference in a single process is safe.
//
// If the 'create' flag is true and the database doesn't already
// exist, the database is created using the given chunk size. If the
// database does exist, the chunk size is detected automatically.
//
// The log is stored on disk in fixed-size files, controlled by the
// 'chunkSize' parameter. If entries are a fixed size, the chunk size
// should be a multiple of that to avoid wasting space. There is a
// trade-off to be made: a chunk is only deleted when its entries do
// not overlap with the live entries at all (this happens through
// calls to 'Forget' and 'Rollback'), so a larger chunk size means
// fewer files, but longer persistence.
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
// Returns a 'WriteError' value if the database files could not be
// written to, and a 'SyncError' value if a periodic synchronisation
// failed.
func (db *LogDB) Append(entry []byte) error {
	return db.AppendEntries([][]byte{entry})
}

// Atomically write a collection of new entries to the log.
//
// Returns the same errors as 'Append', and an 'AtomicityError' value
// if any entry fails to append and rolling back the log failed.
func (db *LogDB) AppendEntries(entries [][]byte) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	originalNewest := db.NewestID()

	for _, entry := range entries {
		if err := db.append(entry); err != nil {
			// Rollback on error.
			if rerr := db.truncate(db.oldest, originalNewest); rerr != nil {
				return &AtomicityError{AppendErr: err, RollbackErr: rerr}
			}
			return err
		}
	}

	return db.periodicSync()
}

// Get looks up an entry by ID.
//
// Returns 'ErrIDOutOfRange' if the requested ID is lesser than the
// oldest or greater than the newest.
func (db *LogDB) Get(id uint64) ([]byte, error) {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	// Check ID is in range.
	if id < db.oldest || id >= db.next || len(db.chunks) == 0 {
		return nil, ErrIDOutOfRange
	}

	// Binary search through chunks for the one containing the ID.
	lo := 0
	hi := len(db.chunks)
	mid := hi / 2
	for ; !(db.chunks[mid].oldest <= id && id < db.chunks[mid].next); mid = (hi + lo) / 2 {
		if db.chunks[mid].next <= id {
			lo = mid + 1
		} else if db.chunks[mid].oldest > id {
			hi = mid - 1
		}
	}

	// Calculate the start and end offset, and return a copy of
	// the relevant byte slice.
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
// Returns 'ErrIDOutOfRange' if the new "oldest" ID is lesser than the
// current oldest, a 'DeleteError' if a chunk file could not be
// deleted, and a 'SyncError' value if a periodic synchronisation
// failed.
func (db *LogDB) Forget(newOldestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	return db.truncate(newOldestID, db.NewestID())
}

// Rollback removes entries from the head of the log.
//
// Returns 'ErrIDOutOfRange' if the new "newest" ID is greater than
// the current next, a 'DeleteError' if a chunk file could not be
// deleted, and a 'SyncError' value if a periodic synchronisation
// failed.
func (db *LogDB) Rollback(newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	return db.truncate(db.OldestID(), newNewestID)
}

// Perform a combination 'Forget'/'Rollback' operation, this is
// atomic.
//
// Returns the same errors as 'Forget' and 'Rollback'.
func (db *LogDB) Truncate(newOldestID, newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	return db.truncate(newOldestID, newNewestID)
}

// SetSync configures the database to synchronise the data to disk
// after touching (appending, forgetting, or rolling back) at most
// this many entries. Data is always synced if an entire chunk is
// forgotten or rolled back.
//
// <0 disables periodic syncing, and 'Sync' must be called instead.
// The default value is 256.
//
// Returns a 'SyncError' value if this triggered an immediate
// synchronisation, which failed.
func (db *LogDB) SetSync(every int) error {
	db.syncEvery = every

	// Immediately perform a periodic sync.
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()
	return db.periodicSync()
}

// Sync writes the data to disk now.
//
// May return a SyncError value.
func (db *LogDB) Sync() error {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()
	return db.sync()
}

// OldestID gets the ID of the oldest log entry.
//
// For an empty database, this will return 0.
func (db *LogDB) OldestID() uint64 {
	return db.oldest
}

// NewestID gets the ID of the newest log entry.
//
// For an empty database, this will return 0.
func (db *LogDB) NewestID() uint64 {
	return db.next - 1
}

// Sync the database and close any open files. It is an error to try
// to use a database after closing it.
//
// May return a 'SyncError' value.
func (db *LogDB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	// First sync everything
	err := db.sync()

	// Then close the open files
	for _, c := range db.chunks {
		_ = c.mmapf.Close()
	}

	// Then release the lock
	funlock(db.lockfile)

	// Nuke the state, so that any further attempts to use this
	// handle will die quickly.
	db.lockfile = nil
	db.chunks = nil
	db.syncDirty = nil

	return err
}

////////// HELPERS //////////

// Create a database. It is an error to call this function if the
// database directory already exists.
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
		lockfile:  lockfile,
		chunkSize: chunkSize,
		syncEvery: 256,
		next:      1,
		syncDirty: make(map[*chunk]struct{}),
	}, nil
}

// Open an existing database. It is an error to call this function if
// the database directory does not exist.
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

	// Read the "oldest" file.
	var oldest uint64
	if err := readFile(path+"/oldest", &oldest); err != nil {
		return nil, &ReadError{err}
	}

	// Get all the chunk files.
	var chunkFiles []os.FileInfo
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, &ReadError{err}
	}
	for _, fi := range fis {
		if !fi.IsDir() && isChunkDataFile(fi) {
			chunkFiles = append(chunkFiles, fi)
		}
	}

	sort.Sort(fileInfoSlice(chunkFiles))

	// Populate the chunk slice.
	chunks := make([]*chunk, len(chunkFiles))
	next := uint64(1)
	var prior *chunk
	var empty bool
	for i, fi := range chunkFiles {
		// Normally a chunk contains at least one entry. This
		// may only false for the final chunk. So if we have a
		// chunk file to process and the 'empty' flag is set,
		// then we have an error.
		if empty {
			return nil, &FormatError{
				FilePath: metaFilePath(prior),
				Err:      errors.New("metadata of non-final chunk contains no entries"),
			}
		}

		c, err := openChunkFile(path, fi, prior, chunkSize)
		if err != nil {
			return nil, err
		}
		chunks[i] = &c
		prior = &c
		next = c.next
		empty = len(c.ends) == 0
	}

	// If the oldest entry according to the metadata is older than
	// the oldest entry we actually have, bump it up to the newer
	// one. This could happen if a chunk is forgotten and then the
	// program crashes before the "oldest" file gets rewritten.
	if len(chunks) > 0 && oldest < chunks[0].oldest {
		oldest = chunks[0].oldest
	}

	return &LogDB{
		path:      path,
		lockfile:  lockfile,
		chunkSize: chunkSize,
		chunks:    chunks,
		oldest:    oldest,
		next:      next,
		syncEvery: 100,
		syncDirty: make(map[*chunk]struct{}),
	}, nil
}

// Append an entry to the database, creating a new chunk if necessary,
// and incrementing the dirty counter. Assumes a write lock is held.
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

	// If the last chunk doesn't have the space for this entry,
	// create a new one.
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

	lastChunk.next++
	db.next++

	// If this is the first entry ever, set the oldest ID to 1
	// (IDs start from 1, not 0)
	if db.oldest == 0 {
		db.oldest = 1
		lastChunk.oldest = 1
	}

	// Mark the current chunk as dirty.
	db.sinceLastSync++
	db.syncDirty[lastChunk] = struct{}{}
	return nil
}

// Add a new chunk to the database. Assumes a write lock is held.
//
// A chunk cannot be empty, so it is only valid to call this if an
// entry is going to be inserted into the chunk immediately.
func (db *LogDB) newChunk() error {
	chunkFile := db.path + "/" + initialChunkFile

	// Filename is "chunk-<1 + last chunk file name>_<next id>"
	if len(db.chunks) > 0 {
		chunkFile = db.path + "/" + db.chunks[len(db.chunks)-1].nextDataFileName(db.next)
	}

	// Create the files for a new chunk.
	err := createChunkFiles(chunkFile, db.chunkSize, db.next)
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

// Remove entries from the beginning and end of the log, performing a
// sync if necessary. Assumes a write lock is held.
func (db *LogDB) truncate(newOldestID, newNewestID uint64) error {
	newNextID := newNewestID + 1

	if newOldestID < db.oldest || newNextID > db.next {
		return ErrIDOutOfRange
	}

	// Remove the metadata for any entries being rolled back.
	for i := len(db.chunks) - 1; i >= 0 && db.chunks[i].next >= newNextID; i-- {
		c := db.chunks[i]
		if c.next-newNextID > uint64(len(c.ends)) {
			c.ends = nil
		} else {
			c.ends = c.ends[0 : uint64(len(c.ends))-(c.next-newNextID)]
		}
		db.syncDirty[c] = struct{}{}
	}

	db.sinceLastSync += newOldestID - db.oldest
	db.sinceLastSync += db.next - newNextID
	db.oldest = newOldestID
	db.next = newNextID

	// Check if this deleted any chunks
	first := 0
	last := len(db.chunks)
	for ; first < len(db.chunks) && db.chunks[first].next < newOldestID; first++ {
	}
	for ; last > 0 && db.chunks[last-1].oldest > newNextID; last-- {
	}

	if first > 0 || last < len(db.chunks) {
		// Mark the chunks for deletion.
		for i := last; i < len(db.chunks); i++ {
			db.chunks[i].delete = true
		}
		for i := 0; i < first; i++ {
			db.chunks[i].delete = true
		}
		// Then sync the db, including writing out the new
		// oldest ID.
		if err := db.sync(); err != nil {
			return err
		}
		db.chunks = db.chunks[first:last]
	}
	return db.periodicSync()
}

// Perform a sync only if needed. Assumes a lock (read or write) is
// held.
func (db *LogDB) periodicSync() error {
	// Sync if the number of unsynced entries is above the
	// threshold
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
	sort.Sort(chunkSlice(dirtyChunks))

	// Sync the chunks in order.
	for _, c := range dirtyChunks {
		if c.delete {
			if err := c.closeAndRemove(); err != nil {
				return &SyncError{&DeleteError{err}}
			}
		} else if err := c.sync(); err != nil {
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
