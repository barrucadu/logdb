package logdb

import (
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const latestVersion = uint16(0)

////////// LOG-STRUCTURED DATABASE //////////

// ChunkDB is a 'LogDB' implementation using an on-disk format where entries are stored in fixed-size
// "chunks". It provides data consistency guarantees in the event of program failure: database entries are
// guaranteed to be contiguous, and if the database thinks an entry is there, it will contain non-corrupt
// data.
//
// In addition to implementing the behaviour specified in the 'LogDB', 'PersistDB', 'BoundedDB', and 'CloseDB'
// interfaces, a 'Sync' is always performed if an entire chunk is forgotten, rolled back, or truncated; or if
// an append creates a new on-disk chunk.
type ChunkDB struct {
	// The underlying 'LockFreeChunkDB'. This is not safe for concurrent use with the 'ChunkDB'.
	*LockFreeChunkDB

	// Any number of goroutines can read simultaneously, but when entries are added or removed, the write
	// lock must be held.
	//
	// An alternative design point to consider is to have a separate 'RWMutex' in each chunk, and hold only
	// the necessary write locks. This would complicate locking but allow for more concurrent reading, and
	// so may be better under some work loads.
	rwlock sync.RWMutex
}

// A LockFreeChunkDB is a 'ChunkDB' with no internal locks. It is NOT safe for concurrent use.
type LockFreeChunkDB struct {
	// Path to the database directory.
	path string

	// Lock file used to prevent multiple simultaneous open handles: concurrent use of one handle is fine,
	// multiple handles is not. This file is locked exclusive, not shared.
	lockfile *os.File

	// Flag indicating that the handle has been closed. This is used to give 'ErrClosed' errors.
	closed bool

	// Size of individual chunks. Entries are not split over chunks, and so they cannot be bigger than this.
	chunkSize uint32

	// Chunks, in order.
	chunks []*chunk

	// Oldest entry ID. This may be > the first chunk oldest if forgetting has happened.
	oldest uint64

	// Newest entry ID. This is not a source of internal truth! It is only here to make 'NewestID'
	// lock-free! This should always be equal to 'db.next() - 1', and is updated in 'AppendEntries',
	// 'Rollback', and 'Truncate'. It doesn't need to be updated in 'Append', as that calls
	// 'AppendEntries', or in 'Forget', as that only deletes from the back.
	newest uint64

	// Data syncing: 'syncEvery' is how many changes (entries appended/truncated) to allow before syncing,
	// 'sinceLastSync' keeps track of this, and 'syncDirty' is the set of chunks to sync. When syncing,
	// first chunks are deleted newest-first, then data is flushed oldest-first. This is to maintain
	// consistency,
	syncEvery     int
	sinceLastSync uint64
	syncDirty     map[*chunk]struct{}

	// Concurrent syncing/reading is safe, but syncing/writing and syncing/syncing is not. To prevent the
	// first, syncing claims a read lock. To prevent the latter, a special sync lock is used. Claiming a
	// write lock would also work, but is far more heavyweight.
	//
	// This is inside LockFreeChunkDB because picking it out would be a real pain. TODO: fix :(
	slock sync.Mutex
}

// Open a 'LockFreeChunkDB' database.
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
func Open(path string, chunkSize uint32, create bool) (*LockFreeChunkDB, error) {
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

// Wrap a 'LockFreeChunkDB' into a 'ChunkDB', which is safe for concurrent use. The underlying
// 'LockFreeChunkDB' should not be used while the returned 'ChunkDB' is live.
func WrapForConcurrency(db *LockFreeChunkDB) *ChunkDB {
	return &ChunkDB{LockFreeChunkDB: db}
}

// Append implements the 'LogDB', 'PersistDB', 'BoundedDB', and 'CloseDB' interfaces.
func (db *LockFreeChunkDB) Append(entry []byte) (uint64, error) {
	return db.AppendEntries([][]byte{entry})
}

// AppendEntries implements the 'LogDB', 'PersistDB', 'BoundedDB', and 'CloseDB' interfaces.
func (db *ChunkDB) AppendEntries(entries [][]byte) (uint64, error) {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.LockFreeChunkDB.AppendEntries(entries)
}

// AppendEntries implements the 'LogDB', 'PersistDB', 'BoundedDB', and 'CloseDB' interfaces.
func (db *LockFreeChunkDB) AppendEntries(entries [][]byte) (uint64, error) {
	defer func() { db.newest = db.next() - 1 }()

	if db.closed {
		return 0, ErrClosed
	}

	originalNewest := db.next() - 1

	var appended bool
	for _, entry := range entries {
		if err := db.append(entry); err != nil {
			// Rollback on error if we've already appended some entries.
			if appended {
				if rerr := db.rollback(originalNewest); rerr != nil {
					return 0, &AtomicityError{AppendErr: err, RollbackErr: rerr}
				}
			}
			return 0, err
		}
		appended = true
	}

	return originalNewest + 1, db.periodicSync()
}

// Get implements the 'LogDB' and 'CloseDB' interfaces.
func (db *ChunkDB) Get(id uint64) ([]byte, error) {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	return db.LockFreeChunkDB.Get(id)
}

// Get implements the 'LogDB' and 'CloseDB' interfaces.
func (db *LockFreeChunkDB) Get(id uint64) ([]byte, error) {
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

// Forget implements the 'LogDB', 'PersistDB', and 'CloseDB' interfaces.
func (db *ChunkDB) Forget(newOldestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.LockFreeChunkDB.Forget(newOldestID)
}

// Forget implements the 'LogDB', 'PersistDB', and 'CloseDB' interfaces.
func (db *LockFreeChunkDB) Forget(newOldestID uint64) error {
	if db.closed {
		return ErrClosed
	}
	return db.forget(newOldestID)
}

// Rollback implements the 'LogDB', 'PersistDB', and 'CloseDB' interfaces.
func (db *ChunkDB) Rollback(newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.LockFreeChunkDB.Rollback(newNewestID)
}

// Rollback implements the 'LogDB', 'PersistDB', and 'CloseDB' interfaces.
func (db *LockFreeChunkDB) Rollback(newNewestID uint64) error {
	defer func() { db.newest = db.next() - 1 }()
	if db.closed {
		return ErrClosed
	}
	return db.rollback(newNewestID)
}

// Truncate implements the 'LogDB', 'PersistDB', and 'CloseDB' interfaces.
func (db *ChunkDB) Truncate(newOldestID, newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.LockFreeChunkDB.Truncate(newOldestID, newNewestID)
}

// Truncate implements the 'LogDB', 'PersistDB', and 'CloseDB' interfaces.
func (db *LockFreeChunkDB) Truncate(newOldestID, newNewestID uint64) error {
	defer func() { db.newest = db.next() - 1 }()
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

// OldestID implements the 'LogDB' interface.
func (db *LockFreeChunkDB) OldestID() uint64 {
	return db.oldest
}

// NewestID implements the 'LogDB' interface.
func (db *LockFreeChunkDB) NewestID() uint64 {
	return db.newest
}

// SetSync implements the 'PersistDB' and 'CloseDB' interface.
func (db *ChunkDB) SetSync(every int) error {
	db.syncEvery = every

	// Immediately perform a periodic sync.
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return db.periodicSync()
}

// SetSync implements the 'PersistDB' and 'CloseDB' interface.
func (db *LockFreeChunkDB) SetSync(every int) error {
	db.syncEvery = every

	// Immediately perform a periodic sync.
	if db.closed {
		return ErrClosed
	}
	return db.periodicSync()
}

// Sync implements the 'PersistDB' and 'CloseDB' interface.
func (db *ChunkDB) Sync() error {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	return db.LockFreeChunkDB.Sync()
}

// Sync implements the 'PersistDB' and 'CloseDB' interface.
func (db *LockFreeChunkDB) Sync() error {
	if db.closed {
		return ErrClosed
	}
	return db.sync()
}

// MaxEntrySize implements the 'BoundedDB' interface.
func (db *LockFreeChunkDB) MaxEntrySize() uint64 {
	return uint64(db.chunkSize)
}

// Close implements the 'CloseDB' interface. This also closes the underlying 'LockFreeChunkDB'.
func (db *ChunkDB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.LockFreeChunkDB.Close()
}

// Close implements the 'CloseDB' interface.
func (db *LockFreeChunkDB) Close() error {
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
func createdb(path string, chunkSize uint32) (*LockFreeChunkDB, error) {
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

	return &LockFreeChunkDB{
		path:      path,
		closed:    false,
		lockfile:  lockfile,
		chunkSize: chunkSize,
		syncEvery: 256,
		syncDirty: make(map[*chunk]struct{}),
	}, nil
}

// Open an existing database. It is an error to call this function if the database directory does not exist.
func opendb(path string) (*LockFreeChunkDB, error) {
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
	var metaFiles []os.FileInfo
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, &ReadError{err}
	}
	for _, fi := range fis {
		if !fi.IsDir() && isBasenameChunkDataFile(fi.Name()) {
			chunkFiles = append(chunkFiles, fi)
		}
	}
	for _, fi := range fis {
		if !fi.IsDir() && isBasenameChunkMetaFile(fi.Name()) {
			metaFiles = append(metaFiles, fi)
		}
	}

	sort.Sort(fileInfoSlice(chunkFiles))

	if len(metaFiles) > 0 {
		// There may be metadata files without accompanying
		// data files, if the program died while deleting.
		// Delete such files.
		for _, fi := range metaFiles {
			if _, err := os.Stat(path + "/" + dataFilePath(fi.Name())); err != nil {
				_ = os.Remove(path + "/" + fi.Name())
			}
		}
	}

	if len(chunkFiles) > 0 {
		// There may be a gap in the chunk files, if the program died while deleting them. Because
		// files are deleted newest-first, the newest contiguous sequence of chunks is what should
		// be retained; all chunks before a gap can be deleted.
		first := 0
		priorCID := uint64(0)
		for i := len(chunkFiles) - 1; i >= 0; i-- {
			// This does no validation because isBasenameChunkDataFile took care of that.
			nameBits := strings.Split(chunkFiles[i].Name(), sep)
			cid, _ := strconv.ParseUint(nameBits[1], 10, 0)

			// priorCID keeps track of the ID of the prior chunk. Because we're traversing
			// backwards, these should decrease by 1 every time with no gaps. If there is a gap,
			// we can enter chunk deleting mode.
			if priorCID > 0 && cid < priorCID-1 {
				filePath := path + "/" + chunkFiles[i].Name()
				metaPath := metaFilePath(filePath)
				_ = os.Remove(filePath)
				_ = os.Remove(metaPath)
			} else {
				priorCID = cid
				first = i
			}
		}
		chunkFiles = chunkFiles[first:]

		// The final chunk may be zero-size, if the program died between the file being created and it
		// being sized. If it is, delete it. Similarly, the final chunk may have no metadata file.
		final := chunkFiles[len(chunkFiles)-1]
		filePath := path + "/" + final.Name()
		metaPath := metaFilePath(filePath)
		if _, err := os.Stat(metaPath); final.Size() == 0 || err != nil {
			_ = os.Remove(filePath)
			_ = os.Remove(metaPath)
			chunkFiles = chunkFiles[:len(chunkFiles)-1]
		}
	}

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

	db := &LockFreeChunkDB{
		path:      path,
		closed:    false,
		lockfile:  lockfile,
		chunkSize: chunkSize,
		chunks:    chunks,
		oldest:    oldest,
		syncEvery: 100,
		syncDirty: make(map[*chunk]struct{}),
	}
	db.newest = db.next() - 1

	return db, nil
}

// Return the 'next' value of the last chunk. Assumes a read lock is held.
func (db *LockFreeChunkDB) next() uint64 {
	if len(db.chunks) == 0 {
		return 1
	}
	return db.chunks[len(db.chunks)-1].next()
}

// Append an entry to the database, creating a new chunk if necessary, and incrementing the dirty counter.
// Assumes a write lock is held.
func (db *LockFreeChunkDB) append(entry []byte) error {
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
func (db *LockFreeChunkDB) newChunk() error {
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
func (db *LockFreeChunkDB) forget(newOldestID uint64) error {
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
func (db *LockFreeChunkDB) rollback(newNewestID uint64) error {
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
func (db *LockFreeChunkDB) periodicSync() error {
	if db.syncEvery >= 0 && db.sinceLastSync > uint64(db.syncEvery) {
		return db.sync()
	}
	return nil
}

// Perform a sync immediately. Assumes a lock (read or write) is held.
func (db *LockFreeChunkDB) sync() error {
	// Suboptimal!
	db.slock.Lock()
	defer db.slock.Unlock()

	// Produce a sorted list of chunks to sync.
	dirtyChunks := make([]*chunk, len(db.syncDirty))
	var i int
	for c := range db.syncDirty {
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
func (db *LockFreeChunkDB) syncOne(c *chunk) error {
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
