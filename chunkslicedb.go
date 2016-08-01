package logdb

import (
	"io/ioutil"
	"os"
	"sort"
	"sync"
)

// chunkSliceDB is the main LogDB instance, representing a database as
// a slice of memory-mapped files.
type chunkSliceDB struct {
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

	chunkSize uint32

	chunks []*chunk

	oldest uint64

	next uint64

	syncEvery     int
	sinceLastSync uint64
	syncDirty     map[*chunk]struct{}
}

// fileInfoSlice implements nice sorting for 'os.FileInfo': first
// compare filenames by length, and then lexicographically.
type fileInfoSlice []os.FileInfo

func (fis fileInfoSlice) Len() int {
	return len(fis)
}

func (fis fileInfoSlice) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func (fis fileInfoSlice) Less(i, j int) bool {
	ni := fis[i].Name()
	nj := fis[j].Name()

	if len(ni) == len(nj) {
		return ni < nj
	}
	return len(ni) < len(nj)
}

func createChunkSliceDB(path string, chunkSize uint32) (*chunkSliceDB, error) {
	// Lock the "version" file.
	lockfile, err := flock(path + "/version")
	if err != nil {
		return nil, &LockError{err}
	}

	// Write the "oldest" file.
	if err := writeFile(path+"/oldest", uint64(0)); err != nil {
		return nil, &WriteError{err}
	}

	return &chunkSliceDB{
		path:      path,
		lockfile:  lockfile,
		chunkSize: chunkSize,
		syncEvery: 256,
		next:      1,
		syncDirty: make(map[*chunk]struct{}),
	}, nil
}

func openChunkSliceDB(path string, chunkSize uint32) (*chunkSliceDB, error) {
	// Lock the "version" file.
	lockfile, err := flock(path + "/version")
	if err != nil {
		return nil, &LockError{err}
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
	for i, fi := range chunkFiles {
		c, err := openChunkFile(path, fi, prior, chunkSize, false)
		if err != nil {
			return nil, err
		}
		chunks[i] = &c
		prior = &c
		next = c.next
	}

	return &chunkSliceDB{
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

func (db *chunkSliceDB) Append(entry []byte) error {
	return db.AppendEntries([][]byte{entry})
}

func (db *chunkSliceDB) AppendEntries(entries [][]byte) error {
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

	return db.periodicSync(false)
}

func (db *chunkSliceDB) append(entry []byte) error {
	if uint32(len(entry)) > db.chunkSize {
		return ErrTooBig
	}

	isNewChunk := false

	// If there are no chunks, create a new one.
	if len(db.chunks) == 0 {
		if err := db.newChunk(); err != nil {
			return &WriteError{err}
		}
		isNewChunk = true
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
			isNewChunk = true
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

	// Mark the current chunk as dirty, unless it's a new chunk:
	// in which case sync it immediately (to avoid an empty chunk
	// on disk)
	if isNewChunk {
		lastChunk.sync()
	} else {
		db.sinceLastSync++
		db.syncDirty[lastChunk] = struct{}{}
	}
	return nil
}

// Add a new chunk to the database.
//
// A chunk cannot be empty, so it is only valid to call this if an
// entry is going to be inserted into the chunk immediately.
func (db *chunkSliceDB) newChunk() error {
	chunkFile := db.path + "/" + initialChunkFile

	// Filename is "chunk-<1 + last chunk file name>"
	if len(db.chunks) > 0 {
		chunkFile = db.path + "/" + db.chunks[len(db.chunks)-1].nextDataFileName()
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
	c, err := openChunkFile(db.path, fi, prior, db.chunkSize, true)
	if err != nil {
		return err
	}
	db.chunks = append(db.chunks, &c)

	// Force a full metadata write when this chunk is first
	// synced.
	c.rollback = true

	return nil
}

func (db *chunkSliceDB) Get(id uint64) ([]byte, error) {
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

func (db *chunkSliceDB) Forget(newOldestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	return db.truncate(newOldestID, db.NewestID())
}

func (db *chunkSliceDB) Rollback(newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	return db.truncate(db.OldestID(), newNewestID)
}

func (db *chunkSliceDB) Truncate(newOldestID, newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()
	return db.truncate(newOldestID, newNewestID)
}

func (db *chunkSliceDB) truncate(newOldestID, newNewestID uint64) error {
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
		c.rollback = true
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
		if err := db.sync(false); err != nil {
			return err
		}
		db.chunks = db.chunks[first:last]
	}
	return db.periodicSync(false)
}

func (db *chunkSliceDB) SetSync(every int) error {
	db.syncEvery = every

	// Immediately perform a periodic sync.
	return db.periodicSync(true)
}

func (db *chunkSliceDB) Sync() error {
	return db.sync(true)
}

// Perform a sync only if needed. This function is not safe to execute
// concurrently with a write, so the 'acquireLock' parameter MUST be
// true UNLESS the write lock is already held by this thread.
func (db *chunkSliceDB) periodicSync(acquireLock bool) error {
	// Sync if the number of unsynced entries is above the
	// threshold
	if db.syncEvery >= 0 && db.sinceLastSync > uint64(db.syncEvery) {
		return db.sync(acquireLock)
	}
	return nil
}

// This function is not safe to execute concurrently with a write, so
// the 'acquireLock' parameter MUST be true UNLESS the write lock is
// already held by this thread.
func (db *chunkSliceDB) sync(acquireLock bool) error {
	if acquireLock {
		db.rwlock.RLock()
		defer db.rwlock.RUnlock()
	}

	for c, _ := range db.syncDirty {
		if c.delete {
			if err := c.closeAndRemove(); err != nil {
				return &SyncError{&DeleteError{err}}
			}
		} else if err := c.sync(); err != nil {
			return &SyncError{err}
		}
	}
	db.syncDirty = make(map[*chunk]struct{})

	// Write the oldest entry ID.
	if err := writeFile(db.path+"/oldest", db.oldest); err != nil {
		return &SyncError{err}
	}

	db.sinceLastSync = 0

	return nil
}

func (db *chunkSliceDB) OldestID() uint64 {
	return db.oldest
}

func (db *chunkSliceDB) NewestID() uint64 {
	return db.next - 1
}

func (db *chunkSliceDB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	// First sync everything
	err := db.sync(false)

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
