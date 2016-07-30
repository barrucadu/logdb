package logdb

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// chunkSliceDB is the main LogDB instance, representing a database as
// a slice of memory-mapped files.
type chunkSliceDB struct {
	path string

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

	newest uint64

	syncEvery     int
	sinceLastSync uint64
	syncDirty     []int
}

// A chunk is one memory-mapped file.
type chunk struct {
	path string

	bytes []byte
	mmapf *os.File

	// One past the ending addresses of entries in the 'bytes'
	// slice.
	//
	// This choice is because starting addresses can always be
	// calculated from ending addresses, as the first entry starts
	// at offset 0 (and there are no gaps). Ending addresses
	// cannot be calculated from starting addresses, unless the
	// ending address of the final entry is stored as well.
	ends []int32

	oldest uint64

	newest uint64

	dirty bool
}

// Delete the files associated with a chunk.
func (c *chunk) closeAndRemove() error {
	if err := closeAndRemove(c.mmapf); err != nil {
		return err
	}
	return os.Remove(c.path + "-meta")
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
	return len(ni) < len(nj) || ni < nj
}

func createChunkSliceDB(path string, chunkSize uint32) (*chunkSliceDB, error) {
	// Write the "oldest" file.
	if err := writeFile(path+"/oldest", uint64(0)); err != nil {
		return nil, &WriteError{err}
	}

	return &chunkSliceDB{path: path, chunkSize: chunkSize, syncEvery: 100}, nil
}

func openChunkSliceDB(path string, chunkSize uint32) (*chunkSliceDB, error) {
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
		if !fi.IsDir() && strings.HasPrefix(fi.Name(), "chunk-") && !strings.HasSuffix(fi.Name(), "-meta") {
			chunkFiles = append(chunkFiles, fi)
		}
	}
	sort.Sort(fileInfoSlice(chunkFiles))

	// Populate the chunk slice.
	chunks := make([]*chunk, len(chunkFiles))
	chunkOldest := oldest
	chunkNewest := oldest
	done := false
	for i, fi := range chunkFiles {
		if done {
			// We ran out of strictly increasing ending
			// positions, which should ONLY happen in the
			// last chunk file.
			return nil, ErrCorrupt
		} else if i > 0 {
			chunkOldest = 1 + chunks[i-1].newest
		}

		var c chunk
		c, done, err = openChunkFile(path, fi, chunkOldest)
		if err != nil {
			return nil, err
		}
		chunks[i] = &c
		chunkNewest = chunkOldest + uint64(len(chunks[i].ends))
	}

	return &chunkSliceDB{
		path:      path,
		chunkSize: chunkSize,
		chunks:    chunks,
		oldest:    oldest,
		newest:    chunkNewest,
		syncEvery: 100,
	}, nil
}

// Open a chunk file
func openChunkFile(basedir string, fi os.FileInfo, chunkOldest uint64) (chunk, bool, error) {
	chunk := chunk{
		path:   basedir + "/" + fi.Name(),
		oldest: chunkOldest,
	}

	// If 'done' gets set, then it means that chunk ending offsets
	// have stopped strictly increasing. This happens if entries
	// are rolled back, but not enough to delete an entire chunk:
	// the ending offsets get reset to 0 in the metadata file in
	// that case. This should only happen in the final chunk, so
	// it is an error for 'done' to become true if there are
	// further chunk files.
	var done bool

	// mmap the data file
	mmapf, bytes, err := mmap(chunk.path)
	if err != nil {
		return chunk, done, &ReadError{err}
	}
	chunk.bytes = bytes
	chunk.mmapf = mmapf

	// read the ending address metadata
	mfile, err := os.Open(basedir + "/" + fi.Name() + "-meta")
	if err != nil {
		return chunk, done, &ReadError{err}
	}
	prior := int32(-1)
	for {
		var this int32
		if err := binary.Read(mfile, binary.LittleEndian, &this); err != nil {
			if err == io.EOF {
				break
			}
			return chunk, done, ErrCorrupt
		}
		if this <= prior {
			done = true
			break
		}
		chunk.ends = append(chunk.ends, this)
		prior = this
	}

	chunk.newest = chunkOldest + uint64(len(chunk.ends))
	return chunk, done, nil
}

func (db *chunkSliceDB) Append(entry []byte) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

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
	db.newest++

	// Mark the current chunk as dirty and perform a periodic sync.
	db.sinceLastSync++
	if !lastChunk.dirty {
		lastChunk.dirty = true
		db.syncDirty = append(db.syncDirty, len(db.chunks)-1)
	}
	return db.periodicSync(true)
}

// Add a new chunk to the database.
func (db *chunkSliceDB) newChunk() error {
	chunkFile := "chunk-0"

	// Filename is "chunk-<1 + last chunk file name>"
	if len(db.chunks) > 0 {
		strnum := db.chunks[len(db.chunks)-1].path[len("chunk-"):]
		num, err := strconv.Atoi(strnum)
		if err != nil {
			return err
		}
		chunkFile = "chunk-" + strconv.Itoa(num+1)
	}

	// Create the chunk files.
	if err := createFile(db.path + "/" + chunkFile + "-meta"); err != nil {
		return err
	}
	if err := createFileAtSize(db.path+"/"+chunkFile, db.chunkSize); err != nil {
		return err
	}

	// Open the newly-created chunk file.
	fi, err := os.Stat(db.path + "/" + chunkFile)
	if err != nil {
		return err
	}
	c, _, err := openChunkFile(db.path, fi, db.newest+1)
	if err != nil {
		return err
	}
	db.chunks = append(db.chunks, &c)

	return nil
}

func (db *chunkSliceDB) Get(id uint64) ([]byte, error) {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	// Check ID is in range.
	if id < db.oldest || id > db.newest || len(db.chunks) == 0 {
		return nil, ErrIDOutOfRange
	}

	// Binary search through chunks for the one containing the ID.
	lo := 0
	hi := len(db.chunks)
	mid := (hi - lo) / 2
	for ; !(db.chunks[mid].oldest <= id && id <= db.chunks[mid].newest); mid = (hi - lo) / 2 {
		if db.chunks[mid].newest < id {
			lo = mid
		} else if db.chunks[mid].oldest > id {
			hi = mid
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
		out[i] = chunk.bytes[start+i]
	}
	return out, nil
}

func (db *chunkSliceDB) Forget(newOldestID uint64) error {
	return db.truncate(newOldestID, db.newest)
}

func (db *chunkSliceDB) Rollback(newNewestID uint64) error {
	return db.truncate(db.oldest, newNewestID)
}

// Truncate the database at both ends.
func (db *chunkSliceDB) truncate(newOldestID uint64, newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	if newOldestID < db.oldest || newNewestID > db.newest {
		return ErrIDOutOfRange
	}

	// Remove the metadata for any entries being rolled back.
	for i := len(db.chunks) - 1; i > 0 && db.chunks[i].newest > newNewestID; i-- {
		c := db.chunks[i]
		c.ends = c.ends[0 : c.newest-newNewestID]
		if !c.dirty {
			c.dirty = true
			db.syncDirty = append(db.syncDirty, i)
		}
	}

	db.sinceLastSync += newOldestID - db.oldest
	db.sinceLastSync += db.newest - newNewestID
	db.oldest = newOldestID
	db.newest = newNewestID

	// Check if this deleted any chunks
	first := 0
	last := len(db.chunks)
	for ; first < len(db.chunks) && db.chunks[first].newest < newOldestID; first++ {
	}
	for ; last > 0 && db.chunks[last].oldest > newNewestID; last-- {
	}

	if first > 0 || last < len(db.chunks) {
		// It did! Sync everything and then delete the files.
		if err := db.sync(false); err != nil {
			return err
		}
		for i, c := range db.chunks {
			if i >= first && i < last {
				continue
			}
			if err := c.closeAndRemove(); err != nil {
				return &DeleteError{err}
			}
		}
		db.chunks = db.chunks[first:last]
	}
	return db.periodicSync(false)
}

func (db *chunkSliceDB) Clone(path string, version uint16, chunkSize uint32) (LogDB, error) {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	panic("unimplemented")
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

	for _, i := range db.syncDirty {
		// To ensure ACID, sync the data first and only then
		// the metadata. This means that if there is a failure
		// between the two syncs, even if the newly-written
		// data is corrupt, there will be no metadata
		// referring to it, and so it will be invisible to the
		// database when next opened.
		if err := fsync(db.chunks[i].mmapf); err != nil {
			return &SyncError{err}
		}
		if err := writeFile(db.chunks[i].path+"-meta", db.chunks[i].ends); err != nil {
			return &SyncError{err}
		}
		db.chunks[i].dirty = false
	}

	// Write the oldest entry ID.
	if err := writeFile(db.path+"/oldest", db.oldest); err != nil {
		return &SyncError{err}
	}

	db.syncDirty = nil
	db.sinceLastSync = 0

	return nil
}

func (db *chunkSliceDB) OldestID() uint64 {
	return db.oldest
}

func (db *chunkSliceDB) NewestID() uint64 {
	return db.newest
}
