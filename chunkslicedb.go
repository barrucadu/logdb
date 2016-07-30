package logdb

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sort"
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

	chunks []chunk

	oldest uint64

	newest uint64

	syncEvery     int
	sinceLastSync uint
}

// A chunk is one memory-mapped file.
type chunk struct {
	path string

	bytes []byte

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
	if err := writeFile(path+"/oldest", uint64(0), true); err != nil {
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
	chunks := make([]chunk, len(chunkFiles))
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

		chunks[i], done, err = openChunkFile(fi, chunkOldest)
		if err != nil {
			return nil, err
		}
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
func openChunkFile(fi os.FileInfo, chunkOldest uint64) (chunk, bool, error) {
	chunk := chunk{
		path:   fi.Name(),
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
	bytes, err := mmap(fi.Name())
	if err != nil {
		return chunk, done, &ReadError{err}
	}
	chunk.bytes = bytes

	// read the ending address metadata
	mfile, err := os.Open(fi.Name() + "-meta")
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

	panic("unimplemented")
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
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	panic("unimplemented")
}

func (db *chunkSliceDB) Rollback(newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	panic("unimplemented")
}

func (db *chunkSliceDB) Clone(path string, version uint16, chunkSize uint32) (LogDB, error) {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	panic("unimplemented")
}

func (db *chunkSliceDB) SetSync(every int) error {
	db.syncEvery = every

	// Sync immediately if the number of unsynced entries is now
	// above the threshold
	if every >= 0 && db.sinceLastSync > uint(every) {
		return db.Sync()
	}
	return nil
}

func (db *chunkSliceDB) Sync() error {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	panic("unimplemented")
}

func (db *chunkSliceDB) OldestID() uint64 {
	return db.oldest
}

func (db *chunkSliceDB) NewestID() uint64 {
	return db.newest
}
