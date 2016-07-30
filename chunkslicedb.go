package logdb

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
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
	csfile, err := os.Create(path + "/oldest")
	if err != nil {
		return nil, &WriteError{err}
	}
	if err := binary.Write(csfile, binary.LittleEndian, uint64(0)); err != nil {
		return nil, &WriteError{err}
	}

	return &chunkSliceDB{path: path, chunkSize: chunkSize}, nil
}

func openChunkSliceDB(path string, chunkSize uint32) (*chunkSliceDB, error) {
	// Read the "oldest" file.
	ofile, err := os.Open(path + "/oldest")
	if err != nil {
		return nil, &ReadError{err}
	}
	var oldest uint64
	if err := binary.Read(ofile, binary.LittleEndian, &oldest); err != nil {
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
		}

		// mmap the data file
		fd, err := syscall.Open(fi.Name(), syscall.O_RDWR, 0644)
		if err != nil {
			return nil, &ReadError{err}
		}
		bytes, err := syscall.Mmap(fd, 0, int(fi.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return nil, &ReadError{err}
		}

		// read the ending address metadata
		mfile, err := os.Open(fi.Name() + "-meta")
		if err != nil {
			return nil, &ReadError{err}
		}
		var ends []int32
		prior := int32(-1)
		for {
			var this int32
			if err := binary.Read(mfile, binary.LittleEndian, &this); err != nil {
				if err == io.EOF {
					break
				}
				return nil, ErrCorrupt
			}
			if this <= prior {
				done = true
				break
			}
			ends = append(ends, this)
			prior = this
		}

		chunkNewest = chunkOldest + uint64(len(ends))
		chunks[i] = chunk{
			path:   fi.Name(),
			bytes:  bytes,
			ends:   ends,
			oldest: chunkOldest,
			newest: chunkNewest,
		}
		chunkOldest++
	}

	return &chunkSliceDB{
		path:      path,
		chunkSize: chunkSize,
		chunks:    chunks,
		oldest:    oldest,
		newest:    chunkNewest,
	}, nil
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

func (db *chunkSliceDB) OldestID() uint64 {
	return db.oldest
}

func (db *chunkSliceDB) NewestID() uint64 {
	return db.newest
}
