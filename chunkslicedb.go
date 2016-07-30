package logdb

import "sync"

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
	ends []int

	oldest uint64

	newest uint64
}

func openChunkSliceDB(path string, chunkSize uint32) (*chunkSliceDB, error) {
	panic("unimplemented")
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
	start := 0
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
