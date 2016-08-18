package logdb

import "sync"

// InMemDB is an in-memory 'LogDB' implementation. As does not support persistence, it shouldn't be used in a
// production system. It is, however, helpful for benchmark comparisons as an absolute best case to compare
// against.
type InMemDB struct {
	rwlock  sync.RWMutex
	entries map[uint64][]byte
	oldest  uint64
	newest  uint64
}

// Append implements the 'LogDB' interface.
func (db *InMemDB) Append(entry []byte) (uint64, error) {
	return db.AppendEntries([][]byte{entry})
}

// AppendEntries implements the 'LogDB' interface.
func (db *InMemDB) AppendEntries(entries [][]byte) (uint64, error) {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	if db.entries == nil {
		db.entries = make(map[uint64][]byte)
	}

	if db.oldest == 0 && len(entries) > 0 {
		db.oldest = 1
	}

	idx := db.newest + 1

	for _, entry := range entries {
		db.newest++
		db.entries[db.newest] = entry
	}

	return idx, nil
}

// Get implements the 'LogDB' interface
func (db *InMemDB) Get(id uint64) ([]byte, error) {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	if db.oldest == 0 || id < db.oldest || id > db.newest {
		return nil, ErrIDOutOfRange
	}

	return db.entries[id], nil
}

// Forget implements the 'LogDB' interface.
func (db *InMemDB) Forget(newOldestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.forget(newOldestID)
}

// Rollback implements the 'LogDB' interface.
func (db *InMemDB) Rollback(newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.rollback(newNewestID)
}

// Truncate implements the 'LogDB' interface.
func (db *InMemDB) Truncate(newOldestID, newNewestID uint64) error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	if newNewestID < newOldestID {
		return ErrIDOutOfRange
	}
	if err := db.forget(newOldestID); err != nil {
		return err
	}
	return db.rollback(newNewestID)
}

// OldestID implements the 'LogDB' interface.
func (db *InMemDB) OldestID() uint64 {
	return db.oldest
}

// NewestID implements the 'LogDB' interface.
func (db *InMemDB) NewestID() uint64 {
	return db.newest
}

// Implements 'Forget'. Assumes a write lock is held.
func (db *InMemDB) forget(newOldestID uint64) error {
	if newOldestID > db.newest {
		return ErrIDOutOfRange
	}
	if newOldestID < db.oldest {
		return nil
	}

	var i uint64
	for i = db.oldest; i < newOldestID; i++ {
		delete(db.entries, i)
	}
	db.oldest = newOldestID

	return nil
}

// Implements 'Rollback'. Assumes a write lock is held.
func (db *InMemDB) rollback(newNewestID uint64) error {
	if newNewestID < db.oldest {
		return ErrIDOutOfRange
	}
	if newNewestID > db.newest {
		return nil
	}

	var i uint64
	for i = db.newest; i > newNewestID; i-- {
		delete(db.entries, i)
	}
	db.newest = newNewestID

	return nil
}
