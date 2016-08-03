// Package raft provides a wrapper making a 'LogDB' instance appropriate for use as a 'LogStore' for the
// github.com/hashicorp/raft library.
package raft

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/barrucadu/logdb"

	"github.com/hashicorp/raft"
	"github.com/ugorji/go/codec"
)

// ErrZeroIndex is returned when attempting to insert a zero-indexed log entry to an empty database.
var ErrZeroIndex = errors.New("zero is not a valid log index")

// ErrDeleteRange is returned when attempting to delete a range from the middle of the database.
var ErrDeleteRange = errors.New("entries can only be deleted from the start or end")

// A NonincreasingIndexError is returned when attempting to insert a log entry with an index not greater than
// the last entry.
type NonincreasingIndexError struct {
	PriorIndex uint64
	GivenIndex uint64
}

func (e *NonincreasingIndexError) Error() string {
	return fmt.Sprintf("log indices must be strictly increasing (expected %v, given %v)", e.PriorIndex+1, e.GivenIndex)
}

// A NoncontiguousIndexError is returned when attempting to insert a log entry with an index more than one
// greater than the last entry.
type NoncontiguousIndexError struct {
	PriorIndex uint64
	GivenIndex uint64
}

func (e *NoncontiguousIndexError) Error() string {
	return fmt.Sprintf("log indices must be contiguous (expected %v, given %v)", e.PriorIndex+1, e.GivenIndex)
}

// LogStore is implements the hashicorp/raft 'LogStore' interface, with the backing store being a 'LogDB'.
type LogStore struct {
	// Reference to the underlying log store.
	store *logdb.LogDB

	// Codec for encoding/decoding log entries.
	handle codec.Handle

	// Everything has been deleted.
	empty bool

	// Database IDs and log indices aren't guaranteed to match up: eg, if the entire cluster snapshots,
	// deletes some entries, and then a new node joins. So keep track of the offset.
	offset   uint64
	isOffset bool
	rwlock   *sync.RWMutex
}

// New creates a 'LogStore' backed by the given 'LogDB'. Log entries are encoded with messagepack.
//
// If an error is returned, the log store could not be read. This shouldn't happen if it was opened
// successfully, but you never know.
func New(store *logdb.LogDB) (*LogStore, error) {
	l := LogStore{
		store:  store,
		empty:  true,
		rwlock: new(sync.RWMutex),
	}

	h := new(codec.MsgpackHandle)
	h.ErrorIfNoField = true
	h.InternString = true
	l.handle = h

	oldest := store.OldestID()
	if oldest > 0 {
		// This works by conflating database and raft IDs, and won't work once the offset is set.
		var log raft.Log
		if err := (&l).GetLog(oldest, &log); err != nil {
			return nil, err
		}
		if log.Index == 0 {
			return nil, ErrZeroIndex
		}
		l.offset = log.Index - 1
		l.isOffset = true
		l.empty = false
	}
	return &l, nil
}

// FirstIndex returns the first index written. 0 for no entries.
func (l *LogStore) FirstIndex() (uint64, error) {
	l.rwlock.RLock()
	defer l.rwlock.RUnlock()

	if l.empty {
		return 0, nil
	}

	return l.store.OldestID() + l.offset, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (l *LogStore) LastIndex() (uint64, error) {
	l.rwlock.RLock()
	defer l.rwlock.RUnlock()

	if l.empty {
		return 0, nil
	}

	return l.store.NewestID() + l.offset, nil
}

// GetLog gets a log entry at a given index.
func (l *LogStore) GetLog(index uint64, log *raft.Log) error {
	l.rwlock.RLock()
	defer l.rwlock.RUnlock()

	if index < l.offset {
		return raft.ErrLogNotFound
	}

	bs, err := l.store.Get(index - l.offset)
	if err != nil {
		return raft.ErrLogNotFound
	}
	return l.decode(bs, log)
}

// StoreLog stores a log entry.
func (l *LogStore) StoreLog(log *raft.Log) error {
	return l.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (l *LogStore) StoreLogs(logs []*raft.Log) error {
	l.rwlock.Lock()
	defer l.rwlock.Unlock()

	if len(logs) == 0 {
		return nil
	}

	// If this is the first entry in the log, set the offset.
	if !l.isOffset {
		if logs[0].Index == 0 {
			return ErrZeroIndex
		}

		l.offset = logs[0].Index - 1
		l.isOffset = true
	}
	l.empty = false

	last := l.store.NewestID() + l.offset

	bss := make([][]byte, len(logs))
	var err error
	for i, log := range logs {
		if log.Index < last+1 {
			return &NonincreasingIndexError{
				PriorIndex: last,
				GivenIndex: log.Index,
			}
		} else if log.Index > last+1 {
			return &NoncontiguousIndexError{
				PriorIndex: last,
				GivenIndex: log.Index,
			}
		}
		last = log.Index
		bss[i], err = l.encode(log)
		if err != nil {
			return err
		}
	}

	return l.store.AppendEntries(bss)
}

// DeleteRange deletes a range of log entries. The range is inclusive.
//
// This makes use of the fact that this can be turned into a Forget or Rollback: deletion is always from one
// end.
func (l *LogStore) DeleteRange(min, max uint64) error {
	l.rwlock.Lock()
	defer l.rwlock.Unlock()

	first := l.store.OldestID() + l.offset
	last := l.store.NewestID() + l.offset

	if min <= first && max >= last {
		if err := l.store.Forget(l.store.NewestID()); err != nil {
			return err
		}
		l.empty = true
		return nil
	} else if min <= first {
		return l.store.Forget(max - l.offset + 1)
	} else if max >= last {
		return l.store.Rollback(min - l.offset - 1)
	}

	return ErrDeleteRange
}

// Close the underlying database. It is not safe to use the 'LogStore' for anything else after doing this.
func (l *LogStore) Close() error {
	return l.store.Close()
}

// Encode a log entry using messagepack.
func (l *LogStore) encode(log *raft.Log) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := codec.NewEncoder(buf, l.handle).Encode(*log)
	return buf.Bytes(), err
}

// Decode a log entry using messagepack.
func (l *LogStore) decode(bs []byte, log *raft.Log) error {
	return codec.NewDecoderBytes(bs, l.handle).Decode(log)
}
