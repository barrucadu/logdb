// Package raft provides a wrapper making a 'LogDB' instance
// appropriate for use as a 'LogStore' for the
// github.com/hashicorp/raft library.
package raft

import (
	"bytes"

	"github.com/barrucadu/logdb"

	"github.com/hashicorp/raft"
	"github.com/ugorji/go/codec"
)

// LogStore is implements the hashicorp/raft 'LogStore' interface,
// with the backing store being a 'LogDB'.
type LogStore struct {
	Store  *logdb.LogDB
	handle codec.Handle
}

// New creates a 'LogStore' backed by the given 'LogDB'. Log entries
// are encoded with messagepack.
func New(store *logdb.LogDB) *LogStore {
	return &LogStore{Store: store}
}

// FirstIndex returns the first index written. 0 for no entries.
func (l *LogStore) FirstIndex() (uint64, error) {
	return l.Store.OldestID(), nil
}

// LastIndex returns the last index written. 0 for no entries.
func (l *LogStore) LastIndex() (uint64, error) {
	return l.Store.NewestID(), nil
}

// GetLog gets a log entry at a given index.
func (l *LogStore) GetLog(index uint64, log *raft.Log) error {
	bs, err := l.Store.Get(index)
	if err != nil {
		return raft.ErrLogNotFound
	}
	return l.decode(bs, log)
}

// StoreLog stores a log entry.
func (l *LogStore) StoreLog(log *raft.Log) error {
	bs, err := l.encode(log)
	if err != nil {
		return err
	}
	return l.Store.Append(bs)
}

// StoreLogs stores multiple log entries.
func (l *LogStore) StoreLogs(logs []*raft.Log) error {
	var err error
	bss := make([][]byte, len(logs))
	for i, log := range logs {
		bss[i], err = l.encode(log)
		if err != nil {
			return err
		}
	}
	return l.Store.AppendEntries(bss)
}

// DeleteRange deletes a range of log entries. The range is inclusive.
//
// This makes use of the fact that this can be turned into a Forget or
// Rollback: deletion is always from one end.
func (l *LogStore) DeleteRange(min, max uint64) error {
	if min <= l.Store.OldestID() {
		return l.Store.Forget(max + 1)
	}
	return l.Store.Rollback(min - 1)
}

// Close the underlying database. It is not safe to use the 'LogStore'
// for anything else after doing this.
func (l *LogStore) Close() error {
	return l.Store.Close()
}

// Encode a log entry using messagepack.
func (l *LogStore) encode(log *raft.Log) ([]byte, error) {
	if l.handle == nil {
		l.createHandle()
	}

	buf := new(bytes.Buffer)
	err := codec.NewEncoder(buf, l.handle).Encode(*log)
	return buf.Bytes(), err
}

// Decode a log entry using messagepack.
func (l *LogStore) decode(bs []byte, log *raft.Log) error {
	if l.handle == nil {
		l.createHandle()
	}

	return codec.NewDecoderBytes(bs, l.handle).Decode(log)
}

// Set the codec handle.
func (l *LogStore) createHandle() {
	h := new(codec.MsgpackHandle)
	h.ErrorIfNoField = true
	h.InternString = true
	l.handle = h
}
