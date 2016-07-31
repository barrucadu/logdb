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
	Store  logdb.LogDB
	Handle codec.Handle
}

// UsingBinc creates a new 'LogStore' using binc to encode and decode
// log entries.
func UsingBinc(store logdb.LogDB) *LogStore {
	h := new(codec.BincHandle)
	h.ErrorIfNoField = true
	h.InternString = true

	return &LogStore{
		Store:  store,
		Handle: h,
	}
}

// UsingCBOR creates a new 'LogStore' using CBOR to encode and decode
// log entries.
func UsingCBOR(store logdb.LogDB) *LogStore {
	h := new(codec.CborHandle)
	h.ErrorIfNoField = true
	h.InternString = true

	return &LogStore{
		Store:  store,
		Handle: h,
	}
}

// UsingJSON creates a new 'LogStore' using JSON to encode and decode
// log entries.
func UsingJSON(store logdb.LogDB) *LogStore {
	h := new(codec.JsonHandle)
	h.ErrorIfNoField = true
	h.InternString = true

	return &LogStore{
		Store:  store,
		Handle: h,
	}
}

// UsingMsgpack creates a new 'LogStore' using msgpack to encode and
// decode log entries.
func UsingMsgpack(store logdb.LogDB) *LogStore {
	h := new(codec.MsgpackHandle)
	h.ErrorIfNoField = true
	h.InternString = true

	return &LogStore{
		Store:  store,
		Handle: h,
	}
}

// UsingSimple creates a new 'LogStore' using simple to encode and
// decode log entries.
func UsingSimple(store logdb.LogDB) *LogStore {
	h := new(codec.SimpleHandle)
	h.ErrorIfNoField = true
	h.InternString = true

	return &LogStore{
		Store:  store,
		Handle: h,
	}
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
	return codec.NewDecoderBytes(bs, l.Handle).Decode(log)
}

// StoreLog stores a log entry.
func (l *LogStore) StoreLog(log *raft.Log) error {
	buf := new(bytes.Buffer)
	if err := codec.NewEncoder(buf, l.Handle).Encode(*log); err != nil {
		return err
	}
	return l.Store.Append(buf.Bytes())
}

// StoreLogs stores multiple log entries.
func (l *LogStore) StoreLogs(logs []*raft.Log) error {
	bss := make([][]byte, len(logs))
	for i, log := range logs {
		buf := new(bytes.Buffer)
		if err := codec.NewEncoder(buf, l.Handle).Encode(*log); err != nil {
			return err
		}
		bss[i] = buf.Bytes()
	}
	return l.Store.AppendEntries(bss)
}

// DeleteRange deletes a range of log entries. The range is inclusive.
//
// This makes use of the fact that this can be turned into a Forget or
// Rollback: deletion is always from one end.
func (l *LogStore) DeleteRange(min, max uint64) error {
	if min <= l.Store.OldestID() {
		return l.Store.Forget(max)
	}
	return l.Store.Rollback(min - 1)
}

// Close the underlying database. It is not safe to use the 'LogStore'
// for anything else after doing this.
func (l *LogStore) Close() error {
	return l.Store.Close()
}
