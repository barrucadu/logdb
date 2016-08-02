package raft

import (
	"fmt"
	"os"
	"testing"

	"github.com/barrucadu/logdb"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

const numEntries = 255

func TestEmptyIndices(t *testing.T) {
	db := assertCreate(t, "empty_indices")
	defer assertClose(t, db)

	assert.Equal(t, uint64(0), assertFirstIndex(t, db))
	assert.Equal(t, uint64(0), assertLastIndex(t, db))
}

func TestStoreLog(t *testing.T) {
	db := assertCreate(t, "store_log")
	defer assertClose(t, db)

	logs := make([]*raft.Log, numEntries)
	for i := 0; i < len(logs); i++ {
		logs[i] = &raft.Log{
			Index: uint64(i),
			Term:  0,
			Type:  raft.LogType(i),
			Data:  []byte(fmt.Sprintf("log entry %v", i)),
		}
	}

	for _, log := range logs {
		assertStoreLog(t, db, log)
	}

	assert.Equal(t, uint64(1), assertFirstIndex(t, db))
	assert.Equal(t, uint64(len(logs)), assertLastIndex(t, db))

	for i, log := range logs {
		l := assertGetLog(t, db, uint64(i+1))
		assert.Equal(t, *log, *l)
	}
}

func TestStoreLogs(t *testing.T) {
	db := assertCreate(t, "store_logs")
	defer assertClose(t, db)

	logs := make([]*raft.Log, numEntries)
	for i := 0; i < len(logs); i++ {
		logs[i] = &raft.Log{
			Index: uint64(i),
			Term:  0,
			Type:  raft.LogType(i),
			Data:  []byte(fmt.Sprintf("log entry %v", i)),
		}
	}

	assertStoreLogs(t, db, logs)

	assert.Equal(t, uint64(1), assertFirstIndex(t, db))
	assert.Equal(t, uint64(len(logs)), assertLastIndex(t, db))

	for i, log := range logs {
		l := assertGetLog(t, db, uint64(i+1))
		assert.Equal(t, *log, *l)
	}
}

func TestDeleteRangeFromStart(t *testing.T) {
	db := assertCreate(t, "delete_range_from_start")
	defer assertClose(t, db)

	logs := filldb(t, db)

	first := assertFirstIndex(t, db)
	toDelete := uint64(50)
	assertDeleteRange(t, db, first, toDelete)

	assert.Equal(t, first+toDelete, assertFirstIndex(t, db))

	for i, log := range logs {
		index := uint64(i) + 1
		if index <= toDelete {
			if err := db.GetLog(index, new(raft.Log)); err == nil {
				t.Fatal("expected log entry to be missing")
			}
		} else {
			l := assertGetLog(t, db, index)
			assert.Equal(t, *log, *l)
		}
	}
}

func TestDeleteRangeFromEnd(t *testing.T) {
	db := assertCreate(t, "delete_range_from_end")
	defer assertClose(t, db)

	logs := filldb(t, db)

	last := assertLastIndex(t, db)
	toDelete := uint64(50)
	assertDeleteRange(t, db, last-toDelete, last)

	assert.Equal(t, last-toDelete-1, assertLastIndex(t, db))

	for i, log := range logs {
		index := uint64(i) + 1
		if index >= last-toDelete {
			if err := db.GetLog(index, new(raft.Log)); err == nil {
				t.Fatal("expected log entry to be missing ")
			}
		} else {
			l := assertGetLog(t, db, index)
			assert.Equal(t, *log, *l)
		}
	}
}

/// ASSERTIONS

func assertCreate(t testing.TB, testName string) *LogStore {
	_ = os.RemoveAll("../test_db/raft/" + testName)
	db, err := logdb.Open("../test_db/raft/"+testName, 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	return New(db)
}

func assertClose(t testing.TB, db *LogStore) {
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertFirstIndex(t testing.TB, db *LogStore) uint64 {
	index, err := db.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func assertLastIndex(t testing.TB, db *LogStore) uint64 {
	index, err := db.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func assertGetLog(t testing.TB, db *LogStore, index uint64) *raft.Log {
	log := new(raft.Log)
	if err := db.GetLog(index, log); err != nil {
		t.Fatal(err)
	}
	return log
}

func assertStoreLog(t testing.TB, db *LogStore, log *raft.Log) {
	if err := db.StoreLog(log); err != nil {
		t.Fatal(err)
	}
}

func assertStoreLogs(t testing.TB, db *LogStore, logs []*raft.Log) {
	if err := db.StoreLogs(logs); err != nil {
		t.Fatal(err)
	}
}

func assertDeleteRange(t testing.TB, db *LogStore, min, max uint64) {
	if err := db.DeleteRange(min, max); err != nil {
		t.Fatal(err)
	}
}

/// UTILITIES

func filldb(t testing.TB, db *LogStore) []*raft.Log {
	logs := make([]*raft.Log, numEntries)
	for i := 0; i < len(logs); i++ {
		logs[i] = &raft.Log{
			Index: uint64(i),
			Term:  0,
			Type:  raft.LogType(i),
			Data:  []byte(fmt.Sprintf("log entry %v", i)),
		}
	}

	assertStoreLogs(t, db, logs)

	return logs
}
