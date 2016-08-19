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

var dbTypes = map[string]logdb.LogDB{
	"chunkdb":           &logdb.ChunkDB{},
	"lock free chunkdb": &logdb.LockFreeChunkDB{},
	"inmem":             &logdb.InMemDB{},
}

func TestRaft_EmptyIndices(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, false, true, "empty_indices")
			defer assertClose(t, db)

			assert.Equal(t, uint64(0), assertFirstIndex(t, db))
			assert.Equal(t, uint64(0), assertLastIndex(t, db))
		}()
	}
}

func TestRaft_StoreLog(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, false, true, "store_log")
			defer assertClose(t, db)

			logs := make([]*raft.Log, numEntries)
			for i := 0; i < len(logs); i++ {
				logs[i] = &raft.Log{
					Index: uint64(i) + 1,
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
		}()
	}
}

func TestRaft_StoreLogs(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, false, true, "store_logs")
			defer assertClose(t, db)

			logs := make([]*raft.Log, numEntries)
			for i := 0; i < len(logs); i++ {
				logs[i] = &raft.Log{
					Index: uint64(i) + 1,
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
		}()
	}
}

func TestRaft_DeleteRangeFromStart(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, false, true, "delete_range_from_start")
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
		}()
	}
}

func TestRaft_DeleteRangeFromEnd(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, false, true, "delete_range_from_end")
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
		}()
	}
}

func TestRaft_Offset(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, false, true, "offset")
			defer assertClose(t, db)

			off := uint64(42)
			logs := filldboff(t, db, off)

			for i, log := range logs {
				l := assertGetLog(t, db, uint64(i)+off)
				assert.Equal(t, *log, *l)
			}
		}()
	}
}

func TestRaft_PersistOffset(t *testing.T) {
	for dbName, dbType := range dbTypes {
		// This test only makes sense for PersistDBs
		if _, ok := dbType.(logdb.PersistDB); !ok {
			continue
		}

		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, false, true, "persist_offset")

			off := uint64(42)
			logs := filldboff(t, db, off)

			assertClose(t, db)
			db2 := assertOpen(t, dbType, false, false, "persist_offset")
			defer assertClose(t, db2)

			for i, log := range logs {
				l := assertGetLog(t, db2, uint64(i)+off)
				assert.Equal(t, *log, *l)
			}
		}()
	}
}

/// ASSERTIONS

func assertOpen(t testing.TB, dbType logdb.LogDB, forBench bool, create bool, testName string) *LogStore {
	// InMemDB has no disk storage (duh)
	if _, ok := dbType.(*logdb.InMemDB); ok {
		ldb, err := New(new(logdb.InMemDB))
		if err != nil {
			t.Fatal("couldnot create inmem store: ", err)
		}
		return ldb
	}

	testDir := "../test_db/raft/" + testName
	if forBench {
		testDir = "../test_db/raft-bench/" + testName
	}

	if create {
		_ = os.RemoveAll(testDir)
	}
	db, err := logdb.Open(testDir, 1024*1024, create)
	if err != nil {
		t.Fatal(err)
	}

	var ldb *LogStore

	switch dbType.(type) {
	case *logdb.ChunkDB:
		var err error
		if ldb, err = New(logdb.WrapForConcurrency(db)); err != nil {
			t.Fatal(err)
		}
	case *logdb.LockFreeChunkDB:
		var err error
		if ldb, err = New(db); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("unknown database type: ", dbType)
	}

	return ldb
}

func assertClose(t testing.TB, db *LogStore) {
	closedb, ok := db.LogDB.(logdb.CloseDB)
	if !ok {
		return
	}
	if err := closedb.Close(); err != nil {
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
	return filldboff(t, db, 1)
}

func filldboff(t testing.TB, db *LogStore, offset uint64) []*raft.Log {
	logs := make([]*raft.Log, numEntries)
	for i := 0; i < len(logs); i++ {
		logs[i] = &raft.Log{
			Index: uint64(i) + offset,
			Term:  0,
			Type:  raft.LogType(i),
			Data:  []byte(fmt.Sprintf("log entry %v", i)),
		}
	}

	assertStoreLogs(t, db, logs)

	return logs
}
