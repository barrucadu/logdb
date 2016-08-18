package logdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	chunkSize  = 113
	firstID    = uint64(1)
	numEntries = 255
)

var dbTypes = map[string]LogDB{
	"chunkdb":           &ChunkDB{},
	"lock free chunkdb": &LockFreeChunkDB{},
}

/* ***** OldestID / NewestID */

func TestOneIndexed(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "one_indexed", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte{42})
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
			assertGet(t, db, 1)
		}()
	}
}

/* ***** Append */

func TestAppend(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "append", chunkSize)
			defer assertClose(t, db)

			vs := make([][]byte, numEntries)
			for i := 0; i < len(vs); i++ {
				vs[i] = []byte(fmt.Sprintf("entry-%v", i))
			}

			for i, v := range vs {
				idx := assertAppend(t, db, v)
				assert.Equal(t, uint64(i+1), idx)
			}

			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, uint64(len(vs)), db.NewestID())

			for i, v := range vs {
				bs := assertGet(t, db, uint64(i+1))
				assert.Equal(t, v, bs)
			}
		}()
	}
}

func TestAppendEntries(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "append_entries", chunkSize)
			defer assertClose(t, db)

			vs := make([][]byte, numEntries)
			for i := 0; i < len(vs); i++ {
				vs[i] = []byte(fmt.Sprintf("entry-%v", i))
			}

			idx := assertAppendEntries(t, db, vs)
			assert.Equal(t, uint64(1), idx)

			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, uint64(len(vs)), db.NewestID())

			for i, v := range vs {
				bs := assertGet(t, db, uint64(i+1))
				assert.Equal(t, v, bs)
			}
		}()
	}
}

func TestNoAppendTooBig(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "no_append_too_big", 1)
			defer assertClose(t, db)

			_, err := db.Append([]byte{1, 2, 3, 4, 5})
			assert.Equal(t, ErrTooBig, err, "expected Append to fail")
		}()
	}
}

/* ***** Get */

func TestNoGetOutOfRange(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "no_get_out_of_range", chunkSize)
			defer assertClose(t, db)

			_, err := db.Get(0)
			assert.Equal(t, ErrIDOutOfRange, err)
			_, err = db.Get(1)
			assert.Equal(t, ErrIDOutOfRange, err)
			_, err = db.Get(2)
			assert.Equal(t, ErrIDOutOfRange, err)
		}()
	}
}

/* ***** Forget */

func TestForgetZero(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "forget_zero", chunkSize)
			defer assertClose(t, db)

			assertForget(t, db, uint64(0))
			assert.Equal(t, uint64(0), db.OldestID())
			assert.Equal(t, uint64(0), db.NewestID())
		}()
	}
}

func TestForgetOne(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "forget_one", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))

			assertForget(t, db, firstID)
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
		}()
	}
}

func TestForgetFuture(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "forget_future", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))

			assert.Equal(t, ErrIDOutOfRange, assertForgetError(t, db, firstID+1))
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
		}()
	}
}

func TestForgetPast(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "forget_past", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))

			assertForget(t, db, 0)
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
		}()
	}
}

func TestForgetMany(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "forget_many", chunkSize)
			defer assertClose(t, db)

			vs := filldb(t, db, numEntries)

			toForget := uint64(50)
			newOldestID := db.OldestID() + toForget
			assertForget(t, db, newOldestID)
			assert.Equal(t, newOldestID, db.OldestID())

			for i, v := range vs {
				if uint64(i)+1 < newOldestID {
					continue
				}
				bs := assertGet(t, db, uint64(i+1))
				assert.Equal(t, v, bs)
			}
		}()
	}
}

/* ***** Rollback */

func TestRollbackZero(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "rollback_zero", chunkSize)
			defer assertClose(t, db)

			assertRollback(t, db, uint64(0))
			assert.Equal(t, uint64(0), db.OldestID())
			assert.Equal(t, uint64(0), db.NewestID())
		}()
	}
}

func TestRollbackOne(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "rollback_one", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))

			assertRollback(t, db, firstID)
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
		}()
	}
}

func TestRollbackFuture(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "rollback_future", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))

			assertRollback(t, db, firstID+1)
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
		}()
	}
}

func TestRollbackPast(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "rollback_past", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))

			assert.Equal(t, ErrIDOutOfRange, assertRollbackError(t, db, 0))
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
		}()
	}
}

func TestRollbackMany(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "rollback_many", chunkSize)
			defer assertClose(t, db)

			vs := filldb(t, db, numEntries)

			toRollback := uint64(50)
			newNewestID := db.NewestID() - toRollback
			assertRollback(t, db, newNewestID)
			assert.Equal(t, newNewestID, db.NewestID())

			for i, v := range vs {
				if uint64(i)+1 > newNewestID {
					break
				}
				bs := assertGet(t, db, uint64(i+1))
				assert.Equal(t, v, bs)
			}
		}()
	}
}

/* ***** Truncate */

func TestTruncateZero(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "truncate_zero", chunkSize)
			defer assertClose(t, db)

			assertTruncate(t, db, 0, 0)
			assert.Equal(t, uint64(0), db.OldestID())
			assert.Equal(t, uint64(0), db.NewestID())
		}()
	}
}

func TestTruncateOne(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "truncate_one", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))

			assertTruncate(t, db, firstID, firstID)
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
		}()
	}
}

func TestTruncateOldFuture(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "truncate_old_future", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))

			assert.Equal(t, ErrIDOutOfRange, assertTruncateError(t, db, 50, 999))
			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, firstID, db.NewestID())
		}()
	}
}

func TestTruncateNewPast(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "truncate_new_past", chunkSize)
			defer assertClose(t, db)

			assertAppend(t, db, []byte("hello world"))
			assertAppend(t, db, []byte("hello world"))
			assertForget(t, db, 2)

			assert.Equal(t, ErrIDOutOfRange, assertTruncateError(t, db, firstID, firstID))
			assert.Equal(t, uint64(2), db.OldestID())
			assert.Equal(t, uint64(2), db.NewestID())
		}()
	}
}

func TestTruncateMany(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "truncate_many", chunkSize)
			defer assertClose(t, db)

			vs := filldb(t, db, numEntries)

			toForget := uint64(20)
			toRollback := uint64(30)
			newOldestID := db.OldestID() + toForget
			newNewestID := db.NewestID() - toRollback
			assertTruncate(t, db, newOldestID, newNewestID)
			assert.Equal(t, newOldestID, db.OldestID())
			assert.Equal(t, newNewestID, db.NewestID())

			for i, v := range vs {
				if uint64(i)+1 < newOldestID {
					continue
				}
				if uint64(i)+1 > newNewestID {
					break
				}
				bs := assertGet(t, db, uint64(i+1))
				assert.Equal(t, v, bs)
			}
		}()
	}
}

func TestNoTruncateNewLessOld(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "truncate_new_less_old", chunkSize)
			defer assertClose(t, db)

			vs := filldb(t, db, numEntries)
			assert.Equal(t, ErrIDOutOfRange, assertTruncateError(t, db, 100, 50))

			assert.Equal(t, firstID, db.OldestID())
			assert.Equal(t, uint64(len(vs)), db.NewestID())
		}()
	}
}

/* ***** Persistence */

func TestPersist(t *testing.T) {
	for dbName, dbType := range dbTypes {
		// This test only makes sense for PersistDBs
		if _, ok := dbType.(PersistDB); !ok {
			continue
		}

		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "persist", chunkSize)

			vs := filldb(t, db, numEntries)

			assertClose(t, db)

			db2 := assertOpen(t, dbType, false, "persist", chunkSize)
			defer assertClose(t, db2)

			for i, v := range vs {
				bs := assertGet(t, db2, uint64(i+1))
				assert.Equal(t, v, bs)
			}
		}()
	}
}

func TestPersistTruncate(t *testing.T) {
	for dbName, dbType := range dbTypes {
		// This test only makes sense for PersistDBs
		if _, ok := dbType.(PersistDB); !ok {
			continue
		}

		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "persist_truncate", chunkSize)

			vs := filldb(t, db, numEntries)

			toForget := uint64(20)
			toRollback := uint64(30)
			newOldestID := db.OldestID() + toForget
			newNewestID := db.NewestID() - toRollback
			assertTruncate(t, db, newOldestID, newNewestID)

			assertClose(t, db)

			db2 := assertOpen(t, dbType, false, "persist_truncate", chunkSize)
			defer assertClose(t, db2)

			assert.Equal(t, newOldestID, db2.OldestID())
			assert.Equal(t, newNewestID, db2.NewestID())

			for i, v := range vs {
				if uint64(i)+1 < newOldestID {
					continue
				}
				if uint64(i)+1 > newNewestID {
					break
				}
				bs := assertGet(t, db2, uint64(i+1))
				assert.Equal(t, v, bs)
			}
		}()
	}
}

func TestDisablePerioidSync(t *testing.T) {
	for dbName, dbType := range dbTypes {
		// This test only makes sense for PersistDBs
		if _, ok := dbType.(PersistDB); !ok {
			continue
		}

		t.Logf("Database: %s\n", dbName)
		func() {
			// Allocate a huge chunk file, because a sync is forced when a
			// chunk is filled, regardless of the periodic syncing
			// behaviour.
			db := assertOpen(t, dbType, true, "disable_periodic_sync", 1024*1024*1024)
			defer assertClose(t, db)

			persistdb, _ := db.(PersistDB)

			assertSetSync(t, persistdb, -1)

			// Write far more than the defauly syncing period.
			filldb(t, db, numEntries*2)

			// Check there is no metadata.
			if fi, err := os.Stat("test_db/disable_periodic_sync/" + initialMetaFile); !(err == nil && fi.Size() == 0) {
				t.Fatal("expected no metadata, got:", fi.Size(), err)
			}
		}()
	}
}

func TestExplicitSync(t *testing.T) {
	for dbName, dbType := range dbTypes {
		// This test only makes sense for PersistDBs
		if _, ok := dbType.(PersistDB); !ok {
			continue
		}

		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "explicit_sync", 1024*1024*1024)
			defer assertClose(t, db)

			persistdb, _ := db.(PersistDB)

			assertSetSync(t, persistdb, -1)
			filldb(t, db, numEntries*2)

			assertSync(t, persistdb)

			// Check there is metadata.
			if fi, err := os.Stat("test_db/disable_periodic_sync/" + initialMetaFile); !(err == nil && fi.Size() > 0) {
				t.Fatal("expected metadata, got:", fi.Size(), err)
			}
		}()
	}
}

func TestSetSyncSyncs(t *testing.T) {
	for dbName, dbType := range dbTypes {
		// This test only makes sense for PersistDBs
		if _, ok := dbType.(PersistDB); !ok {
			continue
		}

		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "setsync_syncs", 1024*1024*1024)
			defer assertClose(t, db)

			persistdb, _ := db.(PersistDB)

			assertSetSync(t, persistdb, -1)
			filldb(t, db, numEntries*2)
			assertSetSync(t, persistdb, 3)

			if fi, err := os.Stat("test_db/disable_periodic_sync/" + initialMetaFile); !(err == nil && fi.Size() > 0) {
				t.Fatal("expected metadata to exist, got:", fi.Size(), err)
			}
		}()
	}
}

/* ***** Closing */

func TestNoUseClosed(t *testing.T) {
	for dbName, dbType := range dbTypes {
		t.Logf("Database: %s\n", dbName)
		func() {
			db := assertOpen(t, dbType, true, "no_use_closed", chunkSize)
			assertClose(t, db)

			_, err := db.Append(nil)
			assert.Equal(t, ErrClosed, err, "expected Append to fail")
			_, err = db.Get(0)
			assert.Equal(t, ErrClosed, err, "expected Get to fail")
			assert.Equal(t, ErrClosed, db.Forget(0), "expected Forget to fail")
			assert.Equal(t, ErrClosed, db.Rollback(0), "expected Rollback to fail")
			assert.Equal(t, ErrClosed, db.Truncate(0, 0), "expected Truncate to fail")

			if persistdb, ok := db.(PersistDB); ok {
				assert.Equal(t, ErrClosed, persistdb.SetSync(0), "expected SetSync to fail")
				assert.Equal(t, ErrClosed, persistdb.Sync(), "expected Sync to fail")
			}

			if closedb, ok := db.(CloseDB); ok {
				assert.Equal(t, ErrClosed, closedb.Close(), "expected Close to fail")
			}
		}()
	}
}

/// ASSERTIONS

func assertOpen(t *testing.T, dbType LogDB, create bool, testName string, cSize uint32) LogDB {
	testDir := "test_db/" + testName

	if create {
		_ = os.RemoveAll(testDir)
	}
	lfdb, err := Open(testDir, cSize, create)
	if err != nil {
		t.Fatal(err)
	}

	var db LogDB
	switch dbType.(type) {
	case *ChunkDB:
		db = WrapForConcurrency(lfdb)
	case *LockFreeChunkDB:
		db = lfdb
	default:
		t.Fatal("unknown database type: ", dbType)
	}
	return db
}

func assertOpenError(t *testing.T, create bool, testName string) error {
	_, err := Open("test_db/"+testName, 0, create)
	if err == nil {
		t.Fatal("should not be able to create or open database")
	}
	return err
}

func assertClose(t *testing.T, db LogDB) {
	closedb, ok := db.(CloseDB)
	if !ok {
		return
	}
	if err := closedb.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertAppend(t *testing.T, db LogDB, entry []byte) uint64 {
	idx, err := db.Append(entry)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func assertAppendEntries(t *testing.T, db LogDB, entries [][]byte) uint64 {
	idx, err := db.AppendEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func assertGet(t *testing.T, db LogDB, id uint64) []byte {
	b, err := db.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertForget(t *testing.T, db LogDB, newOldestID uint64) {
	if err := db.Forget(newOldestID); err != nil {
		t.Fatal(err)
	}
}

func assertForgetError(t *testing.T, db LogDB, newOldestID uint64) error {
	err := db.Forget(newOldestID)
	if err == nil {
		t.Fatal("should not be able to forget")
	}
	return err
}

func assertRollback(t *testing.T, db LogDB, newNewestID uint64) {
	if err := db.Rollback(newNewestID); err != nil {
		t.Fatal(err)
	}
}

func assertRollbackError(t *testing.T, db LogDB, newNewestID uint64) error {
	err := db.Rollback(newNewestID)
	if err == nil {
		t.Fatal("should not be able to rollback")
	}
	return err
}

func assertTruncate(t *testing.T, db LogDB, newOldestID, newNewestID uint64) {
	if err := db.Truncate(newOldestID, newNewestID); err != nil {
		t.Fatal(err)
	}
}

func assertTruncateError(t *testing.T, db LogDB, newOldestID, newNewestID uint64) error {
	err := db.Truncate(newOldestID, newNewestID)
	if err == nil {
		t.Fatal("should not be able to truncate")
	}
	return err
}

func assertSetSync(t *testing.T, db PersistDB, every int) {
	if err := db.SetSync(every); err != nil {
		t.Fatal(err)
	}
}

func assertSync(t *testing.T, db PersistDB) {
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
}

/// HELPERS

func filldb(t *testing.T, db LogDB, num int) [][]byte {
	vs := make([][]byte, num)
	for i := 0; i < num; i++ {
		vs[i] = []byte(fmt.Sprintf("entry-%v", i))
	}

	assertAppendEntries(t, db, vs)

	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, uint64(len(vs)), db.NewestID())

	return vs
}
