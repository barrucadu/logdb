package logdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/errwrap"
	"github.com/stretchr/testify/assert"
)

const (
	chunkSize  = 113
	firstID    = uint64(1)
	numEntries = 255
)

/* ***** OldestID / NewestID */

func TestOneIndexed(t *testing.T) {
	db := assertCreate(t, "one_indexed", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte{42})
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
	assertGet(t, db, 1)
}

/* ***** Append */

func TestAppend(t *testing.T) {
	db := assertCreate(t, "append", chunkSize)
	defer assertClose(t, db)

	vs := make([][]byte, numEntries)
	for i := 0; i < len(vs); i++ {
		vs[i] = []byte(fmt.Sprintf("entry-%v", i))
	}

	for _, v := range vs {
		assertAppend(t, db, v)
	}

	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, uint64(len(vs)), db.NewestID())

	for i, v := range vs {
		bs := assertGet(t, db, uint64(i+1))
		assert.Equal(t, v, bs)
	}
}

func TestAppendEntries(t *testing.T) {
	db := assertCreate(t, "append_entries", chunkSize)
	defer assertClose(t, db)

	vs := make([][]byte, numEntries)
	for i := 0; i < len(vs); i++ {
		vs[i] = []byte(fmt.Sprintf("entry-%v", i))
	}

	assertAppendEntries(t, db, vs)

	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, uint64(len(vs)), db.NewestID())

	for i, v := range vs {
		bs := assertGet(t, db, uint64(i+1))
		assert.Equal(t, v, bs)
	}
}

func TestPersist(t *testing.T) {
	db := assertCreate(t, "persist", chunkSize)

	vs := filldb(t, db, numEntries)

	assertClose(t, db)

	db2 := assertOpen(t, "persist")
	defer assertClose(t, db2)

	for i, v := range vs {
		bs := assertGet(t, db2, uint64(i+1))
		assert.Equal(t, v, bs)
	}
}

func TestNoAppendTooBig(t *testing.T) {
	db := assertCreate(t, "no_append_too_big", 1)
	defer assertClose(t, db)

	assert.Equal(t, ErrTooBig, db.Append([]byte{1, 2, 3, 4, 5}), "expected Append to fail")
}

/* ***** Get */

func TestNoGetOutOfRange(t *testing.T) {
	db := assertCreate(t, "no_get_out_of_range", chunkSize)
	defer assertClose(t, db)

	_, err := db.Get(0)
	assert.Equal(t, ErrIDOutOfRange, err)
	_, err = db.Get(1)
	assert.Equal(t, ErrIDOutOfRange, err)
	_, err = db.Get(2)
	assert.Equal(t, ErrIDOutOfRange, err)
}

/* ***** Forget */

func TestForgetZero(t *testing.T) {
	db := assertCreate(t, "forget_zero", chunkSize)
	defer assertClose(t, db)

	assertForget(t, db, uint64(0))
	assert.Equal(t, uint64(0), db.OldestID())
	assert.Equal(t, uint64(0), db.NewestID())
}

func TestForgetOne(t *testing.T) {
	db := assertCreate(t, "forget_one", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))

	assertForget(t, db, firstID)
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
}

func TestForgetFuture(t *testing.T) {
	db := assertCreate(t, "forget_future", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))

	assert.Equal(t, ErrIDOutOfRange, assertForgetError(t, db, firstID+1))
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
}

func TestForgetPast(t *testing.T) {
	db := assertCreate(t, "forget_past", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))

	assertForget(t, db, 0)
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
}

func TestForgetMany(t *testing.T) {
	db := assertCreate(t, "forget_many", chunkSize)
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
}

/* ***** Rollback */

func TestRollbackZero(t *testing.T) {
	db := assertCreate(t, "rollback_zero", chunkSize)
	defer assertClose(t, db)

	assertRollback(t, db, uint64(0))
	assert.Equal(t, uint64(0), db.OldestID())
	assert.Equal(t, uint64(0), db.NewestID())
}

func TestRollbackOne(t *testing.T) {
	db := assertCreate(t, "rollback_one", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))

	assertRollback(t, db, firstID)
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
}

func TestRollbackFuture(t *testing.T) {
	db := assertCreate(t, "rollback_future", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))

	assertRollback(t, db, firstID+1)
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
}

func TestRollbackPast(t *testing.T) {
	db := assertCreate(t, "rollback_past", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))

	assert.Equal(t, ErrIDOutOfRange, assertRollbackError(t, db, 0))
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
}

func TestRollbackMany(t *testing.T) {
	db := assertCreate(t, "rollback_many", chunkSize)
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
}

/* ***** Truncate */

func TestTruncateZero(t *testing.T) {
	db := assertCreate(t, "truncate_zero", chunkSize)
	defer assertClose(t, db)

	assertTruncate(t, db, 0, 0)
	assert.Equal(t, uint64(0), db.OldestID())
	assert.Equal(t, uint64(0), db.NewestID())
}

func TestTruncateOne(t *testing.T) {
	db := assertCreate(t, "truncate_one", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))

	assertTruncate(t, db, firstID, firstID)
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
}

func TestTruncateOldFuture(t *testing.T) {
	db := assertCreate(t, "truncate_old_future", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))

	assert.Equal(t, ErrIDOutOfRange, assertTruncateError(t, db, 50, 999))
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, firstID, db.NewestID())
}

func TestTruncateNewPast(t *testing.T) {
	db := assertCreate(t, "truncate_new_past", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte("hello world"))
	assertAppend(t, db, []byte("hello world"))
	assertForget(t, db, 2)

	assert.Equal(t, ErrIDOutOfRange, assertTruncateError(t, db, firstID, firstID))
	assert.Equal(t, uint64(2), db.OldestID())
	assert.Equal(t, uint64(2), db.NewestID())
}

func TestTruncateMany(t *testing.T) {
	db := assertCreate(t, "truncate_many", chunkSize)
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
}

func TestNoTruncateNewLessOld(t *testing.T) {
	db := assertCreate(t, "truncate_new_less_old", chunkSize)
	defer assertClose(t, db)

	vs := filldb(t, db, numEntries)
	assert.Equal(t, ErrIDOutOfRange, assertTruncateError(t, db, 100, 50))

	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, uint64(len(vs)), db.NewestID())
}

func TestPersistTruncate(t *testing.T) {
	db := assertCreate(t, "persist_truncate", chunkSize)

	vs := filldb(t, db, numEntries)

	toForget := uint64(20)
	toRollback := uint64(30)
	newOldestID := db.OldestID() + toForget
	newNewestID := db.NewestID() - toRollback
	assertTruncate(t, db, newOldestID, newNewestID)

	assertClose(t, db)

	db2 := assertOpen(t, "persist_truncate")
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
}

/* ***** Corruption */

func TestNoOpenFile(t *testing.T) {
	if err := writeFile("test_db/no_open_file", uint8(1)); err != nil {
		t.Fatal("could not write file: ", err)
	}

	createErr := assertCreateError(t, "no_open_file")
	openErr := assertOpenError(t, "no_open_file")

	assert.True(t, errwrap.ContainsType(createErr, ErrNotDirectory))
	assert.True(t, errwrap.ContainsType(openErr, ErrNotDirectory))
}

func TestNoOpenMissing(t *testing.T) {
	openErr := assertOpenError(t, "no_open_missing")
	assert.True(t, errwrap.ContainsType(openErr, ErrPathDoesntExist))
}

func TestNoConcurrentOpen(t *testing.T) {
	db := assertCreate(t, "no_concurrent_open", chunkSize)
	_, lockerror := assertOpenError(t, "no_concurrent_open").(*LockError)
	assert.True(t, lockerror, "expected lock error")

	assertClose(t, db)
	db2 := assertOpen(t, "no_concurrent_open")
	assertClose(t, db2)
}

func TestNoOpenBadFiles(t *testing.T) {
	if err := os.MkdirAll("test_db/no_open_bad_files", os.ModeDir|0755); err != nil {
		t.Fatal("could not create directory: ", err)
	}

	_ = assertOpenError(t, "no_open_bad_files")

	if err := writeFile("test_db/no_open_bad_files/version", uint16(42)); err != nil {
		t.Fatal("could not write dummy version file: ", err)
	}

	_ = assertOpenError(t, "no_open_bad_files")

	if err := writeFile("test_db/no_open_bad_files/chunk_size", uint32(1024)); err != nil {
		t.Fatal("could not write dummy version file: ", err)
	}

	err := assertOpenError(t, "no_open_bad_files")

	assert.True(t, errwrap.ContainsType(err, ErrUnknownVersion))
}

func TestCorruptOldest(t *testing.T) {
	db := assertCreate(t, "corrupt_oldest", chunkSize)

	filldb(t, db, numEntries)

	assertTruncate(t, db, 20, 40)

	assertClose(t, db)

	// Delete the "oldest" file
	if err := os.Remove("test_db/corrupt_oldest/oldest"); err != nil {
		t.Fatal("failed to remove 'oldest' file:", err)
	}

	db2 := assertOpen(t, "corrupt_oldest")
	defer assertClose(t, db2)

	// This magic number is very dependent on both the chunk size and what exactly filldb writes.
	// 'assertClose' forces a sync, which deletes some chunk files, meaning entry 44, rather than
	// entry 1, is the oldest one.
	assert.Equal(t, uint64(16), db2.OldestID(), "oldest %v", db2.OldestID())
}

func TestNoEmptyNonfinalChunk(t *testing.T) {
	db := assertCreate(t, "no_empty_nonfinal_chunk", chunkSize)
	filldb(t, db, numEntries)
	assertClose(t, db)

	if err := createFile("test_db/no_empty_nonfinal_chunk/"+initialMetaFile, 0); err != nil {
		t.Fatal("failed to truncate meta file to 0 bytes:", err)
	}

	assert.True(t, errwrap.ContainsType(assertOpenError(t, "no_empty_nonfinal_chunk"), ErrEmptyNonfinalChunk))
}

/* ***** Syncing */

func TestDisablePerioidSync(t *testing.T) {
	// Allocate a huge chunk file, because a sync is forced when a
	// chunk is filled, regardless of the periodic syncing
	// behaviour.
	db := assertCreate(t, "disable_periodic_sync", 1024*1024*1024)
	defer assertClose(t, db)

	assertSetSync(t, db, -1)

	// Write far more than the defauly syncing period.
	filldb(t, db, numEntries*2)

	// Check there is no metadata.
	if fi, err := os.Stat("test_db/disable_periodic_sync/" + initialMetaFile); !(err == nil && fi.Size() == 0) {
		t.Fatal("expected no metadata, got:", fi.Size(), err)
	}
}

func TestExplicitSync(t *testing.T) {
	db := assertCreate(t, "explicit_sync", 1024*1024*1024)
	defer assertClose(t, db)

	assertSetSync(t, db, -1)
	filldb(t, db, numEntries*2)

	assertSync(t, db)

	// Check there is metadata.
	if fi, err := os.Stat("test_db/disable_periodic_sync/" + initialMetaFile); !(err == nil && fi.Size() > 0) {
		t.Fatal("expected metadata, got:", fi.Size(), err)
	}
}

func TestSetSyncSyncs(t *testing.T) {
	db := assertCreate(t, "setsync_syncs", 1024*1024*1024)
	defer assertClose(t, db)

	assertSetSync(t, db, -1)
	filldb(t, db, numEntries*2)
	assertSetSync(t, db, 3)

	if fi, err := os.Stat("test_db/disable_periodic_sync/" + initialMetaFile); !(err == nil && fi.Size() > 0) {
		t.Fatal("expected metadata to exist, got:", fi.Size(), err)
	}
}

/* ***** Closing */

func TestNoUseClosed(t *testing.T) {
	db := assertCreate(t, "no_use_closed", chunkSize)
	assertClose(t, db)

	assert.Equal(t, ErrClosed, db.Append(nil), "expected Append to fail")
	assert.Equal(t, ErrClosed, db.Forget(0), "expected Forget to fail")
	assert.Equal(t, ErrClosed, db.Rollback(0), "expected Rollback to fail")
	assert.Equal(t, ErrClosed, db.Truncate(0, 0), "expected Truncate to fail")
	assert.Equal(t, ErrClosed, db.SetSync(0), "expected SetSync to fail")
	assert.Equal(t, ErrClosed, db.Sync(), "expected Sync to fail")
	assert.Equal(t, ErrClosed, db.Close(), "expected Close to fail")

	_, err := db.Get(0)
	assert.Equal(t, ErrClosed, err, "expected Get to fail")
}

/// ASSERTIONS

func assertCreate(t *testing.T, testName string, cSize uint32) *LogDB {
	_ = os.RemoveAll("test_db/" + testName)
	db, err := Open("test_db/"+testName, cSize, true)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func assertCreateError(t *testing.T, testName string) error {
	_, err := Open("test_db/"+testName, 0, true)
	if err == nil {
		t.Fatal("should not be able to create database")
	}
	return err
}

func assertOpen(t *testing.T, testName string) *LogDB {
	db, err := Open("test_db/"+testName, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func assertOpenError(t *testing.T, testName string) error {
	_, err := Open("test_db/"+testName, 0, false)
	if err == nil {
		t.Fatal("should not be able to open database")
	}
	return err
}

func assertClose(t *testing.T, db *LogDB) {
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertAppend(t *testing.T, db *LogDB, entry []byte) {
	if err := db.Append(entry); err != nil {
		t.Fatal(err)
	}
}

func assertAppendEntries(t *testing.T, db *LogDB, entries [][]byte) {
	if err := db.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
}

func assertGet(t *testing.T, db *LogDB, id uint64) []byte {
	b, err := db.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertForget(t *testing.T, db *LogDB, newOldestID uint64) {
	if err := db.Forget(newOldestID); err != nil {
		t.Fatal(err)
	}
}

func assertForgetError(t *testing.T, db *LogDB, newOldestID uint64) error {
	err := db.Forget(newOldestID)
	if err == nil {
		t.Fatal("should not be able to forget")
	}
	return err
}

func assertRollback(t *testing.T, db *LogDB, newNewestID uint64) {
	if err := db.Rollback(newNewestID); err != nil {
		t.Fatal(err)
	}
}

func assertRollbackError(t *testing.T, db *LogDB, newNewestID uint64) error {
	err := db.Rollback(newNewestID)
	if err == nil {
		t.Fatal("should not be able to rollback")
	}
	return err
}

func assertTruncate(t *testing.T, db *LogDB, newOldestID, newNewestID uint64) {
	if err := db.Truncate(newOldestID, newNewestID); err != nil {
		t.Fatal(err)
	}
}

func assertTruncateError(t *testing.T, db *LogDB, newOldestID, newNewestID uint64) error {
	err := db.Truncate(newOldestID, newNewestID)
	if err == nil {
		t.Fatal("should not be able to truncate")
	}
	return err
}

func assertSetSync(t *testing.T, db *LogDB, every int) {
	if err := db.SetSync(every); err != nil {
		t.Fatal(err)
	}
}

func assertSync(t *testing.T, db *LogDB) {
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
}

/// HELPERS

func filldb(t *testing.T, db *LogDB, num int) [][]byte {
	vs := make([][]byte, num)
	for i := 0; i < num; i++ {
		vs[i] = []byte(fmt.Sprintf("entry-%v", i))
	}

	assertAppendEntries(t, db, vs)

	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, uint64(len(vs)), db.NewestID())

	return vs
}
