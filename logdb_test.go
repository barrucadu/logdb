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

func TestOneIndexed(t *testing.T) {
	db := assertCreate(t, "one_indexed", chunkSize)
	defer assertClose(t, db)

	assertAppend(t, db, []byte{42})
	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, uint64(2), db.NextID())
	assertGet(t, db, 1)
}

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
	assert.Equal(t, uint64(len(vs))+1, db.NextID())

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
	assert.Equal(t, uint64(len(vs))+1, db.NextID())

	for i, v := range vs {
		bs := assertGet(t, db, uint64(i+1))
		assert.Equal(t, v, bs)
	}
}

func TestAppendValue(t *testing.T) {
	db := assertCreate(t, "append_value", chunkSize)
	defer assertClose(t, db)

	vs := make([]uint64, numEntries)
	for i := 0; i < len(vs); i++ {
		vs[i] = uint64(0x0FDCBA9876543210 | i)
	}

	for _, v := range vs {
		assertAppendValue(t, db, v)
	}

	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, uint64(len(vs))+1, db.NextID())

	for i, v := range vs {
		var v2 uint64
		assertGetValue(t, db, uint64(i+1), &v2)
		assert.Equal(t, v, v2)
	}
}

func TestAppendValues(t *testing.T) {
	db := assertCreate(t, "append_values", chunkSize)
	defer assertClose(t, db)

	vs := make([]interface{}, numEntries)
	for i := 0; i < len(vs); i++ {
		vs[i] = uint64(0x0FDCBA9876543210 | i)
	}

	assertAppendValues(t, db, vs)

	assert.Equal(t, firstID, db.OldestID())
	assert.Equal(t, uint64(len(vs))+1, db.NextID())

	for i, v := range vs {
		var v2 uint64
		assertGetValue(t, db, uint64(i+1), &v2)
		assert.Equal(t, v, v2)
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

func TestForget(t *testing.T) {
	db := assertCreate(t, "forget", chunkSize)
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

func TestRollback(t *testing.T) {
	db := assertCreate(t, "rollback", chunkSize)
	defer assertClose(t, db)

	vs := filldb(t, db, numEntries)

	toRollback := uint64(50)
	newNextID := db.NextID() - toRollback
	assertRollback(t, db, newNextID)
	assert.Equal(t, newNextID, db.NextID())

	for i, v := range vs {
		if uint64(i)+1 == newNextID {
			break
		}
		bs := assertGet(t, db, uint64(i+1))
		assert.Equal(t, v, bs)
	}
}

func TestTruncate(t *testing.T) {
	db := assertCreate(t, "truncate", chunkSize)
	defer assertClose(t, db)

	vs := filldb(t, db, numEntries)

	toForget := uint64(20)
	toRollback := uint64(30)
	newOldestID := db.OldestID() + toForget
	newNextID := db.NextID() - toRollback
	assertTruncate(t, db, newOldestID, newNextID)
	assert.Equal(t, newOldestID, db.OldestID())
	assert.Equal(t, newNextID, db.NextID())

	for i, v := range vs {
		if uint64(i)+1 < newOldestID {
			continue
		}
		if uint64(i)+1 == newNextID {
			break
		}
		bs := assertGet(t, db, uint64(i+1))
		assert.Equal(t, v, bs)
	}
}

func TestPersistTruncate(t *testing.T) {
	db := assertCreate(t, "persist_truncate", chunkSize)

	vs := filldb(t, db, numEntries)

	toForget := uint64(20)
	toRollback := uint64(30)
	newOldestID := db.OldestID() + toForget
	newNextID := db.NextID() - toRollback
	assertTruncate(t, db, newOldestID, newNextID)

	assertClose(t, db)

	db2 := assertOpen(t, "persist_truncate")
	defer assertClose(t, db2)

	assert.Equal(t, newOldestID, db2.OldestID())
	assert.Equal(t, newNextID, db2.NextID())

	for i, v := range vs {
		if uint64(i)+1 < newOldestID {
			continue
		}
		if uint64(i)+1 == newNextID {
			break
		}
		bs := assertGet(t, db2, uint64(i+1))
		assert.Equal(t, v, bs)
	}
}

/// ASSERTIONS

func assertCreate(t *testing.T, testName string, cSize uint32) LogDB {
	_ = os.RemoveAll("test_db/" + testName)
	db, err := Create("test_db/"+testName, 0, cSize)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func assertOpen(t *testing.T, testName string) LogDB {
	db, err := Open("test_db/" + testName)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func assertClose(t *testing.T, db LogDB) {
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertAppend(t *testing.T, db LogDB, entry []byte) {
	if err := db.Append(entry); err != nil {
		t.Fatal(err)
	}
}

func assertAppendValue(t *testing.T, db LogDB, value interface{}) {
	if err := db.AppendValue(value); err != nil {
		t.Fatal(err)
	}
}

func assertAppendEntries(t *testing.T, db LogDB, entries [][]byte) {
	if err := db.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
}

func assertAppendValues(t *testing.T, db LogDB, values []interface{}) {
	if err := db.AppendValues(values); err != nil {
		t.Fatal(err)
	}
}

func assertGet(t *testing.T, db LogDB, id uint64) []byte {
	b, err := db.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertGetValue(t *testing.T, db LogDB, id uint64, data interface{}) {
	if err := db.GetValue(id, data); err != nil {
		t.Fatal(err)
	}
}

func assertForget(t *testing.T, db LogDB, newOldestID uint64) {
	if err := db.Forget(newOldestID); err != nil {
		t.Fatal(err)
	}
}

func assertRollback(t *testing.T, db LogDB, newNextID uint64) {
	if err := db.Rollback(newNextID); err != nil {
		t.Fatal(err)
	}
}

func assertTruncate(t *testing.T, db LogDB, newOldestID, newNextID uint64) {
	if err := db.Truncate(newOldestID, newNextID); err != nil {
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
	assert.Equal(t, uint64(len(vs))+1, db.NextID())

	return vs
}
