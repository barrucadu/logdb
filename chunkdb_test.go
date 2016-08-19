package logdb

import (
	"os"
	"testing"

	"github.com/hashicorp/errwrap"
	"github.com/stretchr/testify/assert"
)

// These all test the 'LockFreeChunkDB' loading error cases, so there's no need to try other databases.

func TestChunkDB_NoOpenFile(t *testing.T) {
	if err := writeFile("test_db/no_open_file", uint8(1)); err != nil {
		t.Fatal("could not write file: ", err)
	}

	createErr := assertOpenError(t, true, "no_open_file")
	openErr := assertOpenError(t, false, "no_open_file")

	assert.True(t, errwrap.ContainsType(createErr, ErrNotDirectory))
	assert.True(t, errwrap.ContainsType(openErr, ErrNotDirectory))
}

func TestChunkDB_NoOpenMissing(t *testing.T) {
	openErr := assertOpenError(t, false, "no_open_missing")
	assert.True(t, errwrap.ContainsType(openErr, ErrPathDoesntExist))
}

func TestChunkDB_NoConcurrentOpen(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "no_concurrent_open", chunkSize)
	_, lockerror := assertOpenError(t, false, "no_concurrent_open").(*LockError)
	assert.True(t, lockerror, "expected lock error")

	assertClose(t, db)
	db2 := assertOpen(t, dbTypes["lock free chunkdb"], false, "no_concurrent_open", chunkSize)
	assertClose(t, db2)
}

func TestChunkDB_NoOpenBadFiles(t *testing.T) {
	if err := os.MkdirAll("test_db/no_open_bad_files", os.ModeDir|0755); err != nil {
		t.Fatal("could not create directory: ", err)
	}

	_ = assertOpenError(t, false, "no_open_bad_files")

	if err := writeFile("test_db/no_open_bad_files/version", uint16(42)); err != nil {
		t.Fatal("could not write dummy version file: ", err)
	}

	_ = assertOpenError(t, false, "no_open_bad_files")

	if err := writeFile("test_db/no_open_bad_files/chunk_size", uint32(1024)); err != nil {
		t.Fatal("could not write dummy version file: ", err)
	}

	err := assertOpenError(t, false, "no_open_bad_files")

	assert.True(t, errwrap.ContainsType(err, ErrUnknownVersion))
}

func TestChunkDB_CorruptOldest(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "corrupt_oldest", chunkSize)

	filldb(t, db, numEntries)

	assertTruncate(t, db, 20, 40)

	assertClose(t, db)

	// Delete the "oldest" file
	if err := os.Remove("test_db/corrupt_oldest/oldest"); err != nil {
		t.Fatal("failed to remove 'oldest' file:", err)
	}

	db2 := assertOpen(t, dbTypes["lock free chunkdb"], false, "corrupt_oldest", chunkSize)
	defer assertClose(t, db2)

	// This magic number is very dependent on both the chunk size and what exactly filldb writes.
	// 'assertClose' forces a sync, which deletes some chunk files, meaning entry 44, rather than
	// entry 1, is the oldest one.
	assert.Equal(t, uint64(16), db2.OldestID(), "oldest %v", db2.OldestID())
}

func TestChunkDB_NoEmptyNonfinalChunk(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "no_empty_nonfinal_chunk", chunkSize)
	filldb(t, db, numEntries)
	assertClose(t, db)

	if err := createFile("test_db/no_empty_nonfinal_chunk/"+initialMetaFile, 0); err != nil {
		t.Fatal("failed to truncate meta file to 0 bytes:", err)
	}

	assert.True(t, errwrap.ContainsType(assertOpenError(t, false, "no_empty_nonfinal_chunk"), ErrEmptyNonfinalChunk))
}

func TestChunkDB_ZeroSizeFinalChunk(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "zero_size_final_chunk", chunkSize)
	assertClose(t, db)

	if err := createFile("test_db/zero_size_final_chunk/"+initialChunkFile, 0); err != nil {
		t.Fatal("failed to create zero-sized chunk file:", err)
	}

	assertClose(t, assertOpen(t, dbTypes["lock free chunkdb"], false, "zero_size_final_chunk", chunkSize))
}

func TestChunkDB_NoOpenZeroSizeNonfinalChunk(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "no_open_zero_size_nonfinal_chunk", chunkSize)
	filldb(t, db, numEntries)
	assertClose(t, db)

	if err := createFile("test_db/no_open_zero_size_nonfinal_chunk/"+initialChunkFile, 0); err != nil {
		t.Fatal("failed to truncate chunk file to 0 bytes:", err)
	}

	_ = assertOpenError(t, false, "no_open_zero_size_nonfinal_chunk")
}

func TestChunkDB_MissingMetaFinalChunk(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "missing_meta_final_chunk", chunkSize)
	assertClose(t, db)

	if err := createFile("test_db/missing_meta_final_chunk/"+initialChunkFile, chunkSize); err != nil {
		t.Fatal("failed to create chunk file:", err)
	}

	assertClose(t, assertOpen(t, dbTypes["lock free chunkdb"], false, "missing_meta_final_chunk", chunkSize))
}

func TestChunkDB_NoOpenMissingMetaNonfinalChunk(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "no_open_missing_meta_nonfinal_chunk", chunkSize)
	filldb(t, db, numEntries)
	assertClose(t, db)

	if err := os.Remove("test_db/no_open_missing_meta_nonfinal_chunk/" + initialMetaFile); err != nil {
		t.Fatal("failed to delete meta file:", err)
	}

	_ = assertOpenError(t, false, "no_open_missing_meta_nonfinal_chunk")
}

func TestChunkDB_Gap(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "gap", chunkSize)
	filldb(t, db, numEntries)
	assertClose(t, db)

	if err := os.Remove("test_db/gap/chunk_3_44"); err != nil {
		t.Fatal("failed to delete chunk data file:", err)
	}

	assertClose(t, assertOpen(t, dbTypes["lock free chunkdb"], false, "gap", chunkSize))

	for _, dataFile := range []string{"chunk_0_1", "chunk_1_16", "chunk_2_30", "chunk_3_44"} {
		dataPath := "test_db/gap/" + dataFile
		metaPath := metaFilePath(dataPath)
		if _, err := os.Stat(dataPath); err == nil {
			t.Fatal("expected data file to be gone:", dataPath)
		}
		if _, err := os.Stat(metaPath); err == nil {
			t.Fatal("expected meta file to be gone:", metaPath)
		}
	}
}
