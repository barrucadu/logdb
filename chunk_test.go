package logdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
	"testing/quick"

	"github.com/hashicorp/errwrap"
	"github.com/stretchr/testify/assert"
)

/* ***** Filenames */

func TestChunk_Filenames_DataFileSyntax(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, is[0], sep, is[1])
		assert.True(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunk_Filenames_DataFileSyntaxLeadingZero(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s0%v%s%v", chunkPrefix, sep, is[0], sep, is[1])
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunk_Filenames_DataFileSyntaxNoID(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s%s%v", chunkPrefix, sep, sep, is[1])
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunk_Filenames_DataFileSyntaxNoOldest(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s%v%s", chunkPrefix, sep, is[0], sep)
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunk_Filenames_DataFileSyntaxIDNotUint(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s~%v%s%v", chunkPrefix, sep, is[0], sep, is[1])
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunk_Filenames_DataFileSyntaxOldestNotUint(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s%v%s~%v", chunkPrefix, sep, is[0], sep, is[1])
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunk_Filenames_DataFileNext(t *testing.T) {
	quickcheck(t, func(is [3]uint) bool {
		c := &chunk{path: fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, is[0], sep, is[1])}
		nextFileName := fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, is[0]+1, sep, is[2])
		assert.Equal(t, nextFileName, c.nextDataFileName(uint64(is[2])), "next data file name")
		return true
	})
}

func TestChunk_Filenames_MetaFilePath(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		c := &chunk{path: fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, is[0], sep, is[1])}
		assert.Equal(t, metaFilePath(c.path), c.metaFilePath(), "meta file path")
		return true
	})
}

/* ***** Metadata */

func TestChunk_Metadata_Works(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5})
	ends, err := readMetadata(metadata)
	assert.Nil(t, err, "failed to read metadata: %s", err)
	assert.Equal(t, []int32{0, 1, 2, 3, 4, 5}, ends, "ends")
}

func TestChunk_Metadata_NonContiguousIndices(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 5, 2})
	_, err := readMetadata(metadata)
	assert.True(t, errwrap.ContainsType(err, new(MetaContinuityError)), "expected continuity error")
}

func TestChunk_Metadata_NonIncreasingEnds(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 2, 0})
	_, err := readMetadata(metadata)
	assert.True(t, errwrap.ContainsType(err, new(MetaOffsetError)), "expected offset error")
}

func TestChunk_Metadata_Rollback(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 0, 1})
	ends, err := readMetadata(metadata)
	assert.Nil(t, err, "failed to read metadata: %s", err)
	assert.Equal(t, []int32{1}, ends, "failed to apply rollback, got: %v", ends)
}

func TestChunk_Metadata_Incomplete(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1})
	ends, err := readMetadata(metadata)
	assert.NotNil(t, err, "expected to not parse that, got: %v", ends)
}

func TestChunk_Metadata_IncompleteRollback(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 0})
	ends, err := readMetadata(metadata)
	assert.NotNil(t, err, "expected to not parse that, got: %v", ends)
}

/* ***** Opening */

func TestChunk_Open_BadFilePath(t *testing.T) {
	dir, fi := makeFile(t, "open_bad_file_path", "file", 1)
	_, err := openChunkFile(dir, fi, nil, 0)
	assert.True(t, errwrap.ContainsType(err, new(ChunkFileNameError)), "expected chunk file name error, got: %s", err)
}

func TestChunk_Open_BadBasedir(t *testing.T) {
	dir, fi := makeFile(t, "open_bad_basedir", initialChunkFile, 1)
	_, err := openChunkFile(dir+"incorrect!", fi, nil, 500)
	assert.True(t, errwrap.ContainsType(err, new(ReadError)), "expected read error, got: %s", err)
}

func TestChunk_Open_Directory(t *testing.T) {
	dir := "test_db/open_directory/chunk_0_1"
	if err := os.MkdirAll(dir, os.ModeDir|0755); err != nil {
		t.Fatal("error creating directory:", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal("error stating directory:", err)
	}

	_, err = openChunkFile("test_db/open_directory", fi, nil, 500)
	assert.True(t, errwrap.ContainsType(err, new(ReadError)), "expected read error, got: %s", err)
}

func TestChunk_Open_BadSize(t *testing.T) {
	dir, fi := makeFile(t, "open_bad_size", initialChunkFile, 1)
	_, err := openChunkFile(dir, fi, nil, 500)
	assert.True(t, errwrap.ContainsType(err, new(ChunkSizeError)), "expected chunk size error, got: %s", err)
}

func TestChunk_Open_BadMetadata(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "open_bad_metadata", chunkSize)
	filldb(t, db, numEntries)
	assertClose(t, db)
	if err := createFile("test_db/open_bad_metadata/"+initialMetaFile, 3); err != nil {
		t.Fatal("could not truncate metadata file:", err)
	}

	fi, err := os.Stat("test_db/open_bad_metadata/" + initialChunkFile)
	if err != nil {
		t.Fatal("error stating chunk file:", err)
	}

	_, err = openChunkFile("test_db/open_bad_metadata", fi, nil, chunkSize)
	assert.True(t, errwrap.ContainsType(err, new(ChunkMetaError)), "expected chunk meta error, got: %s", err)
}

func TestChunk_Open_MissingMetadata(t *testing.T) {
	dir, fi := makeFile(t, "open_missing_metadata", initialChunkFile, chunkSize)
	_, err := openChunkFile(dir, fi, nil, chunkSize)
	assert.True(t, errwrap.ContainsType(err, new(ReadError)), "expected read error, got: %s", err)
}

func TestChunk_Open_BadContinuity(t *testing.T) {
	db := assertOpen(t, dbTypes["lock free chunkdb"], true, "open_bad_continuity", chunkSize)
	filldb(t, db, numEntries)
	assertClose(t, db)

	fi, err := os.Stat("test_db/open_bad_continuity/" + initialChunkFile)
	if err != nil {
		t.Fatal("error stating chunk file:", err)
	}

	_, err = openChunkFile("test_db/open_bad_continuity", fi, &chunk{oldest: 90}, chunkSize)
	assert.True(t, errwrap.ContainsType(err, new(ChunkContinuityError)), "expected chunk continuity error, got: %s", err)
}

/// HELPERS

func quickcheck(t *testing.T, f interface{}) {
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func makeMetadata(t testing.TB, vals []int32) io.Reader {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, vals); err != nil {
		t.Fatal(err)
	}
	return buf
}

func makeFile(t testing.TB, testName, fileName string, size uint32) (string, os.FileInfo) {
	dir := "test_db/" + testName
	path := dir + "/" + fileName
	if err := os.MkdirAll(dir, os.ModeDir|0755); err != nil {
		t.Fatal("error creating directory:", err)
	}
	if err := createFile(path, size); err != nil {
		t.Fatal("error creating test file:", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal("error stating test file:", err)
	}
	return dir, fi
}
