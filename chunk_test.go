package logdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
)

func TestChunkDataFileSyntax(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, is[0], sep, is[1])
		assert.True(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunkDataFileSyntaxLeadingZero(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s0%v%s%v", chunkPrefix, sep, is[0], sep, is[1])
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunkDataFileSyntaxNoID(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s%s%v", chunkPrefix, sep, sep, is[1])
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunkDataFileSyntaxNoOldest(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s%v%s", chunkPrefix, sep, is[0], sep)
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunkDataFileSyntaxIDNotUint(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s~%v%s%v", chunkPrefix, sep, is[0], sep, is[1])
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunkDataFileSyntaxOldestNotUint(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		chunkFileName := fmt.Sprintf("%s%s%v%s~%v", chunkPrefix, sep, is[0], sep, is[1])
		assert.False(t, isBasenameChunkDataFile(chunkFileName), chunkFileName)
		return true
	})
}

func TestChunkDataFileNext(t *testing.T) {
	quickcheck(t, func(is [3]uint) bool {
		c := &chunk{path: fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, is[0], sep, is[1])}
		nextFileName := fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, is[0]+1, sep, is[2])
		assert.Equal(t, nextFileName, c.nextDataFileName(uint64(is[2])), "next data file name")
		return true
	})
}

func TestChunkMetaFilePath(t *testing.T) {
	quickcheck(t, func(is [2]uint) bool {
		c := &chunk{path: fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, is[0], sep, is[1])}
		assert.Equal(t, metaFilePath(c.path), c.metaFilePath(), "meta file path")
		return true
	})
}

func TestMetadata(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5})
	ends, err := readMetadata(metadata)
	assert.Nil(t, err, "failed to read metadata: %s", err)
	assert.Equal(t, []int32{0, 1, 2, 3, 4, 5}, ends, "ends")
}

func TestMetadataNonContiguousIndices(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 5, 2})
	ends, err := readMetadata(metadata)
	assert.NotNil(t, err, "expected to not parse that, got: %v", ends)
}

func TestMetadataNonIncreasingEnds(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 2, 0})
	ends, err := readMetadata(metadata)
	assert.NotNil(t, err, "expected to not parse that, got: %v", ends)
}

func TestMetadataRollback(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 0, 1})
	ends, err := readMetadata(metadata)
	assert.Nil(t, err, "failed to read metadata: %s", err)
	assert.Equal(t, []int32{1}, ends, "failed to apply rollback, got: %v", ends)
}

func TestMetadataIncomplete(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1})
	ends, err := readMetadata(metadata)
	assert.NotNil(t, err, "expected to not parse that, got: %v", ends)
}

func TestMetadataIncompleteRollback(t *testing.T) {
	metadata := makeMetadata(t, []int32{0, 0, 1, 1, 0})
	ends, err := readMetadata(metadata)
	assert.NotNil(t, err, "expected to not parse that, got: %v", ends)
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
