package logdb

import (
	"fmt"
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

func quickcheck(t *testing.T, f interface{}) {
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}
