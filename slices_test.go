package logdb

import (
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLessFileName(t *testing.T) {
	cmps := [][]string{
		{"chunk_0_1", "chunk_1_5"},
		{"chunk_1_5", "chunk_2_8"},
		{"chunk_2_8", "chunk_3_0"},
		{"chunk_3_0", "chunk_4_9"},
		{"chunk_4_9", "chunk_10_5"},
		{"chunk_10_5", "chunk_10_10"},
		{"chunk_10", "chunk_10_10"},
	}

	for _, cmp := range cmps {
		assert.True(t, lessFileName(cmp[0], cmp[1]), "expected %s < %s", cmp[0], cmp[1])
		assert.False(t, lessFileName(cmp[1], cmp[0]), "expected ! (%s < %s)", cmp[1], cmp[0])
	}
}

func TestSortFileInfoSlice(t *testing.T) {
	sorted := []os.FileInfo{
		&dummyFileInfo{"chunk_0_1"},
		&dummyFileInfo{"chunk_1_5"},
		&dummyFileInfo{"chunk_2_8"},
		&dummyFileInfo{"chunk_3_0"},
		&dummyFileInfo{"chunk_4_9"},
		&dummyFileInfo{"chunk_10_5"},
		&dummyFileInfo{"chunk_10_10"},
	}

	// Copy and shuffle
	shuffled := make([]os.FileInfo, len(sorted))
	for i, fi := range sorted {
		shuffled[i] = fi
	}
	for i := range shuffled {
		j := rand.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	// Sort
	sort.Sort(fileInfoSlice(shuffled))
	assert.Equal(t, sorted, shuffled)
}

func TestSortChunkSlice(t *testing.T) {
	sorted := []*chunk{
		{path: "chunk_0_1"},
		{path: "chunk_1_5"},
		{path: "chunk_2_8"},
		{path: "chunk_3_0"},
		{path: "chunk_4_9"},
		{path: "chunk_10_5"},
		{path: "chunk_10_10"},
	}

	// Copy and shuffle
	shuffled := make([]*chunk, len(sorted))
	for i, fi := range sorted {
		shuffled[i] = fi
	}
	for i := range shuffled {
		j := rand.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	// Sort
	sort.Sort(chunkSlice(shuffled))
	assert.Equal(t, sorted, shuffled)
}

/// HELPERS

// A dummy FileInfo to test the sorting
type dummyFileInfo struct{ name string }

func (fi *dummyFileInfo) Name() string       { return fi.name }
func (fi *dummyFileInfo) Size() int64        { return 0 }
func (fi *dummyFileInfo) Mode() os.FileMode  { return os.FileMode(0) }
func (fi *dummyFileInfo) ModTime() time.Time { return time.Now() }
func (fi *dummyFileInfo) IsDir() bool        { return false }
func (fi *dummyFileInfo) Sys() interface{}   { return nil }
