package logdb

import (
	"os"
	"strings"
)

/// Slices with nice sorting properties.

// Lexicographic sorting by filename split into 'sep'-delimited segments.
type fileInfoSlice []os.FileInfo

func (fis fileInfoSlice) Len() int {
	return len(fis)
}

func (fis fileInfoSlice) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func (fis fileInfoSlice) Less(i, j int) bool {
	bitsI := strings.Split(fis[i].Name(), sep)
	bitsJ := strings.Split(fis[j].Name(), sep)

	max := len(bitsI)
	if len(bitsJ) > max {
		max = len(bitsJ)
	}

	for i := 0; i < max; i++ {
		if i >= len(bitsI) {
			break
		} else if i > len(bitsJ) {
			break
		} else if lessLexico(bitsI[i], bitsJ[i]) {
			return true
		}
	}
	return len(bitsI) < len(bitsJ)
}

// Lexicographic sorting by data file path.
type chunkSlice []*chunk

func (cs chunkSlice) Len() int {
	return len(cs)
}

func (cs chunkSlice) Swap(i, j int) {
	cs[i], cs[j] = cs[j], cs[i]
}

func (cs chunkSlice) Less(i, j int) bool {
	return lessLexico(cs[i].path, cs[j].path)
}

// Compare two strings lexicographically.
func lessLexico(a, b string) bool {
	if len(a) == len(b) {
		return a < b
	}
	return len(a) < len(b)
}
