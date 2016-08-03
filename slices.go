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
	return lessFileName(fis[i].Name(), fis[j].Name())
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
	return lessFileName(cs[i].path, cs[j].path)
}

// Compare two filenames with splitting.
func lessFileName(a, b string) bool {
	as := strings.Split(a, sep)
	bs := strings.Split(b, sep)

	min := len(as)
	if len(bs) < min {
		min = len(bs)
	}

	for i := 0; i < min; i++ {
		if len(as[i]) < len(bs[i]) {
			return true
		} else if len(as[i]) > len(bs[i]) {
			return false
		}

		switch strings.Compare(as[i], bs[i]) {
		case -1:
			return true
		case 1:
			return false
		}
	}

	return len(as) < len(bs)
}
