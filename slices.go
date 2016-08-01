package logdb

import "os"

/// Slices with nice sorting properties.

// Lexicographic sorting by filename.
type fileInfoSlice []os.FileInfo

func (fis fileInfoSlice) Len() int {
	return len(fis)
}

func (fis fileInfoSlice) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func (fis fileInfoSlice) Less(i, j int) bool {
	ni := fis[i].Name()
	nj := fis[j].Name()

	if len(ni) == len(nj) {
		return ni < nj
	}
	return len(ni) < len(nj)
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
	ni := cs[i].path
	nj := cs[j].path

	if len(ni) == len(nj) {
		return ni < nj
	}
	return len(ni) < len(nj)
}
