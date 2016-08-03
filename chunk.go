package logdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/hashicorp/errwrap"
)

// A chunk is one memory-mapped file.
type chunk struct {
	path string

	bytes []byte
	mmapf *os.File

	// One past the ending addresses of entries in the 'bytes'
	// slice.
	//
	// This choice is because starting addresses can always be
	// calculated from ending addresses, as the first entry starts
	// at offset 0 (and there are no gaps). Ending addresses
	// cannot be calculated from starting addresses, unless the
	// ending address of the final entry is stored as well.
	ends []int32

	oldest uint64

	next uint64

	newFrom int
	delete  bool
}

// Delete the files associated with a chunk.
func (c *chunk) closeAndRemove() error {
	if err := closeAndRemove(c.mmapf); err != nil {
		return err
	}
	return os.Remove(c.metaFilePath())
}

const (
	chunkPrefix      = "chunk"
	metaSuffix       = "meta"
	sep              = "_"
	initialChunkFile = chunkPrefix + sep + "0" + sep + "1"
)

// Get the meta file path associated with a chunk data file path.
func metaFilePath(dataFilePath string) string {
	return dataFilePath + sep + metaSuffix
}

// Get the meta file path associated with a chunk.
func (c *chunk) metaFilePath() string {
	return metaFilePath(c.path)
}

// Check if a file basename is a chunk data file.
//
// A valid chunk filename consists of the chunkPrefix followed by one
// or more digits, with no leading zeroes.
func isBasenameChunkDataFile(basename string) bool {
	bits := strings.Split(basename, chunkPrefix+sep)
	// In the form chunkPrefix[.+]
	if len(bits) != 2 || len(bits[0]) != 0 || len(bits[1]) == 0 {
		return false
	}

	bits = strings.Split(bits[1], sep)

	if len(bits) != 2 {
		return false
	}

	// Must be [0-9]+_[0-9]+]
	if len(bits[0]) == 0 || len(bits[1]) == 0 {
		return false
	}
	first, err := strconv.ParseUint(bits[0], 10, 0)
	if err != nil {
		return false
	}
	if _, err := strconv.ParseUint(bits[1], 10, 0); err != nil {
		return false
	}

	// Leading zeroes are disallowed.
	if bits[0][0] == '0' {
		return first == 0
	}

	return true
}

// Given a chunk, get the filename of the next chunk.
//
// This function panics if the chunk path is invalid. This should
// never happen unless openChunkSliceDB or isChunkDataFile is broken.
func (c *chunk) nextDataFileName(oldest uint64) string {
	// If there are directories in the path, correctly identify the basename.
	firstSep := chunkPrefix + sep
	if strings.ContainsRune(c.path, '/') {
		firstSep = "/" + firstSep
	}

	bits := strings.Split(c.path, firstSep)
	if len(bits) < 2 {
		panic("malformed chunk file name: " + c.path)
	}

	bits = strings.Split(bits[len(bits)-1], sep)
	if len(bits) != 2 {
		panic("malformed chunk file name: " + c.path)
	}

	num, err := strconv.ParseUint(bits[0], 10, 0)
	if err != nil {
		panic("malformed chunk file name: " + c.path)
	}

	return fmt.Sprintf("%s%s%v%s%v", chunkPrefix, sep, num+1, sep, oldest)
}

// Create the files for a new chunk. As an empty chunk is not allowed,
// it is assumed that an entry will be immediately written.
func createChunkFiles(dataFilePath string, chunkSize uint32, oldest uint64) error {
	// Create the chunk files.
	if err := createFile(dataFilePath, chunkSize); err != nil {
		return err
	}
	file, err := os.OpenFile(metaFilePath(dataFilePath), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	file.Close()
	return err
}

// Open a chunk file
func openChunkFile(basedir string, fi os.FileInfo, priorChunk *chunk, chunkSize uint32) (chunk, error) {
	chunk := chunk{path: basedir + "/" + fi.Name()}
	// Get the oldest ID from the file name
	if !isBasenameChunkDataFile(fi.Name()) {
		return chunk, fmt.Errorf("invalid chunk data file name: %s", fi.Name())
	}
	// This does no validation because isBasenameChunkDataFile took care of that.
	nameBits := strings.Split(fi.Name(), sep)
	oldnum, _ := strconv.ParseUint(nameBits[2], 10, 0)
	chunk.oldest = uint64(oldnum)

	// mmap the data file
	mmapf, bytes, err := mmap(chunk.path)
	if err != nil {
		return chunk, &ReadError{err}
	}
	if uint32(len(bytes)) != chunkSize {
		return chunk, &FormatError{
			FilePath: chunk.path,
			Err:      fmt.Errorf("incorrect file size (expected %v got %v)", chunkSize, uint32(len(bytes))),
		}
	}
	chunk.bytes = bytes
	chunk.mmapf = mmapf

	// read the ending address metadata
	mfile, err := os.Open((&chunk).metaFilePath())
	if err != nil {
		return chunk, &ReadError{err}
	}
	defer mfile.Close()
	for {
		// metadata is in the format [index int32][end int32]
		// metadata ends at EOF
		// If the indices go backwards, that means entries have been rolled back
		var idx int32
		var this int32
		if err := binary.Read(mfile, binary.LittleEndian, &idx); err != nil {
			if err == io.EOF {
				break
			}
			return chunk, &FormatError{
				FilePath: (&chunk).metaFilePath(),
				Err:      errwrap.Wrapf("unexpected error reading chunk metadata: {{err}}", err),
			}
		}
		if idx > int32(len(chunk.ends)) {
			return chunk, &FormatError{
				FilePath: (&chunk).metaFilePath(),
				Err:      fmt.Errorf("entry index too large (expected <=%v, got %v)", len(chunk.ends), idx),
			}
		}
		if err := binary.Read(mfile, binary.LittleEndian, &this); err != nil {
			if err == io.EOF {
				// EOF here means that syncing failed, so just forget this last entry.
				break
			}
			return chunk, &FormatError{
				FilePath: (&chunk).metaFilePath(),
				Err:      err,
			}
		}
		if idx < int32(len(chunk.ends)) {
			if idx == 0 {
				chunk.ends = nil
			} else {
				chunk.ends = chunk.ends[0:idx]
			}
		}
		if idx > 0 && this < chunk.ends[idx-1] {
			return chunk, &FormatError{
				FilePath: (&chunk).metaFilePath(),
				Err:      fmt.Errorf("entry ending positions are not monotonically increasing (prior %v got %v)", chunk.ends[idx-1], this),
			}
		}
		chunk.ends = append(chunk.ends, this)
	}

	// Chunk oldest/next IDs must match: there can be no gaps!
	if priorChunk != nil && chunk.oldest != priorChunk.next {
		return chunk, &FormatError{
			FilePath: (&chunk).metaFilePath(),
			Err:      fmt.Errorf("discontinuity in entry IDs (expected %v got %v)", priorChunk.next, chunk.oldest),
		}
	}

	chunk.next = chunk.oldest + uint64(len(chunk.ends))
	return chunk, nil
}

// Write a chunk to disk.
func (c *chunk) sync() error {
	// To ensure ACID, sync the data first and only then the
	// metadata. This means that if there is a failure between the
	// two syncs, even if the newly-written data is corrupt, there
	// will be no metadata referring to it, and so it will be
	// invisible to the database when next opened.
	if err := fsync(c.mmapf); err != nil {
		return err
	}

	// Write the new end points.
	for i := c.newFrom; i < len(c.ends); i++ {
		if err := appendFile(c.metaFilePath(), int32(i)); err != nil {
			return err
		}
		if err := appendFile(c.metaFilePath(), c.ends[i]); err != nil {
			return err
		}
	}
	c.newFrom = len(c.ends)

	return nil
}
