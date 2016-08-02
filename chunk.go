package logdb

import (
	"bytes"
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

	newFrom  int
	rollback bool
	delete   bool
}

// Delete the files associated with a chunk.
func (c *chunk) closeAndRemove() error {
	if err := closeAndRemove(c.mmapf); err != nil {
		return err
	}
	return os.Remove(metaFilePath(c))
}

const (
	chunkPrefix      = "chunk"
	metaSuffix       = "meta"
	sep              = "_"
	initialChunkFile = chunkPrefix + sep + "0" + sep + "1"
)

// Get the meta file path associated with a chunk file path.
func metaFilePath(chunkFilePath interface{}) string {
	var path string
	switch cfg := chunkFilePath.(type) {
	case chunk:
		path = cfg.path
	case *chunk:
		path = cfg.path
	case string:
		path = cfg
	default:
		panic("internal error: bad type in metaFilePath")
	}
	return path + sep + metaSuffix
}

// Check if a file is a chunk data file.
//
// A valid chunk filename consists of the chunkPrefix followed by one
// or more digits, with no leading zeroes.
func isChunkDataFile(fi os.FileInfo) bool {
	bits := strings.Split(fi.Name(), chunkPrefix+sep)
	// In the form chunkPrefix[.+]
	if len(bits) != 2 || len(bits[0]) != 0 || len(bits[1]) == 0 {
		return false
	}

	bits = strings.Split(bits[1], sep)

	if len(bits) != 2 {
		return false
	}

	// Must be [.+]_[0-9]+
	if _, err := strconv.Atoi(bits[1]); err != nil {
		return false
	}

	// Special case: '0' is allowed, even though that has a leading zero.
	if bits[0] == "0" {
		return true
	}

	var nozero bool
	for _, r := range []rune(bits[1]) {
		// Must be a digit
		if !(r >= '0' && r <= '9') {
			return false
		}
		// No leading zeroes
		if r != '0' {
			nozero = true
		} else if !nozero {
			return false
		}
	}

	return true
}

// Given a chunk, get the filename of the next chunk.
//
// This function panics if the chunk path is invalid. This should
// never happen unless openChunkSliceDB or isChunkDataFile is broken.
func (c *chunk) nextDataFileName(oldest uint64) string {
	bits := strings.Split(c.path, "/"+chunkPrefix+sep)
	if len(bits) < 2 {
		panic("malformed chunk file name: " + c.path)
	}

	bits = strings.Split(bits[len(bits)-1], sep)
	if len(bits) != 2 {
		panic("malformed chunk file name: " + c.path)
	}

	num, err := strconv.Atoi(bits[0])
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
	nameBits := strings.Split(fi.Name(), sep)
	if len(nameBits) != 3 {
		return chunk, fmt.Errorf("invalid chunk data file name: %s", fi.Name())
	}
	oldnum, err := strconv.Atoi(nameBits[2])
	if err != nil {
		return chunk, fmt.Errorf("invalid chunk data file name: %s", fi.Name())
	}
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
	mfile, err := os.Open(metaFilePath(chunk))
	if err != nil {
		return chunk, &ReadError{err}
	}
	defer mfile.Close()
	priorEnd := int32(-1)
	for {
		var this int32
		if err := binary.Read(mfile, binary.LittleEndian, &this); err != nil {
			if err == io.EOF {
				break
			}
			return chunk, &FormatError{
				FilePath: metaFilePath(chunk),
				Err:      errwrap.Wrapf("unexpected error reading chunk metadata: {{err}}", err),
			}
		}
		if this < priorEnd {
			return chunk, &FormatError{
				FilePath: metaFilePath(chunk),
				Err:      fmt.Errorf("entry ending positions are not monotonically increasing (prior %v got %v)", priorEnd, this),
			}
		}
		chunk.ends = append(chunk.ends, this)
		priorEnd = this
	}

	// Chunk oldest/next IDs must match: there can be no gaps!
	if priorChunk != nil && chunk.oldest != priorChunk.next {
		return chunk, &FormatError{
			FilePath: metaFilePath(chunk),
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

	// If there was a rollback which affected this chunk, rewrite
	// all the metadata. Otherwise only write the new end points.
	metaBuf := new(bytes.Buffer)
	if c.rollback {
		for _, end := range c.ends {
			if err := binary.Write(metaBuf, binary.LittleEndian, end); err != nil {
				return err
			}
		}
		if err := writeFile(metaFilePath(c), metaBuf.Bytes()); err != nil {
			return err
		}
	} else {
		for i := c.newFrom; i < len(c.ends); i++ {
			if err := binary.Write(metaBuf, binary.LittleEndian, c.ends[i]); err != nil {
				return err
			}
		}
		if err := appendFile(metaFilePath(c), metaBuf.Bytes()); err != nil {
			return err
		}
	}

	c.rollback = false
	c.newFrom = len(c.ends)

	return nil
}
