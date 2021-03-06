package logdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Filename-related constants.
const (
	chunkPrefix      = "chunk"
	metaSuffix       = "meta"
	sep              = "_"
	initialChunkFile = chunkPrefix + sep + "0" + sep + "1"
	initialMetaFile  = initialChunkFile + sep + metaSuffix
)

// A chunk is one memory-mapped file.
type chunk struct {
	// Path to the data file. The metadata file name and oldest entry ID are derived from this.
	path string

	// The memory-mapped data file. The 'bytes' slice is produced by mmaping the fd in the 'mmapf' file,
	// meaning that it can be fsynced easily.
	bytes []byte
	mmapf *os.File

	// One past the ending addresses of entries in the 'bytes' slice. This means that entries are contained
	// in the segment 'bytes[prior end:end]', with the 'prior end' for the first entry being 0.
	ends []int32

	// ID of the oldest entry in the chunk. This can be determined from the filename, but it's cheaper to
	// store it here.
	oldest uint64

	// For metadata syncing: 'newFrom' is the index of the first end that needs to be synced, and 'delete'
	// indicates that the chunk needs to be deleted at the next sync.
	newFrom int
	delete  bool
}

// Get the next entry ID in a chunk.
func (c *chunk) next() uint64 {
	return c.oldest + uint64(len(c.ends))
}

// Delete the files associated with a chunk.
func (c *chunk) closeAndRemove() error {
	if err := closeAndRemove(c.mmapf); err != nil {
		return err
	}
	return os.Remove(c.metaFilePath())
}

// Get the data file path associated with a chunk meta file path.
func dataFilePath(metaFilePath string) string {
	return strings.TrimSuffix(metaFilePath, sep+metaSuffix)
}

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
// A valid chunk filename consists of the chunkPrefix followed by one or more digits, with no leading zeroes.
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

// Check if a file basename is a chunk meta file.
//
// A valid chunk meta filename consists of a valid chink data filename followed by the meta suffix.
func isBasenameChunkMetaFile(basename string) bool {
	suff := sep + metaSuffix
	return strings.HasSuffix(basename, suff) && isBasenameChunkDataFile(strings.TrimSuffix(basename, suff))
}

// Given a chunk, get the filename of the next chunk.
//
// This function panics if the chunk path is invalid. This should never happen unless openChunkSliceDB or
// isChunkDataFile is broken.
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

// Create the files for a new chunk. As an empty chunk is not allowed, it is assumed that an entry will be
// immediately written.
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
		return chunk, &ChunkFileNameError{fi.Name()}
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
			Err: &ChunkSizeError{
				ChunkFilePath: chunk.path,
				Expected:      chunkSize,
				Actual:        uint32(len(bytes)),
			},
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
	ends, err := readMetadata(mfile)
	if err != nil {
		return chunk, &FormatError{
			FilePath: (&chunk).metaFilePath(),
			Err: &ChunkMetaError{
				ChunkFilePath: chunk.path,
				Err:           err,
			},
		}
	}
	chunk.ends = ends

	// Chunk oldest/next IDs must match: there can be no gaps!
	if priorChunk != nil && chunk.oldest != priorChunk.next() {
		return chunk, &FormatError{
			FilePath: (&chunk).metaFilePath(),
			Err: &ChunkContinuityError{
				ChunkFilePath: chunk.path,
				Expected:      priorChunk.next(),
				Actual:        chunk.oldest,
			},
		}
	}

	return chunk, nil
}

// Write a chunk to disk.
func (c *chunk) sync() error {
	// To ensure ACID, sync the data first and only then the metadata. This means that if there is a failure
	// between the two syncs, even if the newly-written data is corrupt, there will be no metadata referring
	// to it, and so it will be invisible to the database when next opened.
	if err := fsync(c.mmapf); err != nil {
		return err
	}

	// Construct the metadata as a buffer. This is done rather than appending to the output file directly
	// because individual "write" syscalls with a small enough buffer (which this will be for any reasonable
	// syncing period) are atomic. Multiple appends would have the possibility of failure in the middle.
	buf := new(bytes.Buffer)
	for i := c.newFrom; i < len(c.ends); i++ {
		if err := binary.Write(buf, binary.LittleEndian, int32(i)); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, c.ends[i]); err != nil {
			return err
		}
	}

	// Write the new end points.
	if err := appendFile(c.metaFilePath(), buf.Bytes()); err != nil {
		return err
	}
	c.newFrom = len(c.ends)

	return nil
}

// Read a chunk metadata file.
//
// Metadata is in the format [index int32][end int32], it ends at EOF. If the indices go backwards, that means
// entries have been rolled back
func readMetadata(r io.Reader) ([]int32, error) {
	var ends []int32
	var idx, this int32

	for {
		// Read the index into the ends slice.
		if err := binary.Read(r, binary.LittleEndian, &idx); err != nil {
			if err == io.EOF {
				break
			}
			return ends, err
		}
		if idx > int32(len(ends)) {
			return ends, &MetaContinuityError{
				Expected: int32(len(ends)),
				Actual:   idx,
			}
		}

		// Read the offset. If this fails, it means that syncing failed between the two writes.
		if err := binary.Read(r, binary.LittleEndian, &this); err != nil {
			return ends, err
		}

		// Check the offset is geq the prior offset.
		if idx > 0 && this < ends[idx-1] {
			return ends, &MetaOffsetError{
				Expected: int32(ends[idx-1]),
				Actual:   this,
			}
		}

		// Pop entries from the "ends" slice so that the current index is one past the end, and append it.
		ends = append(ends[0:idx], this)
	}

	return ends, nil
}
