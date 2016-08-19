package logdb

import (
	"bytes"
	"compress/flate"
	"compress/lzw"
	"errors"
	"io/ioutil"
)

// A CompressingDB wraps a 'LogDB' with functions to compress and decompress entries, applied transparently
// during 'Append', 'AppendEntries', and 'Get'.
type CompressingDB struct {
	LogDB

	Compress   func([]byte) ([]byte, error)
	Decompress func([]byte) ([]byte, error)
}

// Append implements the 'LogDB' interface. If the underlying 'LogDB' is also a 'PersistDB', 'BoundedDB', or
// 'CloseDB' then those interfaces are also implemented.
//
// With 'BoundedDB' the bounded entry size is that of the compressed byte array; so it may be possible to
// insert entries which are larger than the bound.
func (db *CompressingDB) Append(entry []byte) (uint64, error) {
	return db.AppendEntries([][]byte{entry})
}

// AppendEntries implements the 'LogDB' interface. If the underlying 'LogDB' is also a 'PersistDB',
// 'BoundedDB', or 'CloseDB' then those interfaces are also implemented.
//
// With 'BoundedDB' the bounded entry size is that of the compressed byte array; so it may be possible to
// insert entries which are larger than the bound.
func (db *CompressingDB) AppendEntries(entries [][]byte) (uint64, error) {
	compressedEntries := make([][]byte, len(entries))

	for i, entry := range entries {
		compressed, err := db.Compress(entry)
		if err != nil {
			return 0, err
		}
		compressedEntries[i] = compressed
	}

	return db.LogDB.AppendEntries(compressedEntries)
}

// Get implements the 'LogDB' interface. If the underlying 'LogDB; is also a 'CloseDB' then that interface is
// also implemented.
//
// With 'BoundedDB' the bounded entry size is that of the compressed byte array; so it may be possible to
// retrieve entries which are larger than the bound.
func (db *CompressingDB) Get(id uint64) ([]byte, error) {
	bs, err := db.LogDB.Get(id)
	if err != nil {
		return nil, err
	}
	return db.Decompress(bs)
}

// CompressIdentity create a 'CompressingDB' with the identity compressor/decompressor.
func CompressIdentity(logdb LogDB) *CompressingDB {
	return &CompressingDB{
		LogDB:      logdb,
		Compress:   func(bs []byte) ([]byte, error) { return bs, nil },
		Decompress: func(bs []byte) ([]byte, error) { return bs, nil },
	}
}

// CompressDEFLATE creates a 'CompressingDB' with DEFLATE compression at the given level.
//
// Returns an error if the level is < -2 or > 9.
func CompressDEFLATE(logdb LogDB, level int) (*CompressingDB, error) {
	if level < -2 || level > 9 {
		return nil, errors.New("flate compression level must be in the range [-2,9]")
	}
	return &CompressingDB{
		LogDB: logdb,
		Compress: func(bs []byte) ([]byte, error) {
			buf := new(bytes.Buffer)
			w, _ := flate.NewWriter(buf, level)
			n, err := w.Write(bs)
			if err != nil {
				return nil, err
			}
			if n < len(bs) {
				return nil, errors.New("could not compress all bytes")
			}
			w.Close()
			return buf.Bytes(), nil
		},
		Decompress: func(bs []byte) ([]byte, error) {
			r := flate.NewReader(bytes.NewReader(bs))
			out, err := ioutil.ReadAll(r)
			r.Close()
			return out, err
		},
	}, nil
}

// CompressLZW creates a 'CompressingDB' with LZW compression with the given order and literal width.
//
// Returns an error if the lit width is < 2 or > 8.
func CompressLZW(logdb LogDB, order lzw.Order, litWidth int) (*CompressingDB, error) {
	if litWidth < 2 || litWidth > 8 {
		return nil, errors.New("LZW literal width must be in the range [2,8]")
	}

	return &CompressingDB{
		LogDB: logdb,
		Compress: func(bs []byte) ([]byte, error) {
			buf := new(bytes.Buffer)
			w := lzw.NewWriter(buf, order, litWidth)
			n, err := w.Write(bs)
			if err != nil {
				return nil, err
			}
			if n < len(bs) {
				return nil, errors.New("could not compress all bytes")
			}
			w.Close()
			return buf.Bytes(), nil
		},
		Decompress: func(bs []byte) ([]byte, error) {
			r := lzw.NewReader(bytes.NewReader(bs), order, litWidth)
			out, err := ioutil.ReadAll(r)
			r.Close()
			return out, err
		},
	}, nil
}
