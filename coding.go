package logdb

import (
	"bytes"
	"compress/flate"
	"compress/lzw"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
)

// ErrNotValueSlice means that AppendValues was called with a non-slice argument.
var ErrNotValueSlice = errors.New("AppendValues must be called with a slice argument")

// A CodingDB wraps a 'LogDB' with functions to encode and decode values of some sort, giving a higher-level
// interface than raw byte slices.
type CodingDB struct {
	LogDB

	Encode func(interface{}) ([]byte, error)
	Decode func([]byte, interface{}) error
}

// IdentityCoder creates a 'CodingDB' with the identity encoder/decoder. It is an error to append a value
// which is not a '[]byte'.
func IdentityCoder(logdb LogDB) *CodingDB {
	return &CodingDB{
		LogDB: logdb,
		Encode: func(val interface{}) ([]byte, error) {
			bs, ok := val.([]byte)
			if !ok {
				tystr := reflect.TypeOf(val).String()
				return nil, fmt.Errorf("identity coder can only encode a '[]byte', got %s", tystr)
			}
			return bs, nil
		},
		Decode: func(bs []byte, data interface{}) error {
			outSlice, ok := data.([]byte)
			if !ok {
				tystr := reflect.TypeOf(data).String()
				return fmt.Errorf("identity coder can only decode a '[]byte', got %s", tystr)
			}
			copy(outSlice, bs)
			return nil
		},
	}
}

// CompressDEFLATE creates a 'CodingDB' with DEFLATE compression at the given level.
//
// Returns an error if the level is < -2 or > 9.
func CompressDEFLATE(logdb LogDB, level int) (*CodingDB, error) {
	if level < -2 || level > 9 {
		return nil, errors.New("flate compression level must be in the range [-2,9]")
	}
	db := IdentityCoder(logdb)
	db.Augment(
		func(bs []byte) ([]byte, error) {
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
		func(bs []byte) ([]byte, error) {
			r := flate.NewReader(bytes.NewReader(bs))
			bs, err := ioutil.ReadAll(r)
			r.Close()
			return bs, err
		},
	)
	return db, nil
}

// CompressLZW creates a 'CodingDB' with LZW compression at the given level.
func CompressLZW(logdb LogDB, order lzw.Order, litWidth int) *CodingDB {
	db := IdentityCoder(logdb)
	db.Augment(
		func(bs []byte) ([]byte, error) {
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
		func(bs []byte) ([]byte, error) {
			r := lzw.NewReader(bytes.NewReader(bs), order, litWidth)
			bs, err := ioutil.ReadAll(r)
			r.Close()
			return bs, err
		},
	)
	return db
}

// Augment modifies the 'Encode' and 'Decode' functions of a 'CodingDB' by post- and pre-composing with the
// given functions.
//
// This is useful for, eg, adding compression to an existing 'CodingDB'.
func (db *CodingDB) Augment(
	postEncode func([]byte) ([]byte, error),
	preDecode func([]byte) ([]byte, error),
) {
	oldEncode := db.Encode
	oldDecode := db.Decode

	db.Encode = func(value interface{}) ([]byte, error) {
		bs, err := oldEncode(value)
		if err != nil {
			return nil, err
		}
		return postEncode(bs)
	}

	db.Decode = func(bs []byte, data interface{}) error {
		bs2, err := preDecode(bs)
		if err != nil {
			return err
		}
		return oldDecode(bs2, data)
	}
}

// AppendValue encodes a value using the encoder, and stores it in the underlying 'LogDB' is there is no
// error.
func (db *CodingDB) AppendValue(value interface{}) (uint64, error) {
	bs, err := db.Encode(value)
	if err != nil {
		return 0, err
	}
	return db.Append(bs)
}

// AppendValues encodes a slice of values (represented as an 'interface{}', to make the casting simpler), and
// stores them in the underlying 'LogDB' if there is no error.
//
// Returns 'ErrNotValueSlice' if called with a non-slice argument.
func (db *CodingDB) AppendValues(values interface{}) (uint64, error) {
	v := reflect.ValueOf(values)
	if v.IsNil() {
		return 0, nil
	}
	if v.Kind() != reflect.Slice {
		return 0, ErrNotValueSlice
	}

	bss := make([][]byte, v.Len())
	for i := 0; i < v.Len(); i++ {
		bs, err := db.Encode(v.Index(i).Interface())
		if err != nil {
			return 0, err
		}
		bss[i] = bs
	}
	return db.AppendEntries(bss)
}

// GetValue retrieves a value from the underlying 'LogDB' and decodes it.
func (db *CodingDB) GetValue(id uint64, data interface{}) error {
	bs, err := db.Get(id)
	if err != nil {
		return err
	}
	return db.Decode(bs, data)
}
