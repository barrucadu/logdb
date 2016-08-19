package logdb

import (
	"errors"
	"fmt"
	"reflect"
)

// ErrNotValueSlice means that AppendValues was called with a non-slice argument.
var ErrNotValueSlice = errors.New("AppendValues must be called with a slice argument")

// A CodingDB wraps a 'LogDB' with functions to encode and decode values of some sort, giving a higher-level
// interface than raw byte slices.
type CodingDB struct {
	LogDB

	Encode func(interface{}) ([]byte, error)
	Decode func([]byte) (interface{}, error)
}

// NewIdentityCoder creates a 'CodingDB' with the identity encoder/decoder. It is an error to append a value
// which is not a '[]byte'.
func NewIdentityCoder(logdb LogDB) *CodingDB {
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
		Decode: func(bs []byte) (interface{}, error) {
			return interface{}(bs), nil
		},
	}
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

	db.Decode = func(bs []byte) (interface{}, error) {
		bs2, err := preDecode(bs)
		if err != nil {
			return nil, err
		}
		return oldDecode(bs2)
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
func (db *CodingDB) GetValue(id uint64) (interface{}, error) {
	bs, err := db.Get(id)
	if err != nil {
		return nil, err
	}
	return db.Decode(bs)
}
