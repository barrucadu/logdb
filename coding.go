package logdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
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

// BinaryCoder creates a 'CodingDB' with the binary encoder/decoder. Values must be valid input for the
// 'binary.Write' function.
func BinaryCoder(logdb LogDB, byteOrder binary.ByteOrder) *CodingDB {
	return &CodingDB{
		LogDB: logdb,
		Encode: func(val interface{}) ([]byte, error) {
			buf := new(bytes.Buffer)
			err := binary.Write(buf, byteOrder, val)
			return buf.Bytes(), err
		},
		Decode: func(bs []byte, data interface{}) error {
			return binary.Read(bytes.NewReader(bs), byteOrder, data)
		},
	}
}

// GobCoder creates a 'CodingDB' with the gob encoder/decoder. Values must be valid input for the 'god.Encode'
// function.
func GobCoder(logdb LogDB) *CodingDB {
	return &CodingDB{
		LogDB: logdb,
		Encode: func(val interface{}) ([]byte, error) {
			buf := new(bytes.Buffer)
			enc := gob.NewEncoder(buf)
			err := enc.Encode(val)
			return buf.Bytes(), err
		},
		Decode: func(bs []byte, data interface{}) error {
			dec := gob.NewDecoder(bytes.NewReader(bs))
			return dec.Decode(data)
		},
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
