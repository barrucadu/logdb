package logdb

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var coderTypes = map[string]func() *CodingDB{
	"id":     func() *CodingDB { return IdentityCoder(&InMemDB{}) },
	"binary": func() *CodingDB { return BinaryCoder(&InMemDB{}, binary.LittleEndian) },
	"gob":    func() *CodingDB { return GobCoder(&InMemDB{}) },
}

func TestCoding_AppendValue(t *testing.T) {
	for coderName, coderFactory := range coderTypes {
		t.Logf("Coder: %s\n", coderName)
		coder := coderFactory()

		bss := make([][]byte, 255)
		for i := 0; i < len(bss); i++ {
			bss[i] = []byte(fmt.Sprintf("entry %v", i))
		}

		for i, bs := range bss {
			idx, err := coder.AppendValue(bs)
			assert.Nil(t, err, "expected no error in append")
			assert.Equal(t, uint64(i+1), idx, "expected equal ID")

			v := make([]byte, len(bs))
			// Gob is slightly special
			if coderName == "gob" {
				err = coder.GetValue(idx, &v)
			} else {
				err = coder.GetValue(idx, v)
			}
			assert.Nil(t, err, "expected no error in get")
			assert.Equal(t, bs, v, "expected equal '[]byte' values")
		}
	}
}

func TestCoding_AppendValues(t *testing.T) {
	for coderName, coderFactory := range coderTypes {
		t.Logf("Coder: %s\n", coderName)
		coder := coderFactory()

		bss := make([][]byte, 255)
		for i := 0; i < len(bss); i++ {
			bss[i] = []byte(fmt.Sprintf("entry %v", i))
		}

		idx, err := coder.AppendValues(bss)
		assert.Nil(t, err, "expected no error in append")
		assert.Equal(t, uint64(1), idx, "expected first ID")

		for i, bs := range bss {
			v := make([]byte, len(bs))
			// Gob is slightly special
			if coderName == "gob" {
				err = coder.GetValue(uint64(i+1), &v)
			} else {
				err = coder.GetValue(uint64(i+1), v)
			}
			assert.Nil(t, err, "expected no error in get")
			assert.Equal(t, bs, v, "expected equal '[]byte' values")
		}
	}
}
