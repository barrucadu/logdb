package logdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var coderTypes = map[string]func() *CodingDB{
	"id": func() *CodingDB { return NewIdentityCoder(&InMemDB{}) },
}

func TestAppendValue(t *testing.T) {
	for coderName, coderFactory := range coderTypes {
		t.Logf("Database: %s\n", coderName)
		coder := coderFactory()

		bss := make([][]byte, 255)
		for i := 0; i < len(bss); i++ {
			bss[i] = []byte(fmt.Sprintf("entry %v", i))
		}

		for i, bs := range bss {
			idx, err := coder.AppendValue(bs)
			assert.Nil(t, err, "expected no error in append")
			assert.Equal(t, uint64(i+1), idx, "expected equal ID")

			v, err := coder.GetValue(idx)
			assert.Nil(t, err, "expected no error in get")
			vbs, ok := v.([]byte)
			assert.True(t, ok, "exected '[]byte' result")
			assert.Equal(t, bs, vbs, "expected equal '[]byte' values")
		}
	}
}

func TestAppendValues(t *testing.T) {
	for coderName, coderFactory := range coderTypes {
		t.Logf("Database: %s\n", coderName)
		coder := coderFactory()

		bss := make([][]byte, 255)
		for i := 0; i < len(bss); i++ {
			bss[i] = []byte(fmt.Sprintf("entry %v", i))
		}

		idx, err := coder.AppendValues(bss)
		assert.Nil(t, err, "expected no error in append")
		assert.Equal(t, uint64(1), idx, "expected first ID")

		for i, bs := range bss {
			v, err := coder.GetValue(uint64(i + 1))
			assert.Nil(t, err, "expected no error in get")
			vbs, ok := v.([]byte)
			assert.True(t, ok, "exected '[]byte' result")
			assert.Equal(t, bs, vbs, "expected equal '[]byte' values")
		}
	}
}
