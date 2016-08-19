package logdb

import (
	"compress/flate"
	"compress/lzw"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var compressTypes = map[string]func() *CompressingDB{
	"id":      func() *CompressingDB { return CompressIdentity(&InMemDB{}) },
	"deflate": func() *CompressingDB { db, _ := CompressDEFLATE(&InMemDB{}, flate.BestCompression); return db },
	"lzw":     func() *CompressingDB { db, _ := CompressLZW(&InMemDB{}, lzw.LSB, 8); return db },
}

func TestCompress_Append(t *testing.T) {
	for compressName, compressFactory := range compressTypes {
		t.Logf("Compress: %s\n", compressName)
		compress := compressFactory()

		bss := make([][]byte, 255)
		for i := 0; i < len(bss); i++ {
			bss[i] = []byte(fmt.Sprintf("entry %v", i))
		}

		for i, bs := range bss {
			idx, err := compress.Append(bs)
			assert.Nil(t, err, "expected no error in append")
			assert.Equal(t, uint64(i+1), idx, "expected equal ID")

			v, err := compress.Get(idx)
			assert.Nil(t, err, "expected no error in get")
			assert.Equal(t, bs, v, "expected equal '[]byte' values")
		}
	}
}

func TestCompress_AppendEntries(t *testing.T) {
	for compressName, compressFactory := range compressTypes {
		t.Logf("Compress: %s\n", compressName)
		compress := compressFactory()

		bss := make([][]byte, 255)
		for i := 0; i < len(bss); i++ {
			bss[i] = []byte(fmt.Sprintf("entry %v", i))
		}

		idx, err := compress.AppendEntries(bss)
		assert.Nil(t, err, "expected no error in append")
		assert.Equal(t, uint64(1), idx, "expected first ID")

		for i, bs := range bss {
			v, err := compress.Get(uint64(i + 1))
			assert.Nil(t, err, "expected no error in get")
			assert.Equal(t, bs, v, "expected equal '[]byte' values")
		}
	}
}
