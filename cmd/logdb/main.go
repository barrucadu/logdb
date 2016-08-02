package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/barrucadu/logdb"
)

const (
	numAppenders  = 5
	numTruncaters = 2
	numSyncers    = 1
)

func main() {
	if len(os.Args) < 3 || (os.Args[1] != "check" && os.Args[1] != "dump" && os.Args[1] != "fuzz") {
		fmt.Printf("usage: %v [check | dump | fuzz] <database-path>\n", os.Args[0])
		os.Exit(1)
	}

	switch os.Args[1] {
	case "check":
		check(os.Args[2])
	case "dump":
		dump(os.Args[2])
	case "fuzz":
		fuzz(os.Args[2])
	}
}

func check(path string) {
	if _, err := logdb.Open(path, 0, false); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Ok!")
}

func dump(path string) {
	db, err := logdb.Open(path, 0, false)
	if err != nil {
		fmt.Printf("could not open database in %s: %s\n", path, err)
		os.Exit(1)
	}

	fmt.Printf("oldest: %v\n", db.OldestID())
	fmt.Printf("newest: %v\n", db.NewestID())

	for i := db.OldestID(); i <= db.NewestID(); i++ {
		v, err := db.Get(i)
		if err != nil {
			fmt.Printf("could not read entry %v: %s\n", i, err)
			os.Exit(1)
		}
		fmt.Printf("%v: %v\n", i, v)
	}
}

func fuzz(path string) {
	db, err := logdb.Open(path, 1024*128, true)
	if err != nil {
		fmt.Printf("could not open database in %s: %s\n", path, err)
		os.Exit(1)
	}

	if err := db.SetSync(-1); err != nil {
		fmt.Printf("could not disable periodic syncing: %s\n", err)
		os.Exit(1)
	}

	appender := func(id int, db *logdb.LogDB) {
		for {
			bs := make([]byte, rand.Intn(255))
			for i := range bs {
				bs[i] = uint8(rand.Uint32())
			}
			if err := db.Append(bs); err != nil {
				fmt.Printf("[A%v] could not append entry: %s\n", id, err)
			}
		}
	}

	truncater := func(id int, db *logdb.LogDB) {
		for {
			oldest := db.OldestID()
			newest := db.NewestID()
			idrange := int64(newest - oldest)
			newOldest := oldest
			newNewest := newest
			if idrange > 0 {
				newOldest += uint64(rand.Int63n(idrange))
				newNewest -= uint64(rand.Int63n(idrange))
			}
			if err := db.Truncate(newOldest, newNewest); err != nil {
				fmt.Printf("[T%v] could not truncate log from [%v:%v] to [%v:%v]: %s\n", id, oldest, newest, newOldest, newNewest, err)
			}
		}
	}

	syncer := func(id int, db *logdb.LogDB) {
		for {
			if err := db.Sync(); err != nil {
				fmt.Printf("[S%v] could not sync: %s\n", id, err)
			}
		}
	}

	fmt.Printf("performing infinite sequence of concurrent appends and truncates...\n")

	for i := 0; i < numAppenders; i++ {
		go appender(i, db)
	}
	for i := 0; i < numTruncaters; i++ {
		go truncater(i, db)
	}
	for i := 0; i < numSyncers; i++ {
		go syncer(i, db)
	}

	for {
	}
}
