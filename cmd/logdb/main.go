package main

import (
	"fmt"
	"os"

	"github.com/barrucadu/logdb"
)

func main() {
	if len(os.Args) < 3 || (os.Args[1] != "check" && os.Args[1] != "dump") {
		fmt.Printf("usage: %v [check | dump] <database-path>\n", os.Args[0])
		os.Exit(1)
	}

	switch os.Args[1] {
	case "check":
		check(os.Args[2])
	case "dump":
		dump(os.Args[2])
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
		fmt.Printf("could not open database in .: %s\n", err)
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
