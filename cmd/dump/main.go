package main

import (
	"fmt"
	"github.com/barrucadu/logdb"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("usage: %v PATH\n", os.Args[0])
		os.Exit(1)
	}

	db, err := logdb.Open(os.Args[1], 0, false)
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
