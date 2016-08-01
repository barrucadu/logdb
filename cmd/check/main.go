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

	if _, err := logdb.Open(os.Args[1], 0, false); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Ok!")
}
