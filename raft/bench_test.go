package raft

import (
	"os"
	"testing"

	"github.com/barrucadu/logdb"

	"github.com/hashicorp/raft/bench"
)

func BenchmarkFirstIndex(b *testing.B) {
	db := assertCreateForBench(b, "first_index")
	defer assertClose(b, db)

	raftbench.FirstIndex(b, db)
}

func BenchmarkLastIndex(b *testing.B) {
	db := assertCreateForBench(b, "last_index")
	defer assertClose(b, db)

	raftbench.LastIndex(b, db)
}

func BenchmarkGetLog(b *testing.B) {
	db := assertCreateForBench(b, "get_log")
	defer assertClose(b, db)

	raftbench.GetLog(b, db)
}

func BenchmarkStoreLog(b *testing.B) {
	db := assertCreateForBench(b, "store_log")
	defer assertClose(b, db)

	raftbench.StoreLog(b, db)
}

func BenchmarkStoreLogs(b *testing.B) {
	db := assertCreateForBench(b, "store_logs")
	defer assertClose(b, db)

	raftbench.StoreLogs(b, db)
}

func BenchmarkDeleteRange(b *testing.B) {
	db := assertCreateForBench(b, "delete_range")
	defer assertClose(b, db)

	raftbench.DeleteRange(b, db)
}

/// HELPERS

func assertCreateForBench(b *testing.B, benchName string) *LogStore {
	_ = os.RemoveAll("../test_db/raft-bench/" + benchName)
	db, err := logdb.Open("../test_db/raft-bench/"+benchName, 1024*1024, true)
	if err != nil {
		b.Fatal(err)
	}
	return New(db)
}
