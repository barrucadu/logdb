package raft

import (
	"os"
	"testing"

	"github.com/barrucadu/logdb"

	"github.com/hashicorp/raft"
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

	// raftbench.StoreLog(b, db)

	// This is the hashicorp benchmark, modified to use 1-based indecing for log entries.
	for n := 0; n < b.N; n++ {
		log := &raft.Log{Index: uint64(n) + 1, Data: []byte("data")}
		if err := db.StoreLog(log); err != nil {
			b.Fatalf("err: %s", err)
		}
	}
}

func BenchmarkStoreLogs(b *testing.B) {
	db := assertCreateForBench(b, "store_logs")
	defer assertClose(b, db)

	raftbench.StoreLogs(b, db)
}

func BenchmarkDeleteRange(b *testing.B) {
	db := assertCreateForBench(b, "delete_range")
	defer assertClose(b, db)

	// raftbench.DeleteRange(b, db)

	// This is the hashicorp benchmark, modified to not have gaps.
	var logs []*raft.Log
	for n := 0; n < b.N; n++ {
		for i := 0; i < 10; i++ {
			logs = append(logs, &raft.Log{Index: uint64(n*10 + i + 1), Data: []byte("data")})
		}
	}
	if err := db.StoreLogs(logs); err != nil {
		b.Fatalf("err: %s", err)
	}
	b.ResetTimer()

	// Delete a range of the data
	for n := 0; n < b.N; n++ {
		offset := 10 * n
		if err := db.DeleteRange(uint64(offset), uint64(offset+9)); err != nil {
			b.Fatalf("err: %s", err)
		}
	}
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
