package raft

import (
	"testing"

	"github.com/barrucadu/logdb"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft/bench"
)

// FirstIndex

func BenchmarkLogDB_ChunkDB_FirstIndex(b *testing.B) {
	db := assertOpen(b, dbTypes["chunkdb"], true, true, "first_index")
	defer assertClose(b, db)

	raftbench.FirstIndex(b, db)
}

func BenchmarkLogDB_LockFreeChunkDB_FirstIndex(b *testing.B) {
	db := assertOpen(b, dbTypes["lock free chunkdb"], true, true, "first_index")
	defer assertClose(b, db)

	raftbench.FirstIndex(b, db)
}

// LastIndex

func BenchmarkLogDB_ChunkDB_LastIndex(b *testing.B) {
	db := assertOpen(b, dbTypes["chunkdb"], true, true, "last_index")
	defer assertClose(b, db)

	raftbench.LastIndex(b, db)
}

func BenchmarkLogDB_LockFreeChunkDB_LastIndex(b *testing.B) {
	db := assertOpen(b, dbTypes["lock free chunkdb"], true, true, "last_index")
	defer assertClose(b, db)

	raftbench.LastIndex(b, db)
}

// GetLog

func BenchmarkLogDB_ChunkDB_GetLog(b *testing.B) {
	db := assertOpen(b, dbTypes["chunkdb"], true, true, "get_log")
	defer assertClose(b, db)

	raftbench.GetLog(b, db)
}

func BenchmarkLogDB_LockFreeChunkDB_GetLog(b *testing.B) {
	db := assertOpen(b, dbTypes["lock free chunkdb"], true, true, "get_log")
	defer assertClose(b, db)

	raftbench.GetLog(b, db)
}

// StoreLog

func benchStoreLog(b *testing.B, dbType logdb.LogDB) {
	db := assertOpen(b, dbType, true, true, "store_log")
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

func BenchmarkLogDB_ChunkDB_StoreLog(b *testing.B) {
	benchStoreLog(b, dbTypes["chunkdb"])
}

func BenchmarkLogDB_LockFreeChunkDB_StoreLog(b *testing.B) {
	benchStoreLog(b, dbTypes["chunkdb"])
}

// StoreLogs

func BenchmarkLogDB_ChunkDB_StoreLogs(b *testing.B) {
	db := assertOpen(b, dbTypes["chunkdb"], true, true, "store_logs")
	defer assertClose(b, db)

	raftbench.StoreLogs(b, db)
}

func BenchmarkLogDB_LockFreeChunkDB_StoreLogs(b *testing.B) {
	db := assertOpen(b, dbTypes["lock free chunkdb"], true, true, "store_logs")
	defer assertClose(b, db)

	raftbench.StoreLogs(b, db)
}

// DeleteRange

func benchDeleteRange(b *testing.B, dbType logdb.LogDB) {
	db := assertOpen(b, dbType, true, true, "delete_range")
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

func BenchmarkLogDB_ChunkDB_DeleteRange(b *testing.B) {
	benchDeleteRange(b, dbTypes["chunkdb"])
}

func BenchmarkLogDB_LockFreeChunkDB_DeleteRange(b *testing.B) {
	benchDeleteRange(b, dbTypes["lock free chunkdb"])
}
