package raft

import (
	"testing"

	"github.com/barrucadu/logdb"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft/bench"
)

// FirstIndex

func benchFirstIndex(b *testing.B, dbType logdb.LogDB) {
	db := assertOpen(b, dbType, true, true, "first_index")
	defer assertClose(b, db)

	raftbench.FirstIndex(b, db)
}

func BenchmarkLogDB_ChunkDB_FirstIndex(b *testing.B) {
	benchFirstIndex(b, dbTypes["chunkdb"])
}

func BenchmarkLogDB_LockFreeChunkDB_FirstIndex(b *testing.B) {
	benchFirstIndex(b, dbTypes["lock free chunkdb"])
}

func BenchmarkLogDB_InMemDB_FirstIndex(b *testing.B) {
	benchFirstIndex(b, dbTypes["inmem"])
}

// LastIndex

func benchLastIndex(b *testing.B, dbType logdb.LogDB) {
	db := assertOpen(b, dbType, true, true, "last_index")
	defer assertClose(b, db)

	raftbench.LastIndex(b, db)
}

func BenchmarkLogDB_ChunkDB_LastIndex(b *testing.B) {
	benchLastIndex(b, dbTypes["chunkdb"])
}

func BenchmarkLogDB_LockFreeChunkDB_LastIndex(b *testing.B) {
	benchLastIndex(b, dbTypes["lock free chunkdb"])
}

func BenchmarkLogDB_InMemDB_LastIndex(b *testing.B) {
	benchLastIndex(b, dbTypes["inmem"])
}

// GetLog

func benchGetLog(b *testing.B, dbType logdb.LogDB) {
	db := assertOpen(b, dbType, true, true, "get_log")
	defer assertClose(b, db)

	raftbench.GetLog(b, db)
}

func BenchmarkLogDB_ChunkDB_GetLog(b *testing.B) {
	benchGetLog(b, dbTypes["chunkdb"])
}

func BenchmarkLogDB_LockFreeChunkDB_GetLog(b *testing.B) {
	benchGetLog(b, dbTypes["lock free chunkdb"])
}

func BenchmarkLogDB_InMemDB_GetLog(b *testing.B) {
	benchGetLog(b, dbTypes["inmem"])
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

func BenchmarkLogDB_InMemDB_StoreLog(b *testing.B) {
	benchStoreLog(b, dbTypes["inmem"])
}

// StoreLogs

func benchStoreLogs(b *testing.B, dbType logdb.LogDB) {
	db := assertOpen(b, dbType, true, true, "store_logs")
	defer assertClose(b, db)

	raftbench.StoreLogs(b, db)
}

func BenchmarkLogDB_ChunkDB_StoreLogs(b *testing.B) {
	benchStoreLogs(b, dbTypes["chunkdb"])
}

func BenchmarkLogDB_LockFreeChunkDB_StoreLogs(b *testing.B) {
	benchStoreLogs(b, dbTypes["lock free chunkdb"])
}

func BenchmarkLogDB_InMemDB_StoreLogs(b *testing.B) {
	benchStoreLogs(b, dbTypes["inmem"])
}

// DeleteRange

func benchDeleteRange(b *testing.B, dbType logdb.LogDB) {
	db := assertOpen(b, dbType, true, true, "delete_range")
	defer assertClose(b, db)

	// raftbench.DeleteRange(b, db)

	// This is the hashicorp benchmark, modified to not have gaps.
	var logs []*raft.Log
	for n := 0; n < b.N; n++ {
		for i := 0; i < 5; i++ {
			logs = append(logs, &raft.Log{Index: uint64(n*5 + i + 1), Data: []byte("data")})
		}
	}
	if err := db.StoreLogs(logs); err != nil {
		b.Fatalf("err: %s", err)
	}
	b.ResetTimer()

	// Delete a range of the data
	for n := 0; n < b.N; n++ {
		offset := 5 * n
		if err := db.DeleteRange(uint64(offset), uint64(offset+4)); err != nil {
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

func BenchmarkLogDB_InMemDB_DeleteRange(b *testing.B) {
	benchDeleteRange(b, dbTypes["inmem"])
}
