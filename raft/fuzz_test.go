package raft

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// Fuzz tester comparing this to hashicorp/raft-boltdb.
func TestFuzz(t *testing.T) {
	logdb := assertCreate(t, "fuzz")
	defer assertClose(t, logdb)
	boltdb := assertCreateBoltStore(t, "fuzz")
	defer boltdb.Close()

	rand := rand.New(rand.NewSource(0))

	if err := fuzzLogStore(boltdb, logdb, rand, 256); err != nil {
		t.Fatal(err)
	}
}

/// FUZZ TESTER

// Compare a "test" implementation with a "spec" implementation by
// performing a sequence of random operations and comparing the
// outputs.
func fuzzLogStore(spec raft.LogStore, test raft.LogStore, rand *rand.Rand, maxops int) error {
	// Keep track of the last log entry generated, so indices and
	// terms will be strictly increasing.
	lastLog := raft.Log{Index: 1 + uint64(rand.Intn(10))}

	for i := 0; i < maxops; i++ {
		action := 3 // rand.Intn(6)

		switch action {
		case 0:
			specFirst, specErr := spec.FirstIndex()
			testFirst, testErr := test.FirstIndex()

			fmt.Printf("-> calling FirstIndex\n")

			if !compareErrors(specErr, testErr) {
				return notExpected("FirstIndex", "error values inconsistent", specErr, testErr)
			}
			if specErr != nil {
				continue
			}
			if specFirst != testFirst {
				return notExpected("FirstIndex", "first indices not equal", specFirst, testFirst)
			}
		case 1:
			specFirst, specErr := spec.LastIndex()
			testFirst, testErr := test.LastIndex()

			fmt.Printf("-> calling LastIndex\n")

			if !compareErrors(specErr, testErr) {
				return notExpected("LastIndex", "error values inconsistent", specErr, testErr)
			}
			if specErr != nil {
				continue
			}
			if specFirst != testFirst {
				return notExpected("LastIndex", "indices not equal", specFirst, testFirst)
			}
		case 2:
			// Generate an index, weighted towards something in range.
			first, _ := spec.FirstIndex()
			last, _ := spec.LastIndex()
			idrange := int64(last) - int64(first)

			// It's a little annoying that the Int*n functions can't accept 0 as an upper bound.
			index := rand.Int63n(26) - 50 + int64(first)
			if idrange > 0 {
				index = rand.Int63n(idrange) + index
			}
			idx := uint64(index)

			fmt.Printf("-> calling GetLog %v\n", idx)

			specLog := new(raft.Log)
			specErr := spec.GetLog(idx, specLog)
			testLog := new(raft.Log)
			testErr := test.GetLog(idx, testLog)

			if !compareErrors(specErr, testErr) {
				return notExpected("GetLog", fmt.Sprintf("error values inconsistent for ID %v", idx), specErr, testErr)
			}
			if specErr != nil {
				continue
			}
			if !compareLogs(specLog, testLog) {
				return notExpected("GetLog", fmt.Sprintf("log entries not equal for ID %v", idx), specLog, testLog)
			}
		case 3:
			lastLog = randLog(lastLog, rand)

			specErr := spec.StoreLog(&lastLog)
			testErr := test.StoreLog(&lastLog)

			fmt.Printf("-> calling StoreLog %v\n", lastLog)

			if !compareErrors(specErr, testErr) {
				return notExpected("StoreLog", "error values inconsistent", specErr, testErr)
			}
		case 4:
			logs := make([]*raft.Log, rand.Intn(100))
			logsV := make([]raft.Log, len(logs))
			for i := range logs {
				lastLog = randLog(lastLog, rand)
				logsV[i] = lastLog
				logs[i] = &logsV[i]
			}

			fmt.Printf("-> calling StoreLogs %v\n", logsV)

			specErr := spec.StoreLogs(logs)
			testErr := test.StoreLogs(logs)

			if !compareErrors(specErr, testErr) {
				return notExpected("StoreLogs", "error values inconsistent", specErr, testErr)
			}
		case 5:
			// Delete randomly from either the front or back, not the middle. This matches use of
			// this method within hashicorp/raft itself.
			first, _ := spec.FirstIndex()
			last, _ := spec.LastIndex()

			// Same issue here wth rand.Int63n as above.
			if first != last {
				if rand.Intn(2) == 0 {
					first += uint64(rand.Int63n(int64(last - first)))
				} else {
					last -= uint64(rand.Int63n(int64(last - first)))
				}
			}

			fmt.Printf("-> calling DeleteRange %v %v\n", first, last)

			specErr := spec.DeleteRange(first, last)
			testErr := test.DeleteRange(first, last)

			if !compareErrors(specErr, testErr) {
				return notExpected("DeleteRange", "error values inconsistent", specErr, testErr)
			}
			if specErr != nil {
				continue
			}
		}

		// After every operation, check the indices are consistent.
		specFirst, specErr := spec.FirstIndex()
		testFirst, testErr := test.FirstIndex()

		if !compareErrors(specErr, testErr) {
			return badInvariant("error values of FirstIndex inconsistent", specErr, testErr)
		}
		if specErr != nil {
			continue
		}
		if specFirst != testFirst {
			return badInvariant("first indices not equal", specFirst, testFirst)
		}

		specLast, specErr := spec.LastIndex()
		testLast, testErr := test.LastIndex()

		if !compareErrors(specErr, testErr) {
			return badInvariant("error values of LastIndex inconsistent", specErr, testErr)
		}
		if specErr != nil {
			continue
		}
		if specLast != testLast {
			return badInvariant("last indices not equal", specLast, testLast)
		}
	}

	return nil
}

// Compare two errors by checking both are nil or non-nil.
func compareErrors(err1 error, err2 error) bool {
	return !((err1 == nil && err2 != nil) || (err1 != nil && err2 == nil))
}

// Compare two raft log values.
func compareLogs(log1 *raft.Log, log2 *raft.Log) bool {
	return log1.Index == log2.Index && log1.Term == log2.Term && log1.Type == log2.Type && reflect.DeepEqual(log1.Data, log2.Data)
}

// Construct an error message.
func notExpected(method, msg string, expected, actual interface{}) error {
	return fmt.Errorf("[%s] %s\nexpected: %v\nactual:   %v", method, msg, expected, actual)
}

// Construct an error message.
func badInvariant(msg string, expected, actual interface{}) error {
	return fmt.Errorf("INVARIANT: %s\nexpected: %v\nactual:   %v", msg, expected, actual)
}

// Generate a random log entry. The pair (index,term) is strictly
// increasing between invocations.
func randLog(lastLog raft.Log, rand *rand.Rand) raft.Log {
	index := lastLog.Index + 1
	term := lastLog.Term

	// Bias towards entries in the same term.
	if rand.Intn(5) == 0 {
		term++
	}

	return raft.Log{
		Index: index,
		Term:  term,
		Type:  raft.LogType(rand.Uint32()),
		Data:  []byte(fmt.Sprintf("entry %v %v", index, term)),
	}
}

/// ASSERTIONS

func assertCreateBoltStore(t testing.TB, testName string) *raftboltdb.BoltStore {
	_ = os.RemoveAll("../test_db/raft/" + testName + "_bolt")
	db, err := raftboltdb.NewBoltStore("../test_db/raft/" + testName + "_bolt")
	if err != nil {
		t.Fatal(err)
	}
	return db
}
