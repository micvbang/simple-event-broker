package tester

import "github.com/micvbang/go-helpy/stringy"

func MakeRandomRecordBatch(size int) [][]byte {
	expectedRecordBatch := make([][]byte, size)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = []byte(stringy.RandomN(10))
	}
	return expectedRecordBatch
}
