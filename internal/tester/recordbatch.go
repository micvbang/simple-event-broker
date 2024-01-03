package tester

import (
	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/stringy"
)

func MakeRandomRecordBatch(size int) [][]byte {
	expectedRecordBatch := make([][]byte, size)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = []byte(stringy.RandomN(1 + inty.RandomN(50)))
	}
	return expectedRecordBatch
}
