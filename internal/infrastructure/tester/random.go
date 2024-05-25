package tester

import (
	"crypto/rand"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/stretchr/testify/require"
)

func RandomBytes(t *testing.T, size int) []byte {
	bs := make([]byte, size)
	_, err := rand.Read(bs)
	require.NoError(t, err)

	return bs
}

func RandomRecord(t *testing.T, size int) recordbatch.Record {
	return RandomBytes(t, size)
}
