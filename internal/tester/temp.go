package tester

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TempFile calls os.CreateTemp and fails the test if there is an error.
func TempFile(t *testing.T) *os.File {
	f, err := os.CreateTemp("", "seb_*")
	require.NoError(t, err)

	return f
}
