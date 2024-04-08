package tester

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TempDir calls os.MkdirTemp and fails the test if there is an error.
func TempDir(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	return tempDir
}
