package storage

import "fmt"

var (
	ErrOutOfBounds   = fmt.Errorf("out of bounds")
	ErrTopicNotFound = fmt.Errorf("topic not found")
)
