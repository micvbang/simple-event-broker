package storage

import "fmt"

var (
	ErrOutOfBounds   = fmt.Errorf("out of bounds")
	ErrTopicNotFound = fmt.Errorf("topic not found")
	ErrNotInCache    = fmt.Errorf("not in cache")
)
