package seb

import "fmt"

var (
	ErrOutOfBounds        = fmt.Errorf("offset out of bounds")
	ErrTopicNotFound      = fmt.Errorf("topic not found")
	ErrTopicAlreadyExists = fmt.Errorf("topic already exists")
	ErrNotInCache         = fmt.Errorf("not in cache")
	ErrNotInStorage       = fmt.Errorf("not in storage")
	ErrUnauthorized       = fmt.Errorf("unauthorized")
)
