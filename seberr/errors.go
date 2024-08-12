package seberr

import (
	"errors"
)

var (
	ErrOutOfBounds        = errors.New("offset out of bounds")
	ErrTopicNotFound      = errors.New("topic not found")
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrNotInCache         = errors.New("not in cache")
	ErrNotInStorage       = errors.New("not in storage")
	ErrUnauthorized       = errors.New("unauthorized")
	ErrPayloadTooLarge    = errors.New("payload too large")
	ErrBadInput           = errors.New("bad input")
	ErrBufferTooSmall     = errors.New("buffer too small")
	ErrNotAuthorized      = errors.New("not authorized")
	ErrNotFound           = errors.New("not found")
)
