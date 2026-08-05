package lsm

import "errors"

var (
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrStoreClosed        = errors.New("store closed")
	ErrIOFailure          = errors.New("io failure")
	ErrCorruptionDetected = errors.New("corruption detected")
	ErrNotImplemented     = errors.New("not implemented")
)
