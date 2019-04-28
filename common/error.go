package common

import (
	"database/sql"
	"fmt"
)

// These two are common
type temporary interface{ Temporary() bool }
type timeout interface{ Timeout() bool }

// This one is used in pq driver
type fatal interface{ Fatal() bool }

// IsErrorTemporary tries to guess if an error is temporary or fatal
// If unsure - report as temporary
func IsErrorTemporary(err error) bool {
	// Check specific error types first
	if err == sql.ErrNoRows {
		return false
	}

	// Then try common interfaces
	if e, ok := err.(temporary); ok {
		return e.Temporary()
	}

	if e, ok := err.(timeout); ok {
		return e.Timeout()
	}

	if e, ok := err.(fatal); ok {
		return !e.Fatal()
	}

	return true
}

// Error is a common error type with permanent flag
type Error struct {
	message   string
	permanent bool
}

func (e Error) Error() string {
	return e.message
}

// Temporary returns false if the error is permanent
func (e Error) Temporary() bool {
	return !e.permanent
}

func errGeneric(msg string, f ...interface{}) Error {
	return Error{
		message: fmt.Sprintf(msg, f...),
	}
}

// ErrorInheritf creates a new error.
// It tries to set the permanent flag based on the provided error.
func ErrorInheritf(err error, msg string, f ...interface{}) error {
	e := errGeneric(msg, f...)
	e.permanent = !IsErrorTemporary(err)
	return e
}

// ErrorPermf returns permanent error
func ErrorPermf(msg string, f ...interface{}) error {
	e := errGeneric(msg, f...)
	e.permanent = true
	return e
}

// ErrorTempf returns temporary error
func ErrorTempf(msg string, f ...interface{}) error {
	return errGeneric(msg, f...)
}
