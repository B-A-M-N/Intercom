package main

import (
	"github.com/google/uuid"
)

// newCommandID mints a command identifier for an interactive submission.
//
// The identity is time-ordered so that a client can sort its own
// submissions, and it is unique so that a retry of the same submission
// keeps its idempotency identity.
func newCommandID() string {
	u, err := uuid.NewV7()
	if err != nil {
		return "cmd_unknown"
	}
	return "cmd_" + u.String()
}
