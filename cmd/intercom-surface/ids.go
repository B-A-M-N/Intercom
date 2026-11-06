package main

import (
	"github.com/google/uuid"
)

// mintInstanceID mints an instance identifier for this process.
//
// UUIDv7 is time-ordered, so identifiers sort by creation without a
// coordination round trip. A restart mints a new instance id.
func mintInstanceID() string {
	u, err := uuid.NewV7()
	if err != nil {
		return "ins_unknown"
	}
	return "ins_" + u.String()
}

// mintSessionID mints a session identifier.
func mintSessionID() string {
	u, err := uuid.NewV7()
	if err != nil {
		return "ses_unknown"
	}
	return "ses_" + u.String()
}
