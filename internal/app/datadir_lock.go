package app

import (
	"fmt"
	"strconv"
)

// LockHeldError reports that LockFile found its lock taken by another holder:
// normally another Parkana process, or another LockFile call on the same path
// in this process (a flock belongs to an open file, not to a process).
type LockHeldError struct {
	// Path is the absolute path of the lock file.
	Path string
	// PID is the process id the holder wrote into the lock file, or 0 when
	// the file held none yet (the holder had not written it) or it was
	// unreadable. It is for the error text only.
	PID int
}

// Error names the lock file and its holder.
func (e *LockHeldError) Error() string {
	return fmt.Sprintf("app: lock %s is held by another process (%s)", e.Path, e.Holder())
}

// Holder describes the holding process for an error text: "pid N", or
// "pid unknown" when the lock file names none.
func (e *LockHeldError) Holder() string {
	if e.PID <= 0 {
		return "pid unknown"
	}
	return "pid " + strconv.Itoa(e.PID)
}
