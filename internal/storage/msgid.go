package storage

import (
	"fmt"
	"sync/atomic"
)

var msgCounter uint64

func newMsgID() string {
	return fmt.Sprintf("msg-%d", atomic.AddUint64(&msgCounter, 1))
}

// NewMsgID is the exported version for use by handlers.
func NewMsgID() string { return newMsgID() }
