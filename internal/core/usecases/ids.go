package usecases

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
)

var msgCounter int64

func newMsgID() string {
	return fmt.Sprintf("msg-%d", atomic.AddInt64(&msgCounter, 1))
}

func newAckID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
