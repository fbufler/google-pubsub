package storage

import (
	"crypto/rand"
	"encoding/hex"
)

func newAckID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
