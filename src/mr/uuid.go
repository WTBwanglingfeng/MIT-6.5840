package mr

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func newUUID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate UUID: %w", err)
	}

	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80

	encoded := hex.EncodeToString(id[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}
