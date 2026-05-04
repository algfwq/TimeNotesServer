package protocol

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

func NewID(prefix string) string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return prefix + "-fallback"
	}
	return prefix + "-" + hex.EncodeToString(buf[:])
}

func NewSecret(prefix string) string {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return prefix + "-fallback"
	}
	return prefix + "-" + base64.RawURLEncoding.EncodeToString(buf[:])
}

func CleanID(value string, fallbackPrefix string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return NewID(fallbackPrefix)
	}
	return value
}

func CleanText(value string, fallback string, max int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	if len(value) > max {
		return value[:max]
	}
	return value
}
