package client

import (
	"encoding/base64"
	"errors"
	"strings"
	"unicode/utf8"
)

func decodeBytes(value string, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf-8", "text":
		return []byte(value), nil
	case "base64", "b64":
		return base64.StdEncoding.DecodeString(value)
	default:
		return nil, errors.New("unsupported encoding")
	}
}

func encodeBytes(value []byte, encoding string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf-8", "text":
		if !utf8.Valid(value) {
			return "", errors.New("value is not valid utf-8")
		}
		return string(value), nil
	case "base64", "b64":
		return base64.StdEncoding.EncodeToString(value), nil
	default:
		return "", errors.New("unsupported encoding")
	}
}
