package main

import (
	"encoding/base64"
	"errors"
	"strings"
	"unicode/utf8"
)

func decodeInput(value string, encoding string) ([]byte, error) {
	switch normalizeEncoding(encoding) {
	case "utf-8", "text":
		return []byte(value), nil
	case "base64", "b64":
		return base64.StdEncoding.DecodeString(value)
	default:
		return nil, errors.New("unsupported encoding")
	}
}

func encodeOutput(value []byte, encoding string) (string, error) {
	switch normalizeEncoding(encoding) {
	case "utf-8", "text":
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

func normalizeEncoding(encoding string) string {
	enc := strings.ToLower(strings.TrimSpace(encoding))
	if enc == "" {
		return "utf-8"
	}
	return enc
}
