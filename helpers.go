package main

import (
	"errors"
	"strings"
	"unicode"
)

// isValidBucketName checks if the bucket name meets the specified naming conventions without using regexp.
func isValidBucketName(name string) error {
	// Check length constraints
	if len(name) < 3 || len(name) > 63 {
		return errors.New("bucket name must be between 3 and 63 characters")
	}

	// Check allowed characters and consecutive periods/dashes
	prevChar := rune(0)
	for i, ch := range name {
		if !unicode.IsLower(ch) && !unicode.IsDigit(ch) && ch != '-' && ch != '.' {
			return errors.New("bucket name can only contain lowercase letters, numbers, dots, and hyphens")
		}
		if (ch == '.' || ch == '-') && prevChar == ch {
			return errors.New("bucket name must not contain consecutive periods or dashes")
		}
		prevChar = ch
	}

	// Check if name is formatted as an IP address
	if isIPAddress(name) {
		return errors.New("bucket name must not be formatted as an IP address")
	}

	// Check start and end constraints
	if name[0] == '-' || name[len(name)-1] == '-' {
		return errors.New("bucket name must not begin or end with a hyphen")
	}

	return nil
}

// isIPAddress checks if the name follows the format of an IP address.
func isIPAddress(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if len(part) == 0 || len(part) > 3 {
			return false
		}
		for _, ch := range part {
			if !unicode.IsDigit(ch) {
				return false
			}
		}
	}
	return true
}
