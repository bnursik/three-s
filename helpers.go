package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
)

func isValidBucketName(name string) error {

	if len(name) < 3 || len(name) > 63 {
		return errors.New("bucket name must be between 3 and 63 characters")
	}

	prevChar := rune(0)
	for _, ch := range name {
		if !unicode.IsLower(ch) && !unicode.IsDigit(ch) && ch != '-' && ch != '.' {
			return errors.New("bucket name can only contain lowercase letters, numbers, dots, and hyphens")
		}
		if (ch == '.' || ch == '-') && prevChar == ch {
			return errors.New("bucket name must not contain consecutive periods or dashes")
		}
		prevChar = ch
	}

	if isIPAddress(name) {
		return errors.New("bucket name must not be formatted as an IP address")
	}

	if name[0] == '-' || name[len(name)-1] == '-' {
		return errors.New("bucket name must not begin or end with a hyphen")
	}

	return nil
}

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

func bucketExists(bucketName string) bool {
	file, err := os.Open("buckets.csv")
	if err != nil {
		// If file doesn't exist, it means there are no buckets yet
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		fmt.Printf("Failed to open metadata file: %v\n", err)
		return false
	}
	defer file.Close()

	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		if record[0] == bucketName {
			return true
		}
	}
	return false
}

// saveBucketMetadata saves the bucket metadata to a CSV file.
func saveBucketMetadata(bucketName string) error {
	file, err := os.OpenFile("buckets.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write metadata entry for the new bucket
	creationTime := time.Now().Format(time.RFC3339)
	record := []string{bucketName, creationTime}
	return writer.Write(record)
}
