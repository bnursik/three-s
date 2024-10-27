package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

// func createBucket(w http.ResponseWriter, r *http.Request) {
// 	bucketName := r.PathValue("BucketName")

// 	if err := isValidBucketName(bucketName); err != nil {
// 		w.WriteHeader(http.StatusBadRequest)
// 		return
// 	}

// 	// Check if bucket already exists
// 	if bucketExists(bucketName) {
// 		w.WriteHeader(http.StatusConflict)
// 		return
// 	}

// 	// Create the bucket directory
// 	bucketDir := filepath.Join(dataDirectory, bucketName)
// 	if err := os.MkdirAll(bucketDir, os.ModePerm); err != nil {
// 		fmt.Printf("failed to create bucket directory: %w", err)
// 		return
// 	}

// 	// Save bucket metadata
// 	if err := saveBucketMetadata(bucketName); err != nil {
// 		fmt.Printf("failed to save bucket metadata: %w", err)
// 		return
// 	}

// 	fmt.Printf("Bucket '%s' created successfully.\n", bucketName)
// }

func main() {
	port := flag.String("port", "8080", "Port for th HTTP server")
	dir := flag.String("dir", "./data", "Directory for storing files")
	flag.Parse()

	//creating directory if it is not exist
	_, err := os.Stat(*dir)
	if os.IsNotExist(err) {
		err = os.Mkdir(*dir, os.ModePerm)
		if err != nil {
			log.Fatalf("Could nor create data directoty: %v", err)
		}
	} else if err != nil {
		log.Fatalf("Error checking directory: %v", err)
	}

	//http.HandleFunc("PUT /{BucketName}", createBucket)
	http.HandleFunc("/status", statusHandler)

	fmt.Printf("Server is running on port %s\n", *port)
	err = http.ListenAndServe(":"+*port, nil)
	if err != nil {
		log.Fatalf("Could not start the server: %v", err)
	}
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Server is running!")
}
