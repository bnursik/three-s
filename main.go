package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

var dirPath string

//TO-DO: gofumt, comments

func main() {
	http.HandleFunc("/", badRequest)
	
	http.HandleFunc("GET /{$}", listAllBuckets)

	http.HandleFunc("PUT /{BucketName}", putBucket)
	http.HandleFunc("PUT /{BucketName}/{$}", putBucket)

	http.HandleFunc("DELETE /{BucketName}", deleteBucket)
	http.HandleFunc("DELETE /{BucketName}/{$}", deleteBucket)

	http.HandleFunc("PUT /{BucketName}/{ObjectKey}", putObject)
	http.HandleFunc("PUT /{BucketName}/{ObjectKey}/{$}", putObject)

	http.HandleFunc("GET /{BucketName}/{ObjectKey}", getObject)
	http.HandleFunc("GET /{BucketName}/{ObjectKey}/{$}", getObject)

	http.HandleFunc("DELETE /{BucketName}/{ObjectKey}", deleteObject)
	http.HandleFunc("DELETE /{BucketName}/{ObjectKey}/{$}", deleteObject)


	
	portF := flag.String("port", "8080", "Port for th HTTP server")
	dir := flag.String("dir", "./data", "Directory for storing files")
	helpFlag := flag.Bool("help", false, "provides usage information")
	flag.Parse()

	if *helpFlag{
		helpMessage()
		os.Exit(0)
	}

	port, err := strconv.Atoi(*portF)
	if err != nil || port == 0{
		log.Fatal("Incorrect port")
	}

	//creating directory if it is not exist
	dirPath = *dir
	_, err = os.Stat(*dir)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}	

	if err != nil{
		err = os.Mkdir(*dir, os.ModePerm)
		if err != nil {
			log.Fatalf("Could nor create data directoty: %v", err)
		}

		bucketdata, err := os.OpenFile(filepath.Join(*dir, "buckets.csv"), os.O_CREATE|os.O_WRONLY, 0o755)
		if err != nil {
			log.Fatal("Could not create bucketdata")
		}
		_, err = bucketdata.Write([]byte("Name,CreationTime,LastModifiedTime,Status\n"))
		if err != nil {
			log.Fatal("Could not write to bucketdata")
		}
	} else{
		_, err := os.Stat(filepath.Join(*dir, "buckets.csv"))
		if err != nil && os.IsNotExist(err){
			bucketdata, err := os.OpenFile(filepath.Join(*dir, "buckets.csv"), os.O_CREATE|os.O_WRONLY, 0o755)
			if err != nil {
				log.Fatal("Could not create bucketdata")
			}
			_, err = bucketdata.Write([]byte("Name,CreationTime,LastModifiedTime,Status\n"))
			if err != nil {
				log.Fatal("Could not write to bucketdata")
			}
		}else if err != nil {
			log.Fatal("Buckets data error")
		}
	}

	fmt.Printf("Server is running on port %s\n", *portF)
	err = http.ListenAndServe(":"+*portF, nil)
	if err != nil {
		log.Fatalf("Could not start the server: %v", err)
	}
}