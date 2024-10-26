package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

func status(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Server is running!")
}

func main() {
	port := flag.String("port", "8080", "Port for th HTTP server")
	dir := flag.String("dir", "./data", "Directory for storing files")
	flag.Parse()

	_, err := os.Stat(*dir)
	if os.IsNotExist(err) {
		err = os.Mkdir(*dir, os.ModePerm)
		if err != nil {
			log.Fatalf("Could nor create data directoty: %v", err)
		}
	} else if err != nil {
		log.Fatalf("Error checking directory: %v", err)
	}

	http.HandleFunc("/status", status)

	err = http.ListenAndServe(":"+*port, nil)
	if err != nil {
		log.Fatalf("Could not start the server: %v", err)
	} else {
		fmt.Printf("Server is running on port %s\n", *port)
	}
}
