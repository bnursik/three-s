package main

import (
	"flag"
	"log"
	"os"
)

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

	StartServer(*port) //starting server

}
