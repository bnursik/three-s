package main

import (
	"fmt"
	"log"
	"net/http"
)

func StartServer(port string) {
	err := http.ListenAndServe(":"+port, nil)

	fmt.Printf("Server is running on port %s\n", port)
	if err != nil {
		log.Fatalf("Could not start the server: %v", err)
	}
}
