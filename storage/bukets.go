package storage

import (
	"net/http"
)

func createBucket(w http.ResponseWriter, r *http.Request) {

}

func init() {
	http.HandleFunc("PUT /buckets/", createBucket)
}
