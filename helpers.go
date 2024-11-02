package main

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

func writeHttpError(w http.ResponseWriter, status int, errorCode string, message string) {
	w.WriteHeader(status)
	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<Error>")
	fmt.Fprintln(w, "\t<Code>"+errorCode+"</Code>")
	fmt.Fprintln(w, "\t<Message>")
	fmt.Fprintln(w, "\t\t"+message)
	fmt.Fprintln(w, "\t</Message>")
	fmt.Fprintln(w, "</Error>")
}

func isValidBucketName(bucketName string) (bool, string){
	if len(bucketName) < 3 || len(bucketName) > 63{
		return false, "Invalid length"
	}

	validName := regexp.MustCompile("^[a-z0-9.-]+$")
	if !validName.MatchString(bucketName) {
		return false, "Contains invalid characters"
	}

	ipRegex := regexp.MustCompile(`^(?:\d{1,3}\.){3}\d{1,3}$`)
	if ipRegex.MatchString(bucketName) {
		return false, "BucketName cannot be converted to the IP adreess"
	}

	if strings.HasPrefix(bucketName, "-") || strings.HasSuffix(bucketName, "-") {
		return false, "BucketName cannnot begin or end with hyphens"
	}

	if strings.HasPrefix(bucketName, ".") || strings.HasSuffix(bucketName, ".") {
		return false, "BucketName cannnot begin or end with dots"
	}

	if strings.Contains(bucketName, "..") || strings.Contains(bucketName, "--") {
		return false, "BucketName cannnot have consecutive hyphens/dots"
	}

	return true, ""

}

func helpMessage(){
	fmt.Println("Simple Storage Service.")
		fmt.Println()
		fmt.Println("**Usage:**")
		fmt.Println("\ttriple-s [-port <N>] [-dir <S>]")
		fmt.Println("\ttriple-s --help")
		fmt.Println()
		fmt.Println("**Options:**")
		fmt.Println("- --help\tShow this screen.")
		fmt.Println("- --port N\tPort number")
		fmt.Println("- --dir S\tPath to the directory")
}