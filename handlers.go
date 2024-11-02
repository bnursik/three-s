package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func badRequest(w http.ResponseWriter, r *http.Request) {
	writeHttpError(w, http.StatusBadRequest, "Bad Request", "Wrong http method")
}

func putBucket(w http.ResponseWriter, r *http.Request) {
	//validating bucket name

	bucketName := r.PathValue("BucketName")
	valid, errMsg := isValidBucketName(bucketName)
	if !valid {
		writeHttpError(w, http.StatusBadRequest, "InvalidBucketName", errMsg) //error 400
		return
	}

	bucketPath := filepath.Join(dirPath, bucketName)
	_, err := os.Stat(bucketPath)
	if !(err != nil && os.IsNotExist(err)) {
		writeHttpError(w, http.StatusConflict, "BucketNameUnavailable", "Bucket with this name already exists") // error 409
		return
	}

	//creating bucket

	err = os.Mkdir(bucketPath, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "BucketCreationError", "Could not create bucket")
		return
	}

	inf, err := os.Stat(bucketPath)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "BucketAccessError", "Could not access the bucket")
		return
	}

	bucketMetadata, err := os.OpenFile(filepath.Join(dirPath, "buckets.csv"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()
	modTime := inf.ModTime()
	modTimeToString := fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", modTime.Year(), modTime.Month(), modTime.Day(), modTime.Hour(), modTime.Minute(), modTime.Second())

	csvWriter := csv.NewWriter(bucketMetadata)
	err = csvWriter.Write([]string{bucketName, modTimeToString, modTimeToString, "Active"})
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update bucket metadata")
		return
	}
	csvWriter.Flush()

	objectMetadata, err := os.OpenFile(filepath.Join(bucketPath, "objects.csv"), os.O_CREATE|os.O_WRONLY, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not create object metadata")
		return
	}
	defer objectMetadata.Close()
	_, err = objectMetadata.Write([]byte("ObjectKey,Size,ContentType,LastModified\n"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update object metadata")
		return
	}

	//xml response
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<CreateBucketResult>")
	fmt.Fprintln(w, "\t<Name>"+bucketName+"</Name>")
	fmt.Fprintln(w, "\t<CreationTime>"+modTimeToString+"</CreationTime>")
	fmt.Fprintln(w, "\t<Status>Active</Status>")
	fmt.Fprintln(w, "</CreateBucketResult>")

}

func listAllBuckets(w http.ResponseWriter, r *http.Request) {
	bucketMetadata, err := os.Open(filepath.Join(dirPath, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	csvReader := csv.NewReader(bucketMetadata)
	var buckets []map[string]string

	//checking first line
	_, err = csvReader.Read()
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}

	fields, err := csvReader.Read()
	for err == nil && len(fields) == 4 {
		bucket := make(map[string]string)
		bucket["Name"] = fields[0]
		bucket["CreationTime"] = fields[1]
		bucket["LastModifiedTime"] = fields[2]
		bucket["Status"] = fields[3]
		if bucket["Status"] == "Active" {
			buckets = append(buckets, bucket)
		}
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Invalid metadata content")
		return
	}

	w.WriteHeader(http.StatusOK)
	//xml response
	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<ListAllMyBucketsResult>")
	fmt.Fprintln(w, "\t<Buckets>")
	for _, bucket := range buckets {
		fmt.Fprintln(w, "\t\t<Bucket>")
		fmt.Fprintln(w, "\t\t\t<Name>"+bucket["Name"]+"</Name>")
		fmt.Fprintln(w, "\t\t\t<CreationTime>"+bucket["CreationTime"]+"</CreationTime>")
		fmt.Fprintln(w, "\t\t\t<LastModifiedTime>"+bucket["LastModifiedTime"]+"</LastModifiedTime>")
		fmt.Fprintln(w, "\t\t\t<Status>"+bucket["Status"]+"</Status>")
		fmt.Fprintln(w, "\t\t</Bucket>")
	}
	fmt.Fprintln(w, "\t</Buckets>")
	fmt.Fprintln(w, "</ListAllMyBucketsResult>")
}

func deleteBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("BucketName")

	bucketMetadata, err := os.Open(filepath.Join(dirPath, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	csvReader := csv.NewReader(bucketMetadata)
	fields, err := csvReader.Read()
	found := false
	buckets := make([][]string, 0)
	for err == nil && len(fields) == 4 {
		if fields[0] == bucketName {
			found = true
			fields[3] = "Deleted"
		}
		buckets = append(buckets, fields)
		fields, err = csvReader.Read()
	}

	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}

	if !found {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Could not delete - bucket does not exist")
		return
	}

	objectMetadata, err := os.Open(filepath.Join(dirPath, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access object metadata")
		return
	}
	defer objectMetadata.Close()

	csvReader = csv.NewReader(objectMetadata)
	_, err = csvReader.Read()
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access object metadata")
		return
	}

	_, err = csvReader.Read()
	if err != io.EOF {
		if err == nil {
			writeHttpError(w, http.StatusConflict, "BucketNotEmpty", "Could not delete - bucket not empty")
		} else {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read object metadata")
		}
		return
	}

	err = os.Remove(filepath.Join(dirPath, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not delete object metadata")
		return
	}

	err = os.Remove(filepath.Join(dirPath, bucketName))
	deleted := true
	if err != nil {
		deleted = false
	}

	metadataWrite, err := os.OpenFile(filepath.Join(dirPath, "buckets.csv"), os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer metadataWrite.Close()

	defer metadataWrite.Close()
	csvWriter := csv.NewWriter(metadataWrite)
	for _, bucket := range buckets {
		if bucket[0] != bucketName || !deleted {
			err = csvWriter.Write(bucket)
			if err != nil {
				writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update bucket metadata")
				return
			}
		}
	}
	csvWriter.Flush()
	w.WriteHeader(http.StatusNoContent)

}


func putObject(w http.ResponseWriter, r *http.Request) {
	bucketMetadata, err := os.Open(filepath.Join(dirPath, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	bucketName := r.PathValue("BucketName")
	csvReader := csv.NewReader(bucketMetadata)
	fields, err := csvReader.Read()
	var buckets[][]string
	found := false

	for err == nil {
		if fields[0] == bucketName{
			found = true
			now := time.Now()
			fields[2] = fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
	}

	buckets = append(buckets, fields)
	fields, err = csvReader.Read()
}

	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetaData", "Could not read bucket metadata")
		return
	}
	if !found{
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Bucket does not exist")
		return
	}

	objectKey := r.PathValue("ObjectKey")
	if objectKey == "objects.csv" {
		writeHttpError(w, http.StatusForbidden, "MetadaAccesDenied", "Metada Acces id denied")
		return
	}

	if len(objectKey) > 1024 {
		writeHttpError(w, http.StatusBadRequest, "ObjectKeyInvalid", "Object key is too long (> 1024)")
		return
	}

	objectMetadata, err := os.Open(filepath.Join(dirPath, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusBadRequest, "MetadataError", "Could not open object metadata")
		return
	}
	defer objectMetadata.Close()

	csvReader = csv.NewReader(objectMetadata)
	fields, err = csvReader.Read()
	objectFound := false
	var objects[][]string
	for err == nil {
		if fields[0] == objectKey {
			objectFound = true
			fields[1] = r.Header.Get("Content-Length")
			fields[2] = r.Header.Get("Content-Type")
			if len(fields[2]) == 0 {
				fields[2] = "text/plain"
			}
			now := time.Now()
			fields[3] = fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
		}
		objects = append(objects, fields)
		fields, err = csvReader.Read()
	}

	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read object metadata")
		return
	}

	if !objectFound {
		now := time.Now()
		newRec := []string{objectKey, r.Header.Get("Content-Length"), r.Header.Get("Content-Type"), fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())}
		if len(newRec[2]) == 0 {
			newRec[2] = "text/plain"
		}
		objects = append(objects, newRec)
	}

	object, err := os.OpenFile(filepath.Join(dirPath, bucketName, objectKey), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "ObjectAccessError", "Could not access object")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "RequestBodyError", "Could not get request body")
		return
	}

	_, err = object.Write(body)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "ObjectWriteError", "Could not write to object")
		return
	}

	objMetadataWrite, err := os.OpenFile(filepath.Join(dirPath, bucketName, "objects.csv"), os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update object metadata")
		return
	}
	defer objMetadataWrite.Close()

	csvWriter := csv.NewWriter(objMetadataWrite)
	for _, object := range objects {
		err = csvWriter.Write(object)
		if err != nil {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not write to object metadata")
			return
		}
	}
	csvWriter.Flush()

	bktMetadataWrite, err := os.OpenFile(filepath.Join(dirPath, "buckets.csv"), os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update bucket metadata")
		return
	}
	defer bktMetadataWrite.Close()

	csvWriter = csv.NewWriter(bktMetadataWrite)
	for _, bucket := range buckets {
		err = csvWriter.Write(bucket)
		if err != nil {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not write to bucket metadata")
			return
		}
	}
	csvWriter.Flush()

}

func getObject(w http.ResponseWriter, r *http.Request){
	//searching for bucket
	bucketMetadata, err := os.Open(filepath.Join(dirPath, "buckets.csv")) 
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not open bucket metadata")
	}

	bucketName := r.PathValue("BucketName")
	csvReader := csv.NewReader(bucketMetadata)
	fields, err := csvReader.Read()
	bucketFound := false
	for err == nil {
		if fields[0] == bucketName {
			bucketFound = true
			break
		}

		fields, err = csvReader.Read()
	}

	if err != io.EOF && !bucketFound{
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}

	if !bucketFound{
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Could not found bucket")
		return
	}


	//seearching for object
	objectMetadata, err := os.Open(filepath.Join(dirPath, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusBadRequest, "MetadataError", "Could not open object metadata")
		return
	}
	defer objectMetadata.Close()

	objectKey := r.PathValue("ObjectKey")
	if objectKey == "object.csv" {
		writeHttpError(w, http.StatusForbidden, "AccesDenied", "Acces is forbidden")
		return
	}
	
	csvReader = csv.NewReader(objectMetadata)
	fields, err = csvReader.Read()
	var objectInfo []string
	for err == nil {
		if fields[0] == objectKey {
			objectInfo = fields
			break
		}
		fields, err = csvReader.Read()
	}

	if err != io.EOF && len(objectInfo) == 0 {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read object metadata")
		return
	}

	if len(objectInfo) == 0 {
		writeHttpError(w, http.StatusNotFound, "ObjectNotFound", "Object does not exist")
		return
	}

	content, err := os.ReadFile(filepath.Join(dirPath, bucketName, objectKey))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "ObjectAccessError", "Could not access object")
		return
	}

	w.Header().Add("Content-Length", objectInfo[1])
	w.Write(content)
}

func deleteObject(w http.ResponseWriter, r *http.Request) {
	bucketMetadata, err := os.Open(filepath.Join(dirPath, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	bucketName := r.PathValue("BucketName")
	csvReader := csv.NewReader(bucketMetadata)
	fields, err := csvReader.Read()
	var buckets [][]string
	bucketFound := false
	for err == nil {
		if fields[0] == bucketName {
			bucketFound = true
		}
		buckets = append(buckets, fields)
		fields, err = csvReader.Read()
	}

	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}

	if !bucketFound {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Bucket does not exist")
		return
	}

	objectKey := r.PathValue("ObjectKey")
	if objectKey == "objects.csv" {
		writeHttpError(w, http.StatusForbidden, "MetadataAccessDenied", "Metadata deleting is forbidden")
		return
	}

	objectMetadata, err := os.Open(filepath.Join(dirPath, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access object metadata")
		return
	}
	defer objectMetadata.Close()

	csvReader = csv.NewReader(objectMetadata)
	fields, err = csvReader.Read()
	var objects [][]string
	objectFound := false
	for err == nil {
		if fields[0] == objectKey {
			objectFound = true
		} else {
			objects = append(objects, fields)
		}
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read object metadata")
		return
	}
	if !objectFound {
		writeHttpError(w, http.StatusNotFound, "ObjectNotFound", "Object does not exist")
		return
	}

	err = os.Remove(filepath.Join(dirPath, bucketName, objectKey))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "ObjectDeletionError", "Could not delete object")
		return
	}

	objMetadataWrite, err := os.OpenFile(filepath.Join(dirPath, bucketName, "objects.csv"), os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update object metadata")
		return
	}
	defer objMetadataWrite.Close()

	csvWriter := csv.NewWriter(objMetadataWrite)
	for _, obj := range objects {
		err = csvWriter.Write(obj)
		if err != nil {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not write to object metadata")
			return
		}
	}
	csvWriter.Flush()

	bktMetadataWrite, err := os.OpenFile(filepath.Join(dirPath, "buckets.csv"), os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update bucket metadata")
		return
	}
	defer bktMetadataWrite.Close()
	csvWriter = csv.NewWriter(bktMetadataWrite)
	for i, bkt := range buckets {
		if bkt[0] == bucketName {
			now := time.Now()
			buckets[i][2] = fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
		}
		err = csvWriter.Write(buckets[i])
		if err != nil {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not write to bucket metadata")
			return
		}
	}
	csvWriter.Flush()
	w.WriteHeader(http.StatusNoContent)
}
