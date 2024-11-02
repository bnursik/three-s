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

func putObject(w http.ResponseWriter, r *http.Request) {
	// searching and modifying bucket
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
	found := false

	for err == nil {
		if fields[0] == bucketName {
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
	if !found {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Bucket does not exist")
		return
	}

	// creating or re-writing object
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
	var objects [][]string
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

func getObject(w http.ResponseWriter, r *http.Request) {
	// searching for bucket
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

	if err != io.EOF && !bucketFound {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}

	if !bucketFound {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Could not found bucket")
		return
	}

	// seearching for object
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

	// deleting the object
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
