package main

import (
	"bytes"
	"context"
	"datacite-db/fuji"
	"encoding/json"
	"errors"
	"flag"
	"golang.org/x/sync/semaphore"
	"math/rand"
	"net/http"
	"time"
)

const (
	dbConnStrDefault = "mongodb://datacite:datacite@localhost:27017"
	//dataDirDefault           = "./data"
	//parallelFilesDefault     = 50
	//parallelDatasetsDefault  = 500
	//dataSetBufferSizeDefault = 10000
	//countBufferSizeDefault   = 100
	//maxDbWorkersDefault      = 3
	dbNameDefault   = "datacite-go"
	fujiUrlDefault  = "http://localhost:1071/fuji/api/v1/evaluate"
	fujiPwdDefault  = "fuji"
	fujiUserDefault = "fuji"
)

type Method string

const (
	RANDOM       Method = "RANDOM"
	CREATOR_LIST Method = "CREATORS"
)

var sem *semaphore.Weighted

func main() {
	//dataDirPtr := flag.String("data", dataDirDefault, "the data file directory")
	dbConnPtr := flag.String("dbconn", dbConnStrDefault, "Database connection string")
	//parallelFilesPtr := flag.Int64("files", parallelFilesDefault, "Number of open files")
	//parallelDatasetsPtr := flag.Int64("datasets", parallelDatasetsDefault, "Number of parallel datasets")
	//dataSetBufferSizePtr := flag.Int("buffer", dataSetBufferSizeDefault, "Buffer size for datasets")
	//countBufferSizePtr := flag.Int("countbuffer", countBufferSizeDefault, "Buffer size for count values")
	//maxDbWorkersPtr := flag.Int("workers", maxDbWorkersDefault, "maximum database workers")
	dbNamePtr := flag.String("database", dbNameDefault, "Database name")
	fujiUrlPtr := flag.String("fuji-url", fujiUrlDefault, "F-UJI API URl")
	fujiUserPtr := flag.String("fuji-user", fujiUserDefault, "F-UJI user name")
	fujiPwdPtr := flag.String("fuji-pwd", fujiPwdDefault, "F-UJI password")
	selectionMethod := flag.String("method", "RANDOM", "Set the method for dataset selection")
	flag.Parse()
	rand.Seed(time.Now().Unix())
	fuji.ConnectToDatabase(dbConnPtr, dbNamePtr)
	sem = semaphore.NewWeighted(6)
	for {
		sem.Acquire(context.Background(), 1)
		go InsertRandomFuji(fujiUrlPtr, fujiUserPtr, fujiPwdPtr)
	}
}

func InsertRandomFuji(fujiUrlPtr *string, fujiUserPtr *string, fujiPwdPtr *string) {
	var doi string
	var err error
	doi, err = fuji.GetRandomDatasetDoi()
	if err != nil {
		println(err.Error())
		print("-")
		sem.Release(1)
		return
	}

	if fuji.FujiExists(&doi) {
		print("*")
		sem.Release(1)
		return
	}
	dcVals := []bool{true, false}
	for _, dc := range dcVals {
		responseBody, err := getFujiResult(doi, fujiUrlPtr, fujiUserPtr, fujiPwdPtr, dc)
		if err != nil {
			println(err.Error())
			print("-")
			sem.Release(1)
			return
		}
		print("+")
		fuji.InsertFujiDataset(responseBody, doi, dc)
	}
	sem.Release(1)
}

func insertFromCreatorList(fujiUrlPtr *string, fujiUserPtr *string, fujiPwdPtr *string, fileNamePtr *string) {

}

func getFujiResult(doi string, fujiUrlPtr *string, fujiUserPtr *string, fujiPwdPtr *string, useDC bool) (map[string]interface{}, error) {
	var err error
	response, err := getFujiResponse(doi, fujiUrlPtr, fujiUserPtr, fujiPwdPtr, useDC)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return nil, errors.New("got status code " + response.Status + " from F-UJI")
	}
	var responseBody map[string]interface{}
	err = json.NewDecoder(response.Body).Decode(&responseBody)
	return responseBody, err
}

func getFujiResponse(doi string, fujiUrlPtr *string, fujiUserPtr *string, fujiPwdPtr *string, useDc bool) (*http.Response, error) {
	var httpClient = http.Client{}
	var err error
	body := struct {
		ObjectIdentifier string `json:"object_identifier"`
		TestDebug        bool   `json:"test_debug"`
		Datacite         bool   `json:"use_datacite"`
	}{"https://doi.org/" + doi, true, useDc}
	bodyjson, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest("POST", *fujiUrlPtr, bytes.NewReader(bodyjson))
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(*fujiUserPtr, *fujiPwdPtr)
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")
	response, err := httpClient.Do(request)
	return response, err
}
