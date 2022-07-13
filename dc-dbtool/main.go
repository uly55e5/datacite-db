package main

import (
	"context"
	"datacite-db/datacite"
	"flag"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/semaphore"
	"io/fs"
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

const (
	dbConnStrDefault         = "mongodb://datacite:datacite@localhost:27017"
	dataDirDefault           = "./data"
	parallelFilesDefault     = 50
	parallelDatasetsDefault  = 500
	dataSetBufferSizeDefault = 10000
	countBufferSizeDefault   = 100
	maxDbWorkersDefault      = 3
	dbNameDefault            = "datacite-go"
)

func main() {
	dataDirPtr := flag.String("data", dataDirDefault, "the data file directory")
	dbConnPtr := flag.String("dbconn", dbConnStrDefault, "Database connection string")
	parallelFilesPtr := flag.Int64("files", parallelFilesDefault, "Number of open files")
	parallelDatasetsPtr := flag.Int64("datasets", parallelDatasetsDefault, "Number of parallel datasets")
	dataSetBufferSizePtr := flag.Int("buffer", dataSetBufferSizeDefault, "Buffer size for datasets")
	countBufferSizePtr := flag.Int("countbuffer", countBufferSizeDefault, "Buffer size for count values")
	maxDbWorkersPtr := flag.Int("workers", maxDbWorkersDefault, "maximum database workers")
	dbNamePtr := flag.String("database", dbNameDefault, "Database name")
	flag.Parse()
	var (
		err        error
		files      []fs.FileInfo
		collection *mongo.Collection
	)

	if collection, err = datacite.ConnectDataCiteCollection(dbConnPtr, dbNamePtr); err != nil {
		return
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = collection.Database().Client().Disconnect(ctx); err != nil {
			println(err.Error())
			return
		}
	}()

	if files, err = ioutil.ReadDir(*dataDirPtr); err != nil {
		println("Read error:", err.Error())
		return
	}

	var (
		modelChan     = make(chan *mongo.ReplaceOneModel, *dataSetBufferSizePtr)
		countChan     = make(chan datacite.Count, *countBufferSizePtr)
		doneChan      = make(chan bool)
		countDoneChan = make(chan bool)
		wg            sync.WaitGroup
		sem           = semaphore.NewWeighted(*parallelDatasetsPtr)
		fileSem       = semaphore.NewWeighted(*parallelFilesPtr)
	)

	go datacite.LogCount(countChan, countDoneChan)
	for i := 0; i < *maxDbWorkersPtr; i++ {
		wg.Add(1)
		go datacite.AddDocuments(modelChan, collection, countChan, doneChan, &wg, *dataSetBufferSizePtr)
	}

	for _, fileInfo := range files {
		if err = fileSem.Acquire(context.Background(), 1); err != nil {
			println("Could not acquire file semaphore.", err.Error())
		}
		if strings.HasSuffix(fileInfo.Name(), ".json") || strings.HasSuffix(fileInfo.Name(), ".ndjson") {
			datacite.ReadFile(*dataDirPtr+"/"+fileInfo.Name(), sem, modelChan, fileSem)
		}
	}
	time.Sleep(10 * time.Second)
	for i := 0; i < *maxDbWorkersPtr; i++ {
		doneChan <- true
	}
	wg.Wait()
	countDoneChan <- true
	time.Sleep(10 * time.Second)
	println("All done.")
}
