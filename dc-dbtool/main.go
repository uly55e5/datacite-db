package main

import (
	"context"
	"datacite-db/datacite"
	"flag"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"golang.org/x/sync/semaphore"
	"io/fs"
	"io/ioutil"
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
		client     *mongo.Client
		err        error
		files      []fs.FileInfo
		collection *mongo.Collection
	)

	var count = datacite.Count{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(*dbConnPtr)); err != nil {
		println(err.Error())
		return
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			println(err.Error())
			return
		}
	}()

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		println(err.Error())
		return
	}

	idModels := datacite.GetIndexes()

	collection = client.Database(*dbNamePtr).Collection("datacite")
	var names []string
	if names, err = collection.Indexes().CreateMany(context.Background(), idModels); err != nil {
		println("Error while creating index:", err.Error())
	}
	println("Index:", names)

	if files, err = ioutil.ReadDir(*dataDirPtr); err != nil {
		println("Read error:", err.Error())
		return
	}

	count.Upserted, err = collection.CountDocuments(context.Background(), bson.D{}, nil)

	var modelChan = make(chan *mongo.ReplaceOneModel, *dataSetBufferSizePtr)
	var countChan = make(chan datacite.Count, *countBufferSizePtr)
	var doneChan = make(chan bool)
	var wg sync.WaitGroup
	for i := 0; i < *maxDbWorkersPtr; i++ {
		wg.Add(1)
		go datacite.AddModels(modelChan, collection, countChan, doneChan, &wg)
	}
	go datacite.LogCount(count, countChan)
	sem := semaphore.NewWeighted(*parallelDatasetsPtr)
	fileSem := semaphore.NewWeighted(*parallelFilesPtr)
	for _, fileInfo := range files {
		if err = fileSem.Acquire(context.Background(), 1); err != nil {
			println("Could not acquire file semaphore.", err.Error())
		}
		datacite.ReadFile(*dataDirPtr+"/"+fileInfo.Name(), sem, modelChan, fileSem)
	}
	time.Sleep(60 * time.Second)
	doneChan <- true
	wg.Wait()
	println(count.Upserted, count.Modified)
}
