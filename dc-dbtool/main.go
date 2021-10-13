package main

import (
	"context"
	"datacite-db/datacite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"golang.org/x/sync/semaphore"
	"io/fs"
	"io/ioutil"
	"time"
)

const (
	dbConnStr         = "mongodb://datacite:datacite@localhost:27017"
	dataDir           = "/home/uly55e5/Projekte/ipb/dcdump/temp-20210917/"
	parallelFiles     = 50
	parallelDatasets  = 500
	dataSetBufferSize = 10000
	countBufferSize   = 100
	maxDbWorkers      = 3
	dbName            = "datacite-go"
)

func main() {

	var (
		client     *mongo.Client
		err        error
		files      []fs.FileInfo
		collection *mongo.Collection
	)

	var count = datacite.Count{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(dbConnStr)); err != nil {
		println(err.Error())
		return
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			println(err.Error())
			return
		}
	}()

	if client.Ping(ctx, readpref.Primary()) != nil {
		println(err.Error())
		return
	}

	var idModels []mongo.IndexModel
	idIndexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "id", Value: 1}},
		Options: options.Index().SetUnique(true),
	}
	idModels = append(idModels, idIndexModel)
	updateIndexModel := mongo.IndexModel{
		Keys:    bson.D{{"attributes.updated", 1}},
		Options: options.Index(),
	}
	idModels = append(idModels, updateIndexModel)

	collection = client.Database(dbName).Collection("datacite")
	var names []string
	names, err = collection.Indexes().CreateMany(context.Background(), idModels)
	if err != nil {
		println("Error while creating index:", err.Error())
	}
	println("Index:", names)

	if files, err = ioutil.ReadDir(dataDir); err != nil {
		println("Read error:", err.Error())
		return
	}

	count.Upserted, err = collection.CountDocuments(context.Background(), bson.D{}, nil)

	var modelChan = make(chan *mongo.ReplaceOneModel, dataSetBufferSize)
	var countChan = make(chan datacite.Count, countBufferSize)
	for i := 0; i < maxDbWorkers; i++ {
		go datacite.AddModels(modelChan, collection, countChan)
	}
	go datacite.LogCount(count, countChan)
	sem := semaphore.NewWeighted(parallelDatasets)
	fileSem := semaphore.NewWeighted(parallelFiles)
	for _, fileInfo := range files {
		if err = fileSem.Acquire(context.Background(), 1); err != nil {
			println("Could not acquire file semaphore.", err.Error())
		}
		datacite.ReadFile(dataDir+fileInfo.Name(), sem, modelChan, fileSem)
	}
	println(count.Upserted, count.Modified)
}
