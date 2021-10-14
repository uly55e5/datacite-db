package datacite

import (
	"bufio"
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"golang.org/x/sync/semaphore"
	"os"
	"sync"
	"time"
)

type Count struct {
	Upserted int64
	Modified int64
	Read     int64
}

func SendReplaceModel(dataset interface{}, modelChan chan *mongo.ReplaceOneModel, sem *semaphore.Weighted) {
	id := dataset.(map[string]interface{})["id"]
	updated := dataset.(map[string]interface{})["attributes"].(map[string]interface{})["updated"]
	var filter = bson.D{{"id", id}, {"attributes.updated", bson.M{"$lt": updated}}}
	model := mongo.NewReplaceOneModel().SetUpsert(true).SetReplacement(dataset).SetFilter(filter)
	modelChan <- model
	sem.Release(1)
}

func AddDocuments(cData chan *mongo.ReplaceOneModel, collection *mongo.Collection, cCount chan Count, cDone chan bool, wg *sync.WaitGroup) {
	var (
		models = make([]mongo.WriteModel, 10000, 10000)
		err    error
		ok     bool
		i      int64 = 0
		done         = false
	)
	for {
		select {
		case models[i] = <-cData:
			i++
		case done = <-cDone:
		}

		if i == 10000 || (done && i > 0) {
			var bulkErr mongo.BulkWriteException
			var res *mongo.BulkWriteResult
			inCtx, inCancel := context.WithTimeout(context.Background(), 600*time.Second)
			inOptions := options.BulkWrite().SetOrdered(false)
			if res, err = collection.BulkWrite(inCtx, models[:i], inOptions); err != nil {
				print("*")
				if bulkErr, ok = err.(mongo.BulkWriteException); ok {
					for _, writeError := range bulkErr.WriteErrors {
						go func(writeError mongo.BulkWriteError) {
							if writeError.Code != 11000 {
								println("Write error:", writeError.Message)
							}
						}(writeError)
					}
				} else {
					println("Bulkwrite error:", err.Error())
				}
			} else {
				print(".")
			}
			if res != nil {
				cCount <- Count{res.UpsertedCount, res.ModifiedCount, i}
			} else {
				cCount <- Count{0, 0, i}
			}
			i = 0
			inCancel()
		}
		if done {
			println("Processing done...")
			wg.Done()
			return
		}
	}
}

func LogCount(cCount chan Count, cDone chan bool) {
	var lastCount = Count{0, 0, 0}
	var count = Count{0, 0, 0}
	var newCount Count
	var done = false
	for {
		select {
		case newCount = <-cCount:
		case done = <-cDone:
		}

		if done {
			println("Done:", count.Upserted, count.Modified, count.Read)
			return
		}
		count.Upserted += newCount.Upserted
		count.Modified += newCount.Modified
		count.Read += newCount.Read
		if count.Upserted-lastCount.Upserted >= 100000 || count.Modified-lastCount.Modified >= 100000 || count.Read-lastCount.Read >= 1000000 {
			println(" U:", count.Upserted, "M:", count.Modified, "R:", count.Read)
			lastCount.Upserted = count.Upserted
			lastCount.Modified = count.Modified
			lastCount.Read = count.Read
		}
	}
}

func ReadLine(lineReader *bufio.Scanner, sem *semaphore.Weighted, modelChan chan *mongo.ReplaceOneModel) {
	var err error
	line := lineReader.Bytes()
	if len(line) < 1 {
		return
	}
	var v map[string]interface{}
	if err = json.Unmarshal(line, &v); err != nil {
		println(err.Error())
		return
	}
	if data, ok := v["data"]; ok {
		dataArray := data.([]interface{})
		for _, dataset := range dataArray {
			if err = sem.Acquire(context.Background(), 1); err != nil {
				println("Could not acquire semaphore", err.Error())
			}
			go SendReplaceModel(dataset, modelChan, sem)
		}
	}
	return
}

func ReadFile(fileName string, sem *semaphore.Weighted, modelChan chan *mongo.ReplaceOneModel, fileSem *semaphore.Weighted) {
	var file *os.File
	var err error
	if file, err = os.Open(fileName); err != nil {
		print("File open error:", err.Error())
		return
	}
	lineReader := bufio.NewScanner(file)
	const maxLineLength = 2000000
	buf := make([]byte, maxLineLength)
	lineReader.Buffer(buf, maxLineLength)
	for lineReader.Scan() {
		ReadLine(lineReader, sem, modelChan)
	}
	if err = file.Close(); err != nil {
		println("Could not close file ", fileName, err.Error())
	}
	fileSem.Release(1)
	return
}

func CreateIndexes(collection *mongo.Collection) {
	var err error
	var idModels = []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bson.D{{"attributes.updated", 1}},
			Options: options.Index(),
		},
		{
			Keys:    bson.D{{"$**", "text"}},
			Options: options.Index().SetLanguageOverride("dummy"),
		},
		{
			Keys:    bson.D{{"attributes.types.citeproc", 1}},
			Options: options.Index(),
		},
	}
	var names []string
	if names, err = collection.Indexes().CreateMany(context.Background(), idModels); err != nil {
		println("Error while creating index:", err.Error())
	}
	for _, name := range names {
		println("Index:", name)
	}
}

func ConnectDatabase(dbConnPtr *string, dbNamePtr *string) (*mongo.Collection, error) {
	var err error
	var client *mongo.Client
	var collection *mongo.Collection

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(*dbConnPtr)); err != nil {
		println(err.Error())
		return nil, err
	}

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		println(err.Error())
		return nil, err
	}
	collection = client.Database(*dbNamePtr).Collection("datacite")
	CreateIndexes(collection)
	var count int64
	if count, err = collection.EstimatedDocumentCount(context.Background(), options.EstimatedDocumentCount().SetMaxTime(10*time.Second)); err != nil {
		println("Error while Counting:", err.Error())
	}
	println("Documents in database:", count)
	return collection, nil
}
