package datacite

import (
	"bufio"
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/semaphore"
	"os"
	"sync"
	"time"
)

type Count struct {
	Upserted int64
	Modified int64
}

func SendReplaceModel(dataset interface{}, modelChan chan *mongo.ReplaceOneModel, sem *semaphore.Weighted) {
	id := dataset.(map[string]interface{})["id"]
	updated := dataset.(map[string]interface{})["attributes"].(map[string]interface{})["updated"]
	var filter = bson.D{{"id", id}, {"attributes.updated", bson.M{"$lt": updated}}}
	model := mongo.NewReplaceOneModel().SetUpsert(true).SetReplacement(dataset).SetFilter(filter)
	modelChan <- model
	sem.Release(1)
}

func AddModels(m chan *mongo.ReplaceOneModel, collection *mongo.Collection, cCount chan Count, cDone chan bool, wg *sync.WaitGroup) {
	var (
		models = make([]mongo.WriteModel, 10000, 10000)
		err    error
		ok     bool
		i      = 0
		done   = false
	)
	for {
		select {
		case models[i] = <-m:
			i++
		case done = <-cDone:
		}

		if i == 10000 || done {
			var bulkErr mongo.BulkWriteException
			var res *mongo.BulkWriteResult
			inCtx, inCancel := context.WithTimeout(context.Background(), 120*time.Second)
			inOptions := options.BulkWrite().SetOrdered(false)
			if res, err = collection.BulkWrite(inCtx, models, inOptions); err != nil {
				print("*")
				if bulkErr, ok = err.(mongo.BulkWriteException); ok {
					for _, writeError := range bulkErr.WriteErrors {
						go func(writeError mongo.BulkWriteError) {
							if writeError.Code != 11000 {
								println(writeError.Message)
							}
						}(writeError)
					}
				} else {
					println(err.Error())
				}
			} else {
				print(".")
			}
			if res != nil {
				cCount <- Count{res.UpsertedCount, res.ModifiedCount}
			}
			i = 0
			inCancel()
		}
		if done {
			wg.Done()
			return
		}
	}
}

func LogCount(count Count, cCount chan Count) {
	var lastCount int64 = 0
	var lastModified int64 = 0
	println(count.Upserted, count.Modified)
	for {
		newCount := <-cCount
		count.Upserted += newCount.Upserted
		count.Modified += newCount.Modified
		if count.Upserted-lastCount > 100000 || count.Modified-lastModified > 100000 {
			println(count.Upserted, count.Modified)
			lastCount = count.Upserted
			lastModified = count.Modified
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

func GetIndexes() []mongo.IndexModel {
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
	return idModels
}
