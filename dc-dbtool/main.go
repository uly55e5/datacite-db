package main

import (
	"bufio"
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"io/fs"
	"io/ioutil"
	"os"
	"time"
)

const (
	dbConnStr = "mongodb://datacite:datacite@localhost:27017"
	dataDir   = "/home/uly55e5/Projekte/ipb/dcdump/temp-20210917/"
)

func main() {

	var (
		count        int64 = 0
		lastcount    int64 = 0
		modified     int64 = 0
		lastmodified int64 = 0
		client       *mongo.Client
		err          error
		files        []fs.FileInfo
		collection   *mongo.Collection
		models       []mongo.WriteModel = nil
	)
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

	collection = client.Database("datacite-go").Collection("datacite")
	indexName, err := collection.Indexes().CreateOne(
		context.TODO(),
		mongo.IndexModel{
			Keys:    bson.D{{Key: "id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	)
	if err != nil {
		println("Error while creating index:", err.Error())
	}
	println("Index:", indexName)

	if files, err = ioutil.ReadDir(dataDir); err != nil {
		println("Read error:", err.Error())
		return
	}

	count, err = collection.CountDocuments(context.TODO(), bson.D{}, nil)

	for _, fileinfo := range files {
		var file *os.File
		file, err = os.Open(dataDir + fileinfo.Name())
		if err != nil {
			print("File open error:", err.Error())
			continue
		}
		lineReader := bufio.NewScanner(file)
		const maxLineLength = 2000000
		buf := make([]byte, maxLineLength)
		lineReader.Buffer(buf, maxLineLength)
		for lineReader.Scan() {
			line := lineReader.Bytes()
			if len(line) < 1 {
				continue
			}
			var v map[string]interface{}
			if err = json.Unmarshal(line, &v); err != nil {
				println(err.Error())
				continue
			}
			if data, ok := v["data"]; ok {
				dataarray := data.([]interface{})
				if len(dataarray) > 0 {
					for _, dataset := range dataarray {
						id := dataset.(map[string]interface{})["id"]
						updated := dataset.(map[string]interface{})["attributes"].(map[string]interface{})["updated"]
						var filter = bson.D{{"id", id}, {"attributes.updated", bson.M{"$lt": updated}}}
						model := mongo.NewReplaceOneModel().SetUpsert(true).SetReplacement(dataset).SetFilter(filter)
						models = append(models, model)
					}
					if len(models) > 10000 {
						var bulkErr mongo.BulkWriteException
						var res *mongo.BulkWriteResult
						inctx, incancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer incancel()
						inOptions := options.BulkWrite().SetOrdered(false)
						if res, err = collection.BulkWrite(inctx, models, inOptions); err != nil {
							print("*")
							if bulkErr, ok = err.(mongo.BulkWriteException); ok {
								for _, berr := range bulkErr.WriteErrors {
									go func(berr mongo.BulkWriteError) {
										if berr.Code != 11000 {
											println(berr.Message)
										}
									}(berr)
								}
							} else {
								println(err.Error())
							}
						} else {
							print(".")
							modified += res.ModifiedCount
							count += res.UpsertedCount
						}
						models = nil
					}
				}
				if count-lastcount > 100000 || modified-lastmodified > 100000 {
					println(count, modified)
					lastcount = count
					lastmodified = modified
				}
			}

		}
		file.Close()
	}
	println(count, modified)
}
