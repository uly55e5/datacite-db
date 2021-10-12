package main

import (
	"bufio"
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"io/ioutil"
	"os"
	"time"
)

func main() {
	var count int64 = 0
	var lastcount int64 = 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://datacite:datacite@10.22.13.12:27017"))
	if err != nil {
		println(err.Error())
		return
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			println(err.Error())
			panic(err)
		}
	}()

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		println(err.Error())
		return
	}

	collection := client.Database("datacite-go").Collection("datacite")
	indexName, err := collection.Indexes().CreateOne(
		context.TODO(),
		mongo.IndexModel{
			Keys:    bson.D{{Key: "id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	)
	if err != nil {
		println(err.Error())
	}
	println(indexName)
	const datadir = "/home/david/Projekte/datacite-data/temp-20210917/"
	files, err := ioutil.ReadDir(datadir)
	if err != nil {

	}
	count,err = collection.CountDocuments(context.TODO(),nil,nil)

	for _, fileinfo := range files {
		file, err := os.Open(datadir + fileinfo.Name())
		if err != nil {
			print(err.Error())
			file.Close()
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
			var res *mongo.InsertManyResult
			if data, ok := v["data"]; ok {
				dataarray := data.([]interface{})
				if len(dataarray) > 0 {
					inctx, incancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer incancel()
					var ordered bool = false
					var inOptions = options.InsertManyOptions{Ordered: &ordered}
					if res, err = collection.InsertMany(inctx, dataarray, &inOptions); err != nil {
						println(err.Error())
						continue
					}
					count += int64(len(res.InsertedIDs))
					if(count-lastcount > 100000) {
						println(count)
						lastcount = count
					}

				}
			}
		}
		file.Close()
	}
}
