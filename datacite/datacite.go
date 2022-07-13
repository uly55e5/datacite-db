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
	"io"
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

func AddDocuments(cData chan *mongo.ReplaceOneModel, collection *mongo.Collection, cCount chan Count, cDone chan bool, wg *sync.WaitGroup, dataSetBufferSize int) {
	var (
		models = make([]mongo.WriteModel, dataSetBufferSize, dataSetBufferSize)
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

		if i == int64(dataSetBufferSize) || (done && i > 0) {
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

func ReadLine(lineReader *bufio.Reader, sem *semaphore.Weighted, modelChan chan *mongo.ReplaceOneModel) error {
	var line, ln []byte
	var isPrefix = true
	var err error
	for isPrefix && err == nil {
		ln, isPrefix, err = lineReader.ReadLine()
		line = append(line, ln...)
	}
	if err != nil {
		return err
	}
	if len(line) < 1 {
		return nil
	}
	var v map[string]interface{}
	if err = json.Unmarshal(line, &v); err != nil {
		println(err.Error())
		return nil
	}
	if data, ok := v["data"]; ok {
		dataArray := data.([]interface{})
		for _, dataset := range dataArray {
			if err = sem.Acquire(context.Background(), 1); err != nil {
				println("Could not acquire semaphore", err.Error())
			}
			go SendReplaceModel(dataset, modelChan, sem)
		}
	} else if _, ok := v["id"]; ok {
		if err = sem.Acquire(context.Background(), 1); err != nil {
			println("Could not acquire semaphore", err.Error())
		}
		go SendReplaceModel(v, modelChan, sem)
	}
	return nil
}

func ReadFile(fileName string, sem *semaphore.Weighted, modelChan chan *mongo.ReplaceOneModel, fileSem *semaphore.Weighted) {
	var file *os.File
	var err error
	if file, err = os.Open(fileName); err != nil {
		print("File open error:", err.Error())
		return
	}
	lineReader := bufio.NewReaderSize(file, 8000)
	for {
		err = ReadLine(lineReader, sem, modelChan)
		if err == io.EOF {
			break
		}
		if err != nil {
			print(err.Error())
		}
	}
	if err = file.Close(); err != nil {
		println("Could not close file ", fileName, err.Error())
	}
	fileSem.Release(1)
	return
}

func CreateIndexes(collection *mongo.Collection) {
	var err error
	var stdIndexFields = []string{
		"attributes.relatedIdentifiers.relationType",
		"attributes.updated",
		"attributes.created",
		"attributes.types.citeproc",
		"attributes.types.bibtex",
		"attributes.types.ris",
		"attributes.types.schemaOrg",
		"attributes.types.resourceType",
		"attributes.types.resourceTypeGeneral",
		"attributes.relatedIdentifiers.relatedIdentifierType",
		"attributes.subjects.subject",
		"attributes.subjects.subjectScheme",
		"attributes.citationCount",
		"attributes.rightsList.rights",
		"attributes.contributors.name",
		"attributes.contributors.affiliation",
		"attributes.contributors.contributorType",
		"attributes.creators.name",
		"attributes.creators.affiliation",
		"attributes.formats",
		"attributes.identifiers.identifier",
		"attributes.identifiers.identifierType",
		"attributes.publicationYear",
		"attributes.publisher",
		"attributes.published",
		"attributes.referenceCount",
		"relationships.client.data.type",
		"relationships.client.data.id",
		"attributes.relatedIdentifiers.resourceTypeGeneral",
	}
	var name string
	var model mongo.IndexModel
	var idModels []mongo.IndexModel
	for _, name = range stdIndexFields {
		model = mongo.IndexModel{
			Keys:    bson.D{{name, 1}},
			Options: options.Index(),
		}
		idModels = append(idModels, model)
	}
	idModels = append(idModels, mongo.IndexModel{
		Keys:    bson.D{{Key: "id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})

	/*idModels = append(idModels, mongo.IndexModel{
		Keys:    bson.D{{"$**", "text"}},
		Options: options.Index().SetLanguageOverride("dummy"),
	})*/
	println("Creating Indexes...")
	var names []string
	if names, err = collection.Indexes().CreateMany(context.Background(), idModels); err != nil {
		println("Error while creating index:", err.Error())
	}
	for _, name := range names {
		println("Index:", name)
	}
}

func ConnectDataCiteCollection(dbConnPtr *string, dbNamePtr *string) (*mongo.Collection, error) {
	var err error
	var client *mongo.Client
	var collection *mongo.Collection

	client, err = ConnectDatabase(dbConnPtr)
	if err != nil {
		return nil, err
	}
	collection = client.Database(*dbNamePtr).Collection("datacite")
	var count int64
	if count, err = collection.EstimatedDocumentCount(context.Background(), options.EstimatedDocumentCount().SetMaxTime(10*time.Second)); err != nil {
		println("Error while Counting:", err.Error())
	}
	println("Documents in database:", count)
	CreateIndexes(collection)
	return collection, nil
}

func ConnectDatabase(dbConnPtr *string) (*mongo.Client, error) {
	var client *mongo.Client
	var err error
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
	return client, err
}
