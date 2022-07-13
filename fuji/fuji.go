package fuji

import (
	"context"
	"datacite-db/datacite"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/rand"
)

var database *mongo.Database

func ConnectToDatabase(dbConnPtr *string, dbNamePtr *string) error {
	var client *mongo.Client
	var err error
	client, err = datacite.ConnectDatabase(dbConnPtr)
	if err != nil {
		return err
	}
	database = client.Database(*dbNamePtr)
	return nil
}

func GetRandomDatasetDoi() (string, error) {
	if database == nil {
		return "", errors.New("database not connected")
	}
	var err error
	prefixCollection := database.Collection("doi_prefix")
	ctx := context.TODO()
	nPrefix, err := prefixCollection.CountDocuments(ctx, bson.D{{"ds_count", bson.D{{"$gt", 0}}}})
	if err != nil {
		return "", err
	}
	prefixNumber := rand.Int63n(nPrefix)
	prefixResult := prefixCollection.FindOne(ctx, bson.D{{"ds_count", bson.D{{"$gt", 0}}}}, options.FindOne().SetSkip(prefixNumber))
	var prefixData bson.M
	err = prefixResult.Decode(&prefixData)
	if err != nil {
		return "", err
	}
	prefix := fmt.Sprintf("%v", prefixData["_id"])
	dataciteCount := int64(prefixData["ds_count"].(int32))

	dataciteCollection := database.Collection("datacite")
	dataciteNumber := rand.Int63n(dataciteCount)
	dataciteResult := dataciteCollection.FindOne(ctx, bson.D{{"id", bson.D{{"$regex", "^" + prefix}}}, {"attributes.types.resourceTypeGeneral", "Dataset"}}, options.FindOne().SetHint(bson.D{{"id", 1}}).SetSkip(dataciteNumber).SetProjection(bson.D{{"id", 1}}))

	var dataciteMap bson.M
	err = dataciteResult.Decode(&dataciteMap)
	if err != nil {
		return "", err
	}
	doi := fmt.Sprintf("%v", dataciteMap["id"])
	return doi, nil
}

func InsertFujiDataset(responseBody map[string]interface{}, doi string) error {
	if database == nil {
		return errors.New("database not connected")
	}
	var ctx = context.TODO()
	var err error
	resultCollection := database.Collection("fuji-result")
	responseBody["_id"] = doi
	_, err = resultCollection.InsertOne(ctx, responseBody)
	return err
}

func FujiExists(doi *string) bool {
	if database == nil {
		println("Database not connected")
		return false
	}
	count, _ := database.Collection("fuji-result").CountDocuments(context.Background(), bson.D{{"_id", doi}})
	if count > 0 {
		return true
	}
	return false
}
