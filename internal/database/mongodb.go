package database

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func ConnectMongodb(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(
		options.Client().ApplyURI(uri),
	)

	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, err
	}

	return client, nil
}

func CreateJobIndexes(
	ctx context.Context,
	db *mongo.Database,
) error {
	collection := db.Collection("jobs")

	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "idempotencyKey", Value: 1},
		},
		Options: options.Index().
			SetUnique(true).
			SetPartialFilterExpression(
				bson.M{
					"idempotencyKey": bson.M{
						"$exists": true,
					},
				},
			),	
	}

	_, err := collection.Indexes().CreateOne(
		ctx,
		indexModel,
	)
	
	return err
}