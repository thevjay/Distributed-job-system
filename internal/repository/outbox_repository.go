package repository

import (
	"context"
	"distributed-job-system/internal/model"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

type OutboxRepository struct {
	collection *mongo.Collection
}

func NewOutboxRepository(
	db *mongo.Database,
) *OutboxRepository {
	
	return &OutboxRepository{
		collection: db.Collection("outbox_events"),
	}
}


func (r *OutboxRepository) Create(
	ctx	context.Context,
	event *model.OutboxEvent,
) error {

	_, err := r.collection.InsertOne(ctx, event)
	return err
}