package repository

import (
	"context"
	"time"

	"distributed-job-system/internal/model"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type JobRepository struct {
	collection *mongo.Collection
	
}

func NewJobRepository(db *mongo.Database) *JobRepository {
	return &JobRepository{
		collection: db.Collection("jobs"),
	}
}

func (r *JobRepository) Create(
	ctx context.Context,
	job *model.Job,
) error {
	_, err := r.collection.InsertOne(ctx, job)

	return err
}

func (r *JobRepository) GetAll(ctx context.Context) ([]*model.Job,error) {

	cursor, err := r.collection.Find(ctx, bson.M{})
	// bson.M{} is mandatory because MongoDB's driver expects a filter argument. 
	// If you don't want to filter, you pass an empty one.
	
	if err != nil {
		return nil, err
	}

	defer cursor.Close(ctx)
	var jobs []*model.Job
	for cursor.Next(ctx) {
		var job model.Job
		if err := cursor.Decode(&job); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (r *JobRepository) GetByID(ctx context.Context, id string) (*model.Job, error) {

	var job model.Job
	
	err := r.collection.FindOne(
		ctx,
		bson.M{"_id": id},
	).Decode(&job)

	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (r *JobRepository) UpdateStatus(
	ctx context.Context,
	id string,
	status model.JobStatus,
) error {
	
	_, err := r.collection.UpdateOne(
		ctx,
		bson.M{"_id":id},
		bson.M{
			"$set": bson.M{
				"status": status,
				"updatedAt": time.Now(),
			},
		},
	)

	return err
}

func (r *JobRepository) IncrementAttempts(
	ctx context.Context,
	id string,
) (int,error) {

	result,err := r.collection.UpdateOne(
		ctx,
		bson.M{"_id":id},
		bson.M{
			"$inc": bson.M{
				"attempts":1,
			},
			"$set": bson.M{
				"updatedAt": time.Now(),
			},
		},
	)

	if err != nil {
		return 0, err
	}

	if result.MatchedCount == 0 {
		return 0, mongo.ErrNoDocuments
	}

	// We need the updated document to know the new attempt number.
	var job model.Job

	err = r.collection.FindOne(
		ctx,
		bson.M{"_id": id},
	).Decode(&job)

	if err != nil {
		return 0, err
	}
	
	return job.Attempts, nil
}

func (r *JobRepository) GetByIdempotencyKey(ctx context.Context,key string,) (*model.Job, error) {
	var job model.Job

	err := r.collection.FindOne(
		ctx,
		bson.M{
			"idempotencyKey": key,
		},
	).Decode(&job)

	if err != nil {
		return nil, err
	}

	return &job,nil
}

func (r *JobRepository) ClaimJob(
	ctx context.Context,
	id string,
) ( *model.Job, error) {

	now := time.Now();
	leaseUntil := now.Add(10 * time.Second)

	filter := bson.M{
		"_id": id,
		"$or": bson.A{
			bson.M{
				"status": model.StatusQueued,
			},
			bson.M{
				"status": model.StatusProcessing,
				"leaseUntil": bson.M{
					"$lt": now,
				},
			},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"status":     model.StatusProcessing,
			"leaseUntil": leaseUntil,
			"updatedAt":  now,
		},
	}

	var job model.Job

	err := r.collection.FindOneAndUpdate(
		ctx,
		filter,
		// bson.M{
		// 	"_id": id,
		// 	"status": model.StatusQueued,
		// },
		// bson.M{
		// 	"$set": bson.M{
		// 		"status": model.StatusProcessing,
		// 		"updatedAt": time.Now(),
		// 	},
		//		},   // Only claim this job if it is currently queued

		update,
	).Decode(&job)

	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (r *JobRepository) RenewLease(
	ctx context.Context,
	id string,
) error {

	now := time.Now()
	leaseUntil := now.Add(10* time.Second)

	result, err := r.collection.UpdateOne(
		ctx,
		bson.M{
			"_id": id,
			"status": model.StatusProcessing,
		},
		bson.M{
			"$set": bson.M{
				"leaseUntil": leaseUntil,
				"updatedAt": now,
			},
		},
	)

	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}

func (r *JobRepository) CompleteJob(
	ctx context.Context,
	id	string,
) error {

	result, err := r.collection.UpdateOne(
		ctx,
		bson.M{
			"_id": id,
			"status":  model.StatusProcessing,
		},
		bson.M{
			"$set": bson.M{
				"status": model.StatusCompleted,
				"leaseUntil": nil,
				"updatedAt": time.Now(),
			},
		},
	)

	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}

func (r *JobRepository) FailJob(
	ctx context.Context,
	id	string,
) error {

	result, err := r.collection.UpdateOne(
		ctx,
		bson.M{
			"_id": id,
			"status": model.StatusProcessing,
		},
		bson.M{
			"$set": bson.M{
				"status": model.StatusFailed,
				"leaseUntil": nil,
				"updatedAt": time.Now(),
			},
		},
	)

	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}