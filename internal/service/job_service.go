package service

import (
	"context"
	"distributed-job-system/internal/job"
	"distributed-job-system/internal/model"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/repository"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type JobService struct {
	repository *repository.JobRepository
	queue	   *queue.RedisQueue
}

func NewJobService(
	repository *repository.JobRepository,
	queue	*queue.RedisQueue,
) *JobService {

	return &JobService{
		repository: repository,
		queue:	queue,
	}
}

func (s *JobService) CreateJob(
	ctx context.Context,
	req *job.CreateJobRequest,
) (*model.Job, bool,error) {

	// 1.Check whether this request was already processed
	existingJob, err := s.repository.GetByIdempotencyKey(
		ctx,
		req.IdempotencyKey,
	)

	if err == nil {
		return existingJob, false,nil
	}

	if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, err
	}

	// 2. No existing job -> create a new Job
	now := time.Now()

	job := &model.Job{
		ID:        uuid.NewString(),
		Type:      req.Type,
		Payload:   req.Payload,
		IdempotencyKey: req.IdempotencyKey,
		Status:    model.StatusQueued,
		Attempts:  0,
		CreatedAt: now,
		UpdatedAt: now,
	}

	// 3. Save job in MongoDB
	if err := s.repository.Create(ctx, job); err != nil {

		// Another concurrent request created
		// The same idempotency key first.
		if mongo.IsDuplicateKeyError(err) {
			existingJob, findErr := s.repository.GetByIdempotencyKey(
				ctx,
				req.IdempotencyKey,
			)

			if findErr != nil {
				return nil, false,findErr
			}
			
			return existingJob, false,nil
		}

		return nil, false,err
	}

	// 4. Push job to redis
	if err := s.queue.Enqueue(ctx, job.ID); err != nil {
		return nil, false,err
	}

	return job,true, nil
}

func (s *JobService) GetJobByID(ctx context.Context, id string,) (*model.Job, error) {
	return s.repository.GetByID(ctx, id)
}

func (s *JobService) GetAllJobs(ctx context.Context) ([]*model.Job, error) {
	return s.repository.GetAll(ctx)
}

func (s *JobService) ReplayJob(
	ctx context.Context,
	id 	string,
) (*model.Job, error) {

	job, err := s.repository.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if job.Status != model.StatusFailed {
		return nil, errors.New(
			"Only failed jobs can be replayed",
		)
	}

	if err := s.repository.ReplayJob(
		ctx,
		id,
	); err != nil {
		return nil, err
	}

	job, err = s.repository.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := s.queue.Enqueue(
		ctx,
		job.ID,
	); err != nil {
		return nil, err
	}

	return job, nil
}