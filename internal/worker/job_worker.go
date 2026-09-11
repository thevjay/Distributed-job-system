package worker

import (
	"context"
	"distributed-job-system/internal/model"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/repository"
	"errors"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type JobWorker struct {
	id		int
	queue	*queue.RedisQueue
	repository	*repository.JobRepository
}

func NewJobWorker(
	id	int,
	queue	*queue.RedisQueue,
	repository *repository.JobRepository,
) *JobWorker {
	return &JobWorker{
		id:		id,
		queue: queue,
		repository: repository,
	}
}

func (w *JobWorker) Start(ctx context.Context){
	const (
		MaxAttempts = 3
		BaseDelay   = 1 * time.Second
	)

	log.Printf("Worker %d started", w.id)

	for {
		// Check whether application is shutting down.
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down", w.id)
			return

		default:
		}

		// -------------------------------------------------------
		// Wait for a job from Redis
		// -------------------------------------------------------

		jobID, err := w.queue.Dequeue(ctx)

		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Worker %d context cancelled", w.id)
				return
			}

			if errors.Is(err,redis.Nil) {
				// Redis timeout - no job available.
				// Loop again and check context.
				continue
			}
			log.Printf(
				"Worker %d failed to dequeue job: %v",
				w.id, 
				err,
			)

			continue
		}

		// IMPORTANT:
		// Context may have been cancelled around the same time
		// Redis returned a job.
		if ctx.Err() != nil {
			log.Printf("Worker %d received shutdown signal, skipping job %s",w.id,jobID)
			return
		}

		log.Printf(
			"Worker %d received job %s", 
			w.id,
			jobID,
		)

		// -------------------------------------------------------
		// Get Job
		// -------------------------------------------------------

		job, err := w.repository.ClaimJob(ctx, jobID)

		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Worker %d context cancelled", w.id)
				return
			}

			if errors.Is(err, mongo.ErrNoDocuments) {

				log.Printf(
					"Worker %d: job %s already claimed or no longer queued",
					w.id,
					jobID,
				)
				continue
			}

			log.Printf("Worker %d: failed to get job %s: %v", w.id,jobID,err)
			continue
		}
		// -------------------------------------------------------
		// Increment Attempt		
		// -------------------------------------------------------
		attempt, err := w.repository.IncrementAttempts(ctx, jobID)

		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Worker %d context cancelled", w.id)
				return
			}

			log.Printf("Worker %d: failed to increment attempts for job %s: %v",w.id,jobID,err)
			continue
		}
		log.Printf(
			"Worker %d: processing job %s, attempt %d",
			w.id,
			jobID,
			attempt,
		)
		// -------------------------------------------------------
		// Mark Processing	
		// -------------------------------------------------------
		if err := w.repository.UpdateStatus(
			ctx,
			jobID,
			model.StatusProcessing,
		); err != nil {

			if ctx.Err() != nil {
				log.Printf("Worker %d context cancelled",w.id)
				return
			}

			log.Printf(
				"Worker %d failed to update job %s: %v",
				w.id,
				jobID,
				err,
			)
			continue
		}

		log.Printf(
			"Worker %d processing job %s",
			w.id,
			jobID,
		)
		
		// -------------------------------------------------------
		// Simulate Job Processing		
		// -------------------------------------------------------

		// select {
		// case <-time.After(3 * time.Second):
		// 	//  Processing finished.

		// case <-ctx.Done():
		// 	log.Printf("Worker %d: cancellation received while processing job %s", w.id,jobID,)
		// 	return
		// }

		// -------------------------------------------------------
		// Simulate Failure
		// -------------------------------------------------------

		if job.Type == "fail" {

			log.Printf(
				"worker %d: job %s failed on attempt %d",
				w.id,
				jobID,
				attempt,
			)

			// -------------------------------------------------------
			// Retry
			// -------------------------------------------------------

			if attempt < MaxAttempts {

				delay := BaseDelay * time.Duration(1<<(attempt-1))

				log.Printf(
					"Worker %d: retrying job %s after %v",
					w.id,
					jobID,
					delay,
				)

				// Context-aware retry delay.

				select {
				case <-time.After(delay):
					// Retry delay completed
				
				case <-ctx.Done():
					log.Printf("Worker %d: cancellation received during retry delay", w.id,)
					return
				}

				// Put the job back into Redis.
				err := w.queue.Enqueue(ctx,jobID)
				if err != nil {
					if ctx.Err() != nil {
						log.Printf("Worker %d context cancelled",w.id)
						return
					}

					log.Printf(
						"Worker %d: failed to enqueue retry for job %s: %v",
						w.id,
						jobID,
						err,
					)
				}

				continue
			}

			// -------------------------------------------------------
			// Permanently Failed
			// -------------------------------------------------------

			log.Printf(
				"Worker %d: job %s permanently failed",
				w.id,
				jobID,
			)

			if err := w.repository.UpdateStatus(
				ctx,
				jobID,
				model.StatusFailed,
			); err != nil {
				log.Printf("Worker %d: failed to mark job %s as failed: %v",w.id,jobID,err)
			}

			continue
		}

		// -------------------------------------------------------
		// Successful Job
		// -------------------------------------------------------

		if err := w.repository.UpdateStatus(
			ctx,
			jobID,
			model.StatusCompleted,
		); err != nil {

			if ctx.Err() != nil {
				log.Printf("Worker %d context cancelled",w.id)
				return
			}

			log.Printf("Worker %d: failed to mark job %s completed: %v",w.id,jobID,err)
			continue
		}

		log.Printf("Worker %d completed job %s",w.id,jobID)
	}
}


func (w *JobWorker) processJob(
	ctx context.Context,
	jobID string,
) error {

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	processing := time.NewTimer(30 * time.Second)
	defer processing.Stop()

	for {
		select {
		case <-processing.C:
			return nil

		case <-ticker.C:
			if err := w.repository.RenewLease(
				ctx,
				jobID,
			); err != nil {
				return err
			}

			log.Printf("Worker: renewed lease for job %s", jobID,)

			case <-ctx.Done():
				return ctx.Err()
		}

	}
}
