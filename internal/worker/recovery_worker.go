package worker

import (
	"context"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/repository"
	"errors"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
)


type RecoveryWorker struct {
	repository *repository.JobRepository
	queue	*queue.RedisQueue
}

func NewRecoveryWorker(
	repository 	*repository.JobRepository,
	queue	*queue.RedisQueue,
) *RecoveryWorker {
	return &RecoveryWorker{
		repository: repository,
		queue: queue,
	}
}

func (r *RecoveryWorker) Start(ctx context.Context) {

	ticker	:= time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("Recovery worker started")

	for {
		select {
		
		case <-ctx.Done():
			log.Println("Recovery worker shutting down")
			return
		case <-ticker.C:

			jobs, err := r.repository.GetExpiredJobs(ctx)

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				log.Printf("Recovery worker: failed to find expired jobs: %v",err)
				continue
			}

			for _, job := range jobs {

				err := r.repository.RecoverJob(
					ctx,
					job.ID,
				)

				if err != nil {
					if errors.Is(
						err, mongo.ErrNoDocuments,
					) {
						continue
					}

					log.Printf("Recovery worker: failed to recover job %s: %v",job.ID,err,)

					continue
				}

				if err := r.queue.Enqueue(ctx,job.ID); err != nil {
					log.Printf("Recovery worker: failed to enqueue recovered job %s: %v",job.ID,err,)
					continue
				}

				log.Printf("Recovery worker: recovered job %s", job.ID,)
			}
		}
	}
}