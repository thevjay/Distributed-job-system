package queue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const JobQueue = "job_queue"

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue() *RedisQueue {

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	return &RedisQueue{
		client: client,
	}
}

func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}

func (q *RedisQueue) Enqueue(ctx context.Context, jobID string,) error {
	return q.client.LPush(
		ctx,
		JobQueue,
		jobID,
	).Err()
}

func (q *RedisQueue) Dequeue(ctx context.Context,) (string, error) {

	result, err := q.client.BRPop(
		ctx,
		1*time.Second,
		JobQueue,
	).Result()

	// BRPop -means: Blocking Right Pop

/**
			Worker
			  ↓
			BRPop
			  ↓
			Waiting...
			  ↓
			New job arrives
			  ↓
			Redis wakes worker
			  ↓
			Worker gets job

			That's why BRPop is useful for a basic queue implementation.
*/

	if err != nil {
		return "", err
	}

	return result[1], nil
}