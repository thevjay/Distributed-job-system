package model

import "time"

type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	ID             string     `bson:"_id,omitempty" json:"id"`
	Type           string     `bson:"type" json:"type"`
	Payload        any        `bson:"payload" json:"payload"`
	IdempotencyKey string     `bson:"idempotencyKey" json:"idempotencyKey"`

	Status         JobStatus  `bson:"status" json:"status"`
	Attempts       int        `bson:"attempts" json:"attempts"`

	LastError string `bson:"lastError,omitempty" json:"lastError,omitempty"`
	
	LeaseUntil     *time.Time `bson:"leaseUntil,omitempty" json:"leaseUntil,omitempty"`

	CreatedAt      time.Time  `bson:"createdAt" json:"createdAt"`
	UpdatedAt      time.Time  `bson:"updatedAt" json:"updatedAt"`
}