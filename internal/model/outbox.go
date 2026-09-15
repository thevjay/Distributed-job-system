package model

import "time"

type OutboxStatus string

const (
	OutboxPending	OutboxStatus = "pending"
	OutboxPublished	OutboxStatus = "published"
)

type OutboxEvent struct {
	ID        string       `bson:"_id,omitempty" json:"id"`
	Type      string       `bson:"type" json:"type"`
	JobID     string       `bson:"jobId" json:"jobId"`
	Status    OutboxStatus `bson:"status" json:"status"`
	CreatedAt time.Time    `bson:"createdAt" json:"createdAt"`
	PublishedAt *time.Time `bson:"publishedAt,omitempty" json:"publishedAt,omitempty"`
}


