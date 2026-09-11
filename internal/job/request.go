package job

type CreateJobRequest struct {
    Type           string `json:"type"`
    Payload        any    `json:"payload"`
    IdempotencyKey string `json:"idempotencyKey"`
}