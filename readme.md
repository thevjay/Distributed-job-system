go mod init distributed-job-system

go run ./cmd/server

go get go.mongodb.org/mongo-driver/v2/mongo

go get go.mongodb.org/mongo-driver/v2/bson

go get github.com/redis/go-redis/v9


Phase 1 → Go + Chi server
Phase 2 → MongoDB
Phase 3 → Job model + CRUD
Phase 4 → Redis queue
Phase 5 → Worker pool
Phase 6 → Goroutines + channels
Phase 7 → Retry + exponential backoff
Phase 8 → Cancellation
Phase 9 → Idempotency
Phase 10 → Graceful shutdown
Phase 11 → Docker
Phase 12 → Metrics / observability

distributed-job-system/
│
├── cmd/
│   └── server/
│       └── main.go
│
├── internal/
│   ├── handler/
│   ├── service/
│   ├── repository/
│   └── worker/
│
├── go.mod
└── go.sum

handler
   ↓
service
   ↓
repository
   ↓
MongoDB

Start a clean
1.architecture
2.build it in stages
3.module
4.Structure


# Step 10 - Graceful Shutdown + Context Cancellation.
   - Currently, if You press: Ctrl + c
   - We want this instead:
      Ctrl + C
         ↓
      Receive shutdown signal
         ↓
      Cancel application context
         ↓
      Workers stop
         ↓
      Wait for workers to finish/exit
         ↓
      Shutdown HTTP server
         ↓
      Close MongoDB
         ↓
      Close Redis
         ↓
      Application exits

      This is called graceful shutdown.

- We're shutting down. Don't start new work, and allow existing work to finish where appropriate.



                  Ctrl+C
                     ↓
                signalChan
                     ↓
                  cancel()
                     ↓
              context cancelled
                     ↓
       ┌─────────────┼─────────────┐
       ↓             ↓             ↓
   Worker 1      Worker 2      Worker 3
       ↓             ↓             ↓
    ctx.Done()    BRPOP exits    ctx.Done()
       ↓             ↓             ↓
      return        return        return
       └─────────────┼─────────────┘
                     ↓
                  wg.Wait()
                     ↓
              workers stopped
                     ↓
              HTTP Shutdown
                     ↓
             MongoDB disconnect
                     ↓
          Application shutdown


# 11 - Idempotency & Duplicate Job Protection:
- 1. Idempotency means:
  - Repeating the same request should produce the same logical result instead of creating duplicate work

 Therefore, we eventually need worker-side protection too.
 

# 11.6 - Worker-side Duplicate Protection
   We've solved:
     - Client sends the same job request multiple times
   Now we need to solve:
    - The same job gets delivered to workers multiple times
    
```
Redis
 │
 ├── Worker 1 → job-123
 │
 └── Worker 2 → job-123

Now 

Worker 1 → process job-123
Worker 2 → process job-123

That's dangerous

queued
   |
   | atomic operation
   |

 Processing

Worker 1                    Worker 2

Get job                     Get job
   ↓                           ↓
queued                       queued
   ↓                           ↓
Update processing           Update processing
   ↓                           ↓
    BOTH PROCESS ❌


The problem is the gap between:
READ
 ↓
CHECK
 ↓
UPDATE

  - MongoDB atomic update
    Find:
      _id = jobID
      AND status = queued

      Then: 
       status = processing  


db.jobs.findOneAndUpdate(
    {
        _id: "job-123",
        status: "queued"
    },
    {
        $set: {
            status: "processing"
        }
    }
)
```

# One important problem with our current Redis flow

- Currently Redis does:
  BRPOP
    ↓
  job removed from Redis
    ↓
  Claim MongoDB

- if the worker crashes after Redis removes the Job:
  Redis
    ↓
  job removed
    ↓
  Worker 💥

  MongoDB might contain:
   processing
   leaseUntil = future


# Architecture:
- We'll eventually have:

                 Redis
                   │
                   ▼
              Normal Worker
                   │
                   ▼
                Claim
                   │
             ┌─────┴─────┐
             │           │
          finishes     crashes
             │           │
             ▼           ▼
         completed   lease expires
                         │
                         ▼
                  Recovery Worker
                         │
                         ▼
                  Find expired jobs
                         │
                         ▼
                    Redis queue
                         │
                         ▼
                   Normal Worker

-> This gives us failure recovery.
# 
# Lease Renewal / Heartbeart:
 We now have:
 ```
    queued
      ↓
    ClaimJob()
      ↓
    processing
      ↓
    leaseUntil = now + 10 seconds
    
 ```
But there's a problem.
Suppose a job takes 30 seconds:
```
0 sec   → worker claims job
10 sec  → lease expires
11 sec  → another worker can claim it ❌
30 sec  → original worker finishes

```
Now two workers can process the same job.
So we need a heartbeat.

12.3 - What is a heartbeat?
- The worker periodically tells MongoDB:
  -- " I'm still processig this job. Extend my lease."

  Example:
  ```
Worker
  │
  ├── Claim → lease 10 sec
  │
  ├── after 5 sec → renew
  │
  ├── after 5 sec → renew
  │
  ├── after 5 sec → renew
  │
  └── job completed

  ```

  As long as the worker is alive, the lease remains valid.

#Create a Recovery Worker :
- Architecture:
                MongoDB
                   │
                   ▼
            Recovery Worker
                   │
          expired processing jobs
                   │
                   ▼
                 Redis
                   │
                   ▼
            Normal Workers
```

# Dead Letter Queue:
                    Job Created
                         ↓
                      queued
                         ↓
                    Redis Queue
                         ↓
                      Worker
                         ↓
                   processing
                    /       \
                 success    failure
                   ↓          ↓
              completed     retry
                              ↓
                         attempt 2
                              ↓
                           retry
                              ↓
                         attempt 3
                              ↓
                           failed

- The problem is that `failed` doesn't tell us much about why it failed, and `there's no convenient way to replay the job.`

# Why do we need a DLQ?
- imagine this job:
  `
  {
   "type":"fail",
   "payload":{
      "userId": 123
   }
  }

  worker:
   Attempt 1 -> failure
   Attempt 2 -> failure
   Attempt 3 -> failure

 We don't want:
   failure -> retry -> failure -> retry -> failure -> retry

instead:
 Attempt 1
   ↓
failure
   ↓
retry

Attempt 2
   ↓
failure
   ↓
retry

Attempt 3
   ↓
failure
   ↓
DLQ

# DLQ basically means:
  - This job failed too many times. Stop automatically processing it and keep it for investigation/replay.

# Repository have RetryJob:
- Processing - RetryJob() - queued - Enqueue Redis  
-  This is cleaner and consistent with your recovery mechanism.

# Job lifecycle:
                 CREATE
                   ↓
                QUEUED
                   ↓
                CLAIMED
                   ↓
              PROCESSING
                   │
             ┌─────┴─────┐
             │           │
          SUCCESS       FAIL
             │           │
             ↓           ↓
         COMPLETED    attempts < 3?
                         │
                    ┌────┴────┐
                    │         │
                   YES        NO
                    │         │
                    ↓         ↓
                  QUEUED     FAILED
                    │         │
                    ↓         ↓
                 REDIS       DLQ

# complete retry + DLQ flow.
Attempt 1
   ↓
Worker 2
   ↓
FAIL
   ↓
wait 1 sec
   ↓
RetryJob()
   ↓
Redis

- Then:

Attempt 2
   ↓
Worker 1
   ↓
FAIL
   ↓
wait 2 sec
   ↓
RetryJob()
   ↓
Redis
 
- Then:
Attempt 3
   ↓
Worker 3
   ↓
FAIL
   ↓
MaxAttempts reached
   ↓
FailJob()
   ↓
FAILED / DLQ
- This is especially good:
```
   Worker 2 → attempt 1
   Worker 1 → attempt 2
   Worker 3 → attempt 3
```
It Proves your job is not tied to a particular worker. Any worker can pick up the next attempt because the state is persisted in MongoDB and the job ID is placed back into Redis.

# At this point your system supports:
```
                 CREATE JOB
                     │
                     ▼
                  QUEUED
                     │
                     ▼
                Redis Queue
                     │
                     ▼
                  Worker
                     │
                     ▼
                ClaimJob()
                     │
                     ▼
                PROCESSING
                     │
              ┌──────┴──────┐
              │             │
           SUCCESS         FAIL
              │             │
              ▼             ▼
         COMPLETED      attempts < 3
                            │
                       ┌────┴────┐
                       │         │
                      YES        NO
                       │         │
                       ▼         ▼
                   RetryJob   FailJob
                       │         │
                       ▼         ▼
                    QUEUED     FAILED
                       │         │
                       ▼         ▼
                     Redis      DLQ
``

And you've also got crash recovery:
```
PROCESSING
    │
    │ Worker crashes
    ▼
Lease expires
    │
    ▼
Recovery Worker
    │
    ▼
QUEUED
    │
    ▼
Redis
    │
    ▼
Worker

```
That's a very solid concurrency/distributed-processing foundation for 

```
# DLQ Replay
 - If a job is permanently failed, an administrator should be able to replay it.
 
 For Example: GET /jobs/{id}


# Transactional Outbox Pattern.
- current CreateJob() effectively does:
  ```
      Request
        ↓
      MongoDB: Create Job
        ↓
      Redis: Enqueue Job

  ```
if the application crashes here:
 ```
   MongoDB
     ↓
  💥CRASH
     ↓
    Redis 

   - The job exists in MongoDB but isn't in Redis.

 ```

# The Outbox Pattern fixes this by storing an event alongside the job.
```
   MongoDB transaction
┌──────────────────────────┐
│ Create Job                │
│ Create Outbox Event       │
└────────────┬─────────────┘
             ↓
          COMMIT
             ↓
      Outbox Publisher
             ↓
          Redis
```
# OutboxEvent model
- What is this?
jobID = abc123

we create 
{
  "_id": "event-123",
  "type": "job.created",
  "jobId": "abc123",
  "status": "pending"
}

- This job needs to be published to Redis.
- outbox repository
- Why a separate collection
``
Database
│
├── jobs
│
└── outbox_events

``

- jobs:
```
{
  "_id": "job-123",
  "type": "email",
  "status": "queued"
}
```

outbox_events:
{
  "_id": "event-123",
  "type": "job.created",
  "jobId": "job-123",
  "status": "pending"
}

# Atomically Create Job + Outbox Event:
```
One MongoDB transaction
        │
        ├── Create Job
        │
        └── Create Outbox Event
        │
        ▼
      COMMIT
```