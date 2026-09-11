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

  
