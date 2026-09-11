package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"distributed-job-system/internal/job"
	"distributed-job-system/internal/service"

	"github.com/go-chi/chi/v5"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type JobHandler	struct {
	service	*service.JobService
}

func NewJobHandler(service *service.JobService,) *JobHandler {
	return &JobHandler{
		service: service,
	}
} 

func (h *JobHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	
	var req job.CreateJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Println("Decode error:", err)
		http.Error(
			w,
			"invalid request body",
			http.StatusBadRequest,
		)
		return
	}

	job, created ,err := h.service.CreateJob(
		r.Context(),
		&req,
	)

	if err != nil {
		http.Error(
			w,
			"failed to created job",
			http.StatusInternalServerError,
		)
		return
	}

	status := http.StatusOK

	if created {
		status = http.StatusCreated
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_= json.NewEncoder(w).Encode(job)
}

func (h *JobHandler) GetJobByID(w http.ResponseWriter, r *http.Request) {

	id := chi.URLParam(r, "id")

	job, err := h.service.GetJobByID(
		r.Context(),
		id,
	)

// r.Context() -> Give me the context associated with this HTTP request.
// Think of it as a request lifecycle control object.
/**
HTTP Request
   |
   Context
       | Cancellation
	   | Deadline
	   | Request-scoped values
 
It allows work deeper in Your application to know:
*Is the request still alive?
*Has the client disconnected?
*Has the Deadline expired?
*Has this request been cancelled?
*Are there request-scoped values that need to travel with it?


HTTP Request
     │
     │ r.Context()
     ↓
  Handler
     │
     │ ctx
     ↓
  Service
     │
     │ ctx
     ↓
 Repository
     │
     │ ctx
     ↓
 MongoDB


This is exactly the pattern we want:
                REQUEST
                   │
                   ↓
             r.Context()
                   │
                   ↓
                Handler
                   │
                   │ ctx
                   ↓
                Service
                   │
                   │ ctx
                   ↓
              Repository
                   │
                   │ ctx
                   ↓
                MongoDB

context.Context carries request-scoped cancellation, deadlines, and values across API boundaries, allowing operations such as database calls and external requests to stop when the original request is cancelled or times out.
*/
	if err != nil {

		if errors.Is(err, mongo.ErrNoDocuments) {
			http.Error(
				w,
				"job not found",
				http.StatusNotFound,
			)
			return
		}

		http.Error(
			w,
			"failed to get job",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	json.NewEncoder(w).Encode(job)
}

func (h *JobHandler) GetAllJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.service.GetAllJobs(r.Context())
	if err != nil {
		http.Error(w,"failed to fetched jobs", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type","application/json")
	json.NewEncoder(w).Encode(jobs)
}