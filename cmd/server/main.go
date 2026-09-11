package main

import (
	"context"
	"distributed-job-system/internal/database"
	"distributed-job-system/internal/handler"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/repository"
	"distributed-job-system/internal/service"
	"distributed-job-system/internal/worker"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
)

func main() {
	var shuttingDown atomic.Bool

	// Root application context.
	// When cancel() is called, all workers using this context
	// Will receive the cancellation signal.

	// ctx := context.Background()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// -------------------------------------------------------
	//	MongoDB		
	// -------------------------------------------------------

	mongoClient, err := database.ConnectMongodb(
		ctx,
		"mongodb://localhost:27017/distributed_jobs",
	)
	if err != nil {
		log.Fatal("MongoDB connection failed:", err)
	}

	log.Println("MongoDB connected")

	// -------------------------------------------------------
	// Redis	
	// -------------------------------------------------------
	
	redisQueue := queue.NewRedisQueue();

	if err := redisQueue.Ping(ctx); err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	log.Println("Redis connected")

	// -------------------------------------------------------
	//	Dependencies		
	// -------------------------------------------------------

	db := mongoClient.Database("distributed_jobs")

	if err := database.CreateJobIndexes(ctx, db); err != nil {
		log.Fatal("Failed to create MongoDB indexes:", err)
	}
	log.Println("MongoDB indexes created")
	
	jobRepository := repository.NewJobRepository(db)

	jobService := service.NewJobService(
		jobRepository,
		redisQueue,
	)

	jobHandler := handler.NewJobHandler(jobService)

	// -------------------------------------------------------
	// Router		
	// -------------------------------------------------------

	router := chi.NewRouter()

	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if shuttingDown.Load() {
				http.Error(
					w,"Server is shutting down",http.StatusServiceUnavailable,
				)
				return
			}

			next.ServeHTTP(w, r)
		})
	})

	router.Get("/health", func(w http.ResponseWriter, r *http.Request){
		w.WriteHeader(http.StatusOK)
		_,_  = w.Write([]byte("OK"))
	})

	router.Post("/jobs", jobHandler.CreateJob)
	router.Get("/jobs/{id}", jobHandler.GetJobByID)
	router.Get("/jobs", jobHandler.GetAllJobs)


	// -------------------------------------------------------
	// HTTP Server
	// -------------------------------------------------------

	server := &http.Server{
		Addr:	":8080",
		Handler: router,
	}

	// -------------------------------------------------------
	// Worker Pool	
	// -------------------------------------------------------

	// go jobWorker.Start(ctx)
	var wg sync.WaitGroup

	for i := 1; i <= 3; i++ {
		jobWorker := worker.NewJobWorker(
			i,
			redisQueue,
			jobRepository,
		)
		
		wg.Add(1)
		
		go func(w *worker.JobWorker) {
			defer wg.Done()

			w.Start(ctx)
		}(jobWorker)

		// go jobWorker.Start(ctx)
	}

	// -------------------------------------------------------
	// Start HTTP Server		
	// -------------------------------------------------------
	go func() {
		log.Println("Server running on :8080")

		if err := server.ListenAndServe(); err != nil && 
			err != http.ErrServerClosed {
			log.Fatal("HTTP server failed:", err)
		}
	}()
	
	// -------------------------------------------------------
	// OS Shutdown Signal	
	// -------------------------------------------------------

	signalChan := make(chan os.Signal, 1)

	signal.Notify(
		signalChan,
		os.Interrupt,
		syscall.SIGTERM,
	)
	
	// Wait until Ctrl+C / SIGTERM
	<-signalChan

	log.Println("Shutdown signal received")

	// Immediately reject new HTTP requests.
	shuttingDown.Store(true)

	log.Println("Application is now in shutdown mode")
	
	// -------------------------------------------------------
	// Shutdown HTTP Server	
	// -------------------------------------------------------
	
	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer shutdownCancel()
	
	log.Println("Shutting down HTTP server...")
	
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Println("HTTP server shutdown error:", err)
		} else {
			log.Println("HTTP server stopped")
		}
		
		
			// -------------------------------------------------------
			// Cancel Worker Context	
			// -------------------------------------------------------
		
			cancel()
		
			log.Println("Waiting for workers to stop...")
			wg.Wait()
		
			log.Println("All workers stopped")
			
		// -------------------------------------------------------
		// Disconnect MongoDB		
		// -------------------------------------------------------

	log.Println("Disconnecting MongoDB...")

	if err := mongoClient.Disconnect(shutdownCtx); err != nil {
		log.Println("MongoDB disconnect error:", err)
	} else {
		log.Println("MongoDB disconnected")
	}

	log.Println("Application shutdown complete")

/**
Ctrl+C
   ↓
signalChan
   ↓
cancel()
   ↓
ctx.Done()
   ↓
workers stop

*/

}