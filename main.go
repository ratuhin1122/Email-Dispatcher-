package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"os"
	"sync"
	"time"
)

// Recipient represents a target email address (unchanged from original).
type Recipient struct {
	Name  string
	Email string
}

// EmailJob carries a database-tracked job through the channel.
type EmailJob struct {
	ID        string
	Recipient Recipient
}

// FailedJob captures failure context for the Dead Letter Queue.
type FailedJob struct {
	JobID     string
	Recipient Recipient
	Reason    string
	Error     string
	Timestamp time.Time
}

func main() {
	// Load configuration from environment variables / .env
	cfg := LoadConfig()

	// Handle "migrate" subcommand: go run . migrate [down]
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		db, err := InitDB(cfg)
		if err != nil {
			fmt.Printf("❌ Database connection failed: %v\n", err)
			os.Exit(1)
		}
		defer db.Close()

		direction := "up"
		if len(os.Args) > 2 && os.Args[2] == "down" {
			direction = "down"
		}
		fmt.Printf("Running migrations (%s)...\n", direction)
		if err := RunMigrations(db, direction); err != nil {
			fmt.Printf("❌ Migration failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("✅ Migrations complete")
		return
	}

	// Initialize database connection pool
	db, err := InitDB(cfg)
	if err != nil {
		fmt.Printf("❌ Database connection failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	fmt.Printf("✅ Database connected (pool: maxOpen=%d, maxIdle=%d)\n",
		cfg.DBMaxOpenConns, cfg.DBMaxIdleConns)

	repo := NewEmailJobRepo(db)

	// Initialize Redis client and rate limiter.
	// Redis is non-fatal: if unavailable, the app continues without rate limiting
	// (fail-open strategy — no email jobs are lost).
	var rl *RateLimiter
	redisClient, err := InitRedis(cfg)
	if err != nil {
		fmt.Printf("⚠️  Redis connection failed (continuing without rate limiting): %v\n", err)
	} else {
		defer redisClient.Close()
		rl = NewRateLimiter(redisClient, cfg.EmailRateLimit, cfg.EmailRateWindow)
		fmt.Printf("✅ Redis connected (pool: size=%d, minIdle=%d) — rate limit: %d emails per %s\n",
			cfg.RedisPoolSize, cfg.RedisMinIdleConns, cfg.EmailRateLimit, cfg.EmailRateWindow)
	}

	// Channels — same pattern as original, now carrying EmailJob instead of Recipient
	jobChan := make(chan EmailJob)
	dlqChan := make(chan FailedJob)

	// Producer goroutine
	go func() {
		if err := loadRecipients(context.Background(), repo, "emails.csv", jobChan); err != nil {
			fmt.Printf("Producer error: %v\n", err)
		}
	}()

	// 3 consumer workers (unchanged count)
	var workerWg sync.WaitGroup
	workerCount := 3
	for i := 1; i <= workerCount; i++ {
		workerWg.Add(1)
		go emailWorker(i, cfg, repo, rl, jobChan, dlqChan, &workerWg)
	}

	// DLQ worker
	var dlqWg sync.WaitGroup
	dlqWg.Add(1)
	go dlqWorker(dlqChan, &dlqWg)

	// Wait for all email workers to finish
	workerWg.Wait()

	// Close DLQ channel after all email workers are done
	close(dlqChan)

	// Wait for DLQ worker to finish flushing failed emails to disk
	dlqWg.Wait()
}

func executeTemplate(r Recipient) (string, error) {
	t, err := template.ParseFiles("email.tmpl")
	if err != nil {
		return "", err
	}
	var tpl bytes.Buffer
	err = t.Execute(&tpl, r)
	if err != nil {
		return "", err
	}
	return tpl.String(), nil
}
