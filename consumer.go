package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/smtp"
	"os"
	"sync"
	"time"
)

// emailWorker processes email jobs from the channel.
// For each job it: marks as processing → renders template → sends via SMTP → marks as sent/failed.
// The existing Mailpit SMTP sending logic is preserved.
func emailWorker(id int, cfg *Config, repo *EmailJobRepo, ch chan EmailJob, dlqChan chan FailedJob, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range ch {
		ctx := context.Background()
		r := job.Recipient

		// Mark as processing in database
		if err := repo.MarkProcessing(ctx, job.ID); err != nil {
			fmt.Printf("Worker %d: failed to mark processing for %s: %v\n", id, r.Email, err)
		}

		// Render email template (existing logic)
		msg, err := executeTemplate(r)
		if err != nil {
			fmt.Printf("Worker %d: template error for %s: %v\n", id, r.Email, err)

			// Mark as failed in database
			if dbErr := repo.MarkFailed(ctx, job.ID, err.Error()); dbErr != nil {
				fmt.Printf("Worker %d: failed to mark failed for %s: %v\n", id, r.Email, dbErr)
			}

			dlqChan <- FailedJob{
				JobID:     job.ID,
				Recipient: r,
				Reason:    "TEMPLATE_ERROR",
				Error:     err.Error(),
				Timestamp: time.Now(),
			}
			continue
		}

		// Send via SMTP to Mailpit (existing logic, now using config)
		err = smtp.SendMail(
			cfg.SMTPHost+":"+cfg.SMTPPort,
			nil,
			"admin@gmail.com",
			[]string{r.Email},
			[]byte(msg),
		)
		if err != nil {
			fmt.Printf("Worker %d: send error for %s: %v\n", id, r.Email, err)

			// Mark as failed in database
			if dbErr := repo.MarkFailed(ctx, job.ID, err.Error()); dbErr != nil {
				fmt.Printf("Worker %d: failed to mark failed for %s: %v\n", id, r.Email, dbErr)
			}

			dlqChan <- FailedJob{
				JobID:     job.ID,
				Recipient: r,
				Reason:    "SMTP_ERROR",
				Error:     err.Error(),
				Timestamp: time.Now(),
			}
			continue
		}

		// Mark as sent in database with the rendered body
		if dbErr := repo.MarkSent(ctx, job.ID, msg); dbErr != nil {
			fmt.Printf("Worker %d: failed to mark sent for %s: %v\n", id, r.Email, dbErr)
		}

		time.Sleep(50 * time.Millisecond)

		fmt.Printf("Email sent to %s by worker %d\n", r.Email, id)
	}
}

// dlqWorker captures failed email jobs and writes them to a CSV file.
// Preserved from the original implementation with JobID column added.
func dlqWorker(dlqChan chan FailedJob, wg *sync.WaitGroup) {
	defer wg.Done()

	fileName := "failed_emails.csv"
	fileExists := false
	if _, err := os.Stat(fileName); err == nil {
		fileExists = true
	}

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error opening DLQ file: %v\n", err)
		return
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	// Write header row if file is brand new
	if !fileExists {
		writer.Write([]string{"Name", "Email", "Reason", "Error", "Timestamp", "JobID"})
	}

	failureCount := 0
	for job := range dlqChan {
		failureCount++
		fmt.Printf("⚠️  [DLQ] Captured failure for %s: %s\n", job.Recipient.Email, job.Error)
		writer.Write([]string{
			job.Recipient.Name,
			job.Recipient.Email,
			job.Reason,
			job.Error,
			job.Timestamp.Format(time.RFC3339),
			job.JobID,
		})
	}

	if failureCount > 0 {
		fmt.Printf("📦 DLQ processing complete: %d failed job(s) logged to %s\n", failureCount, fileName)
	}
}
