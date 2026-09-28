package main

import (
	"encoding/csv"
	"fmt"
	"net/smtp"
	"os"
	"sync"
	"time"
)

func emailWorker(id int, ch chan Recipient, dlqChan chan FailedJob, wg *sync.WaitGroup) {
	defer wg.Done()
	for r := range ch {
		smtpHost := "localhost"
		smtpPort := "1025"

		msg, err := executeTemplate(r)
		if err != nil {
			fmt.Printf("Worker %d: template error for %s: %v\n", id, r.Email, err)
			dlqChan <- FailedJob{
				Recipient: r,
				Reason:    "TEMPLATE_ERROR",
				Error:     err.Error(),
				Timestamp: time.Now(),
			}
			continue
		}

		err = smtp.SendMail(
			smtpHost+":"+smtpPort,
			nil,
			"admin@gmail.com",
			[]string{r.Email},
			[]byte(msg),
		)
		if err != nil {
			fmt.Printf("Worker %d: send error for %s: %v\n", id, r.Email, err)
			dlqChan <- FailedJob{
				Recipient: r,
				Reason:    "SMTP_ERROR",
				Error:     err.Error(),
				Timestamp: time.Now(),
			}
			continue
		}
		time.Sleep(50 * time.Millisecond)

		fmt.Printf("Email sent to %s by worker %d\n", r.Email, id)
	}
}

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
		writer.Write([]string{"Name", "Email", "Reason", "Error", "Timestamp"})
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
		})
	}

	if failureCount > 0 {
		fmt.Printf("📦 DLQ processing complete: %d failed job(s) logged to %s\n", failureCount, fileName)
	}
}