package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
)

// loadRecipients reads recipients from CSV, persists each as an email job in PostgreSQL,
// then sends the job to the channel for consumer processing.
// The original CSV reading logic is preserved.
func loadRecipients(ctx context.Context, repo *EmailJobRepo, filepath string, jobChan chan EmailJob) error {
	defer close(jobChan)
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := csv.NewReader(f)

	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, record := range records[1:] {
		r := Recipient{
			Name:  record[0],
			Email: record[1],
		}

		subject := fmt.Sprintf("Welcome to Screenlaps, %s! 🚀", r.Name)

		// Persist to PostgreSQL with status 'pending'
		id, err := repo.CreateJob(ctx, r.Email, subject, "")
		if err != nil {
			fmt.Printf("Producer: failed to persist job for %s: %v\n", r.Email, err)
			continue
		}

		// Send to channel for consumer processing (preserves existing channel pattern)
		jobChan <- EmailJob{
			ID:        id,
			Recipient: r,
		}
	}
	return nil
}
