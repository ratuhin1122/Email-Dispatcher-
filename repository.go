package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// EmailJobRecord represents a row in the email_jobs table.
type EmailJobRecord struct {
	ID           string
	Recipient    string
	Subject      string
	Body         string
	Status       string
	Attempts     int
	ErrorMessage sql.NullString
	CreatedAt    time.Time
	UpdatedAt    time.Time
	SentAt       sql.NullTime
}

// EmailJobRepo provides data access methods for the email_jobs table.
type EmailJobRepo struct {
	db *sql.DB
}

// NewEmailJobRepo creates a new repository backed by the given connection pool.
func NewEmailJobRepo(db *sql.DB) *EmailJobRepo {
	return &EmailJobRepo{db: db}
}

// CreateJob inserts a new email job with status 'pending' and returns the generated UUID.
func (r *EmailJobRepo) CreateJob(ctx context.Context, recipient, subject, body string) (string, error) {
	var id string
	err := r.db.QueryRowContext(ctx,
		`INSERT INTO email_jobs (recipient, subject, body, status)
		 VALUES ($1, $2, $3, 'pending')
		 RETURNING id`,
		recipient, subject, body,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("create job: %w", err)
	}
	return id, nil
}

// MarkProcessing atomically transitions a job from 'pending' to 'processing'.
// The WHERE clause ensures only a job in 'pending' status can be transitioned,
// providing safety when multiple consumers are running concurrently.
func (r *EmailJobRepo) MarkProcessing(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE email_jobs
		 SET status = 'processing', updated_at = NOW()
		 WHERE id = $1 AND status = 'pending'`,
		id,
	)
	if err != nil {
		return fmt.Errorf("mark processing: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark processing rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("mark processing: job %s not found or not in pending status", id)
	}
	return nil
}

// MarkSent transitions a job to 'sent' and records the rendered body and sent timestamp.
func (r *EmailJobRepo) MarkSent(ctx context.Context, id string, body string) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE email_jobs
		 SET status = 'sent', body = $2, sent_at = NOW(), updated_at = NOW()
		 WHERE id = $1 AND status = 'processing'`,
		id, body,
	)
	if err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark sent rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("mark sent: job %s not found or not in processing status", id)
	}
	return nil
}

// MarkFailed transitions a job to 'failed', increments attempts, and stores the error message.
func (r *EmailJobRepo) MarkFailed(ctx context.Context, id string, errMsg string) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE email_jobs
		 SET status = 'failed', attempts = attempts + 1, error_message = $2, updated_at = NOW()
		 WHERE id = $1 AND status = 'processing'`,
		id, errMsg,
	)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark failed rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("mark failed: job %s not found or not in processing status", id)
	}
	return nil
}

// GetJobByID retrieves a single email job by its UUID.
func (r *EmailJobRepo) GetJobByID(ctx context.Context, id string) (*EmailJobRecord, error) {
	job := &EmailJobRecord{}
	err := r.db.QueryRowContext(ctx,
		`SELECT id, recipient, subject, body, status, attempts,
		        error_message, created_at, updated_at, sent_at
		 FROM email_jobs WHERE id = $1`,
		id,
	).Scan(
		&job.ID, &job.Recipient, &job.Subject, &job.Body, &job.Status,
		&job.Attempts, &job.ErrorMessage, &job.CreatedAt, &job.UpdatedAt, &job.SentAt,
	)
	if err != nil {
		return nil, fmt.Errorf("get job by id: %w", err)
	}
	return job, nil
}
