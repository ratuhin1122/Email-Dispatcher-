package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// setupTestRepo creates a repo connected to the test database.
// Skips the test if PostgreSQL is not available.
func setupTestRepo(t *testing.T) *EmailJobRepo {
	t.Helper()
	cfg := LoadConfig()
	db, err := InitDB(cfg)
	if err != nil {
		t.Skipf("Skipping integration test: database not available: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Clean up test data from previous runs
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = db.ExecContext(ctx, "DELETE FROM email_jobs WHERE recipient LIKE '%@test.example.com'")

	return NewEmailJobRepo(db)
}

func TestCreateJob(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	id, err := repo.CreateJob(ctx, "create@test.example.com", "Test Subject", "Test Body")
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	if id == "" {
		t.Fatal("Expected non-empty UUID")
	}

	job, err := repo.GetJobByID(ctx, id)
	if err != nil {
		t.Fatalf("GetJobByID failed: %v", err)
	}
	if job.Status != "pending" {
		t.Errorf("Expected status 'pending', got '%s'", job.Status)
	}
	if job.Recipient != "create@test.example.com" {
		t.Errorf("Expected recipient 'create@test.example.com', got '%s'", job.Recipient)
	}
	if job.Attempts != 0 {
		t.Errorf("Expected 0 attempts, got %d", job.Attempts)
	}
}

func TestEmailJobLifecycle_Sent(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	// Create → pending
	id, err := repo.CreateJob(ctx, "lifecycle-sent@test.example.com", "Lifecycle Test", "")
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// pending → processing
	if err := repo.MarkProcessing(ctx, id); err != nil {
		t.Fatalf("MarkProcessing failed: %v", err)
	}
	job, _ := repo.GetJobByID(ctx, id)
	if job.Status != "processing" {
		t.Errorf("Expected status 'processing', got '%s'", job.Status)
	}

	// processing → sent
	if err := repo.MarkSent(ctx, id, "<html>rendered body</html>"); err != nil {
		t.Fatalf("MarkSent failed: %v", err)
	}
	job, _ = repo.GetJobByID(ctx, id)
	if job.Status != "sent" {
		t.Errorf("Expected status 'sent', got '%s'", job.Status)
	}
	if !job.SentAt.Valid {
		t.Error("Expected sent_at to be set")
	}
	if job.Body != "<html>rendered body</html>" {
		t.Errorf("Expected body to be updated, got '%s'", job.Body)
	}
}

func TestEmailJobLifecycle_Failed(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	// Create → pending
	id, err := repo.CreateJob(ctx, "lifecycle-fail@test.example.com", "Failure Test", "")
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// pending → processing
	if err := repo.MarkProcessing(ctx, id); err != nil {
		t.Fatalf("MarkProcessing failed: %v", err)
	}

	// processing → failed
	if err := repo.MarkFailed(ctx, id, "connection refused"); err != nil {
		t.Fatalf("MarkFailed failed: %v", err)
	}
	job, _ := repo.GetJobByID(ctx, id)
	if job.Status != "failed" {
		t.Errorf("Expected status 'failed', got '%s'", job.Status)
	}
	if job.Attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", job.Attempts)
	}
	if !job.ErrorMessage.Valid || job.ErrorMessage.String != "connection refused" {
		t.Errorf("Expected error_message 'connection refused', got '%v'", job.ErrorMessage)
	}
	if job.SentAt.Valid {
		t.Error("Expected sent_at to be NULL for failed job")
	}
}

func TestMarkProcessingIdempotency(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	id, err := repo.CreateJob(ctx, "idempotent@test.example.com", "Idempotency Test", "")
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// First call succeeds: pending → processing
	if err := repo.MarkProcessing(ctx, id); err != nil {
		t.Fatalf("First MarkProcessing should succeed: %v", err)
	}

	// Second call fails: already in 'processing', not 'pending'
	if err := repo.MarkProcessing(ctx, id); err == nil {
		t.Error("Second MarkProcessing should fail but succeeded")
	}
}

func TestMarkSentRequiresProcessing(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	id, err := repo.CreateJob(ctx, "sent-guard@test.example.com", "Guard Test", "")
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Attempting to mark sent directly from pending should fail
	if err := repo.MarkSent(ctx, id, "body"); err == nil {
		t.Error("MarkSent from pending should fail but succeeded")
	}
}

func TestConnectionPoolConfig(t *testing.T) {
	cfg := LoadConfig()
	db, err := InitDB(cfg)
	if err != nil {
		t.Skipf("Skipping pool test: database not available: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if stats.MaxOpenConnections != cfg.DBMaxOpenConns {
		t.Errorf("Expected MaxOpenConnections=%d, got %d", cfg.DBMaxOpenConns, stats.MaxOpenConnections)
	}
}

func TestEmailWorkerFailure(t *testing.T) {
	repo := setupTestRepo(t)
	ctx := context.Background()

	// Configure with invalid SMTP port to force network failure
	badCfg := LoadConfig()
	badCfg.SMTPPort = "9999"

	id, err := repo.CreateJob(ctx, "fail-worker@test.example.com", "Worker Fail Test", "")
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	jobChan := make(chan EmailJob, 1)
	dlqChan := make(chan FailedJob, 1)
	var wg sync.WaitGroup

	jobChan <- EmailJob{
		ID:        id,
		Recipient: Recipient{Name: "Fail User", Email: "fail-worker@test.example.com"},
	}
	close(jobChan)

	wg.Add(1)
	go emailWorker(1, badCfg, repo, nil, jobChan, dlqChan, &wg)
	wg.Wait()
	close(dlqChan)

	// Check that a failed job was emitted to dlqChan
	failedJob, ok := <-dlqChan
	if !ok {
		t.Fatal("Expected failed job on dlqChan, got none")
	}
	if failedJob.JobID != id {
		t.Errorf("Expected FailedJob JobID=%s, got %s", id, failedJob.JobID)
	}
	if failedJob.Reason != "SMTP_ERROR" {
		t.Errorf("Expected Reason='SMTP_ERROR', got %s", failedJob.Reason)
	}

	// Check database state
	job, err := repo.GetJobByID(ctx, id)
	if err != nil {
		t.Fatalf("GetJobByID failed: %v", err)
	}
	if job.Status != "failed" {
		t.Errorf("Expected status 'failed', got '%s'", job.Status)
	}
	if job.Attempts != 1 {
		t.Errorf("Expected attempts=1, got %d", job.Attempts)
	}
	if !job.ErrorMessage.Valid || job.ErrorMessage.String == "" {
		t.Error("Expected error_message to be populated")
	}
}
