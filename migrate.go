package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// RunMigrations executes SQL migration files from the migrations/ directory.
// direction should be "up" or "down".
func RunMigrations(db *sql.DB, direction string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Ensure the schema_migrations tracking table exists
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	if direction == "down" {
		return runDown(ctx, db)
	}
	return runUp(ctx, db)
}

func runUp(ctx context.Context, db *sql.DB) error {
	files, err := filepath.Glob("migrations/*.up.sql")
	if err != nil {
		return fmt.Errorf("glob migration files: %w", err)
	}
	if len(files) == 0 {
		fmt.Println("No migration files found in migrations/")
		return nil
	}
	sort.Strings(files)

	applied := 0
	for _, file := range files {
		version := extractVersion(file)

		// Check if already applied
		var exists bool
		err := db.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", version,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if exists {
			fmt.Printf("  ✓ %s (already applied)\n", version)
			continue
		}

		// Read the SQL file
		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}

		// Execute in a transaction
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", version, err)
		}

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			tx.Rollback()
			return fmt.Errorf("execute migration %s: %w", version, err)
		}

		if _, err := tx.ExecContext(ctx,
			"INSERT INTO schema_migrations (version) VALUES ($1)", version,
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %s: %w", version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}

		fmt.Printf("  ▶ Applied: %s\n", version)
		applied++
	}

	if applied == 0 {
		fmt.Println("All migrations are up to date")
	} else {
		fmt.Printf("Applied %d migration(s)\n", applied)
	}
	return nil
}

func runDown(ctx context.Context, db *sql.DB) error {
	// Get the latest applied migration
	var version string
	err := db.QueryRowContext(ctx,
		"SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
	).Scan(&version)
	if err == sql.ErrNoRows {
		fmt.Println("No migrations to roll back")
		return nil
	}
	if err != nil {
		return fmt.Errorf("get latest migration: %w", err)
	}

	// Find the corresponding .down.sql file
	downFile := filepath.Join("migrations", version+".down.sql")
	content, err := os.ReadFile(downFile)
	if err != nil {
		return fmt.Errorf("read down migration %s: %w", downFile, err)
	}

	// Execute in a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for rollback %s: %w", version, err)
	}

	if _, err := tx.ExecContext(ctx, string(content)); err != nil {
		tx.Rollback()
		return fmt.Errorf("execute down migration %s: %w", version, err)
	}

	if _, err := tx.ExecContext(ctx,
		"DELETE FROM schema_migrations WHERE version = $1", version,
	); err != nil {
		tx.Rollback()
		return fmt.Errorf("remove migration record %s: %w", version, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rollback %s: %w", version, err)
	}

	fmt.Printf("  ◀ Rolled back: %s\n", version)
	return nil
}

// extractVersion derives the migration version name from a filename.
// "migrations/001_create_email_jobs.up.sql" → "001_create_email_jobs"
func extractVersion(filename string) string {
	base := filepath.Base(filename)
	base = strings.TrimSuffix(base, ".up.sql")
	base = strings.TrimSuffix(base, ".down.sql")
	return base
}
