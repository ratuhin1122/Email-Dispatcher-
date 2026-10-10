package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// setupTestRedis creates a Redis client for testing.
// Skips the test if Redis is not available.
func setupTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	cfg := LoadConfig()
	client, err := InitRedis(cfg)
	if err != nil {
		t.Skipf("Skipping Redis test: Redis not available: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// cleanRateLimitKeys removes all rate-limit keys to isolate tests.
func cleanRateLimitKeys(t *testing.T, client *redis.Client) {
	t.Helper()
	ctx := context.Background()
	iter := client.Scan(ctx, 0, "ratelimit:email:*", 100).Iterator()
	for iter.Next(ctx) {
		client.Del(ctx, iter.Val())
	}
}

func TestRateLimiter_BelowLimit(t *testing.T) {
	client := setupTestRedis(t)
	cleanRateLimitKeys(t, client)

	rl := NewRateLimiter(client, 10, 60*time.Second)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		allowed, err := rl.Allow(ctx)
		if err != nil {
			t.Fatalf("Allow() error: %v", err)
		}
		if !allowed {
			t.Fatalf("Expected request %d to be allowed, but was denied", i+1)
		}
	}
}

func TestRateLimiter_ExceedsLimit(t *testing.T) {
	client := setupTestRedis(t)
	cleanRateLimitKeys(t, client)

	limit := 3
	rl := NewRateLimiter(client, limit, 60*time.Second)

	ctx := context.Background()

	// Use up all the allowed requests
	for i := 0; i < limit; i++ {
		allowed, err := rl.Allow(ctx)
		if err != nil {
			t.Fatalf("Allow() error on request %d: %v", i+1, err)
		}
		if !allowed {
			t.Fatalf("Expected request %d to be allowed", i+1)
		}
	}

	// Next request should be denied
	allowed, err := rl.Allow(ctx)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if allowed {
		t.Fatal("Expected request beyond limit to be denied, but was allowed")
	}
}

func TestRateLimiter_WindowReset(t *testing.T) {
	client := setupTestRedis(t)
	cleanRateLimitKeys(t, client)

	// Use a 1-second window so we can test reset quickly
	limit := 2
	rl := NewRateLimiter(client, limit, 1*time.Second)

	ctx := context.Background()

	// Use up all the allowed requests
	for i := 0; i < limit; i++ {
		allowed, err := rl.Allow(ctx)
		if err != nil {
			t.Fatalf("Allow() error: %v", err)
		}
		if !allowed {
			t.Fatalf("Expected request %d to be allowed", i+1)
		}
	}

	// Should be denied now
	allowed, err := rl.Allow(ctx)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if allowed {
		t.Fatal("Expected to be rate limited")
	}

	// Wait for window to reset
	time.Sleep(1100 * time.Millisecond)

	// Should be allowed again after window reset
	allowed, err = rl.Allow(ctx)
	if err != nil {
		t.Fatalf("Allow() error after reset: %v", err)
	}
	if !allowed {
		t.Fatal("Expected request to be allowed after window reset")
	}
}

func TestRateLimiter_MultipleConsumersSharedLimit(t *testing.T) {
	client := setupTestRedis(t)
	cleanRateLimitKeys(t, client)

	limit := 5
	rl := NewRateLimiter(client, limit, 60*time.Second)

	ctx := context.Background()
	var allowedCount atomic.Int32
	var deniedCount atomic.Int32

	// Simulate 3 consumers each trying to send 3 emails (total 9, limit 5)
	var wg sync.WaitGroup
	consumerCount := 3
	requestsPerConsumer := 3

	for c := 0; c < consumerCount; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < requestsPerConsumer; i++ {
				allowed, err := rl.Allow(ctx)
				if err != nil {
					t.Errorf("Allow() error: %v", err)
					return
				}
				if allowed {
					allowedCount.Add(1)
				} else {
					deniedCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	total := int(allowedCount.Load()) + int(deniedCount.Load())
	if total != consumerCount*requestsPerConsumer {
		t.Errorf("Expected %d total requests, got %d", consumerCount*requestsPerConsumer, total)
	}
	if int(allowedCount.Load()) > limit {
		t.Errorf("Expected at most %d allowed, got %d", limit, allowedCount.Load())
	}
	if int(allowedCount.Load()) < limit {
		t.Errorf("Expected exactly %d allowed (all within limit), got %d", limit, allowedCount.Load())
	}
}

func TestRateLimiter_ConcurrentRequests(t *testing.T) {
	client := setupTestRedis(t)
	cleanRateLimitKeys(t, client)

	limit := 10
	rl := NewRateLimiter(client, limit, 60*time.Second)

	ctx := context.Background()
	var allowedCount atomic.Int32

	// Fire 20 concurrent requests with limit of 10
	var wg sync.WaitGroup
	goroutines := 20
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed, err := rl.Allow(ctx)
			if err != nil {
				t.Errorf("Allow() error: %v", err)
				return
			}
			if allowed {
				allowedCount.Add(1)
			}
		}()
	}

	wg.Wait()

	if int(allowedCount.Load()) != limit {
		t.Errorf("Expected exactly %d allowed out of %d concurrent requests, got %d",
			limit, goroutines, allowedCount.Load())
	}
}

func TestRateLimiter_RedisUnavailable_FailOpen(t *testing.T) {
	// Create a client pointing to a non-existent Redis server
	client := redis.NewClient(&redis.Options{
		Addr:        "localhost:59999", // unlikely to be running
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	})
	defer client.Close()

	rl := NewRateLimiter(client, 5, 60*time.Second)
	// failOpen is true by default

	ctx := context.Background()
	allowed, err := rl.Allow(ctx)
	if err != nil {
		t.Fatalf("Expected fail-open to return nil error, got: %v", err)
	}
	if !allowed {
		t.Fatal("Expected fail-open to allow the request when Redis is unavailable")
	}
}

func TestRateLimiter_Wait(t *testing.T) {
	client := setupTestRedis(t)
	cleanRateLimitKeys(t, client)

	// Limit of 2, 1-second window
	rl := NewRateLimiter(client, 2, 1*time.Second)

	ctx := context.Background()

	// Use up the limit
	for i := 0; i < 2; i++ {
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("Wait() error: %v", err)
		}
	}

	// Next Wait should block until window resets, then succeed
	start := time.Now()
	if err := rl.Wait(ctx); err != nil {
		t.Fatalf("Wait() error after limit: %v", err)
	}
	elapsed := time.Since(start)

	// Should have waited at least ~100ms (first backoff tick)
	if elapsed < 100*time.Millisecond {
		t.Errorf("Expected Wait to block, but returned in %v", elapsed)
	}
}

func TestRateLimiter_WaitContextCancelled(t *testing.T) {
	client := setupTestRedis(t)
	cleanRateLimitKeys(t, client)

	// Limit of 1, long window
	rl := NewRateLimiter(client, 1, 60*time.Second)

	ctx := context.Background()

	// Use up the limit
	allowed, _ := rl.Allow(ctx)
	if !allowed {
		t.Fatal("First request should be allowed")
	}

	// Wait with a cancelled context should return quickly
	cancelCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := rl.Wait(cancelCtx)
	if err == nil {
		t.Fatal("Expected Wait to return error on context cancellation")
	}
}
