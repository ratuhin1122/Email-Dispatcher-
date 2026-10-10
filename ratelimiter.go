package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// luaRateLimit is a Lua script that atomically increments a fixed-window
// counter in Redis. It returns the current count after incrementing.
//
// Algorithm: Fixed-Window Counter
//   - Key format: ratelimit:email:<window_timestamp>
//   - On first request in a window, INCR creates the key with value 1
//     and EXPIRE sets the TTL to the window duration.
//   - Subsequent requests within the window simply INCR.
//   - When the window expires, Redis deletes the key automatically,
//     resetting the counter for the next window.
//
// The entire script runs atomically on the Redis server, preventing
// race conditions between the 3 concurrent consumers.
var luaRateLimit = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end

return current
`)

// RateLimiter provides a shared, Redis-backed rate limiter for email sending.
// All consumers share the same rate limit by using the same Redis key.
type RateLimiter struct {
	client *redis.Client
	limit  int
	window time.Duration

	// failOpen controls behavior when Redis is unavailable.
	// true  = allow sends to proceed (log warning, don't lose jobs)
	// false = block sends until Redis recovers
	failOpen bool
}

// NewRateLimiter creates a rate limiter backed by the given Redis client.
// limit is the maximum number of emails allowed per window.
// window is the duration of each rate-limit window.
func NewRateLimiter(client *redis.Client, limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		client:   client,
		limit:    limit,
		window:   window,
		failOpen: true, // fail open: allow sends when Redis is down
	}
}

// Allow checks whether a send is permitted under the current rate limit.
// It returns true if the request is within the limit, false if the limit
// has been reached.
//
// If Redis is unavailable and failOpen is true, Allow returns true with
// a logged warning. No email jobs are lost.
//
// If Redis is unavailable and failOpen is false, Allow returns false,
// causing the consumer to wait/retry.
func (rl *RateLimiter) Allow(ctx context.Context) (bool, error) {
	// Compute the current window key based on truncated time
	windowSeconds := int(rl.window.Seconds())
	if windowSeconds <= 0 {
		windowSeconds = 1
	}
	windowStart := time.Now().Unix() / int64(windowSeconds) * int64(windowSeconds)
	key := fmt.Sprintf("ratelimit:email:%d", windowStart)

	result, err := luaRateLimit.Run(ctx, rl.client, []string{key}, rl.limit, windowSeconds).Int()
	if err != nil {
		if rl.failOpen {
			fmt.Printf("⚠️  Rate limiter: Redis unavailable, failing open: %v\n", err)
			return true, nil
		}
		return false, fmt.Errorf("rate limiter: Redis error: %w", err)
	}

	return result <= rl.limit, nil
}

// Wait blocks until the rate limiter allows the request through.
// It uses an exponential backoff strategy to avoid busy-waiting.
// The maximum wait is bounded by the window duration.
//
// This ensures email jobs are never lost — they are retried until
// the rate-limit window resets.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	backoff := 100 * time.Millisecond
	maxBackoff := 2 * time.Second

	for {
		allowed, err := rl.Allow(ctx)
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}

		// Rate limit exceeded — wait and retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// Exponential backoff, capped at maxBackoff
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// Limit returns the configured rate limit.
func (rl *RateLimiter) Limit() int {
	return rl.limit
}

// Window returns the configured rate-limit window duration.
func (rl *RateLimiter) Window() time.Duration {
	return rl.window
}
