package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// InitRedis creates a Redis client with connection pooling configured
// according to cfg. The pool is managed internally by go-redis.
//
// Pool sizing rationale:
//   - PoolSize (default 5):  3 consumers + headroom for health checks.
//   - MinIdleConns (default 2): keeps warm connections ready to avoid
//     cold-start latency on burst dispatches.
func InitRedis(cfg *Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisHost + ":" + cfg.RedisPort,
		Password:     cfg.RedisPassword,
		DB:           0,
		PoolSize:     cfg.RedisPoolSize,
		MinIdleConns: cfg.RedisMinIdleConns,
		DialTimeout:  cfg.RedisDialTimeout,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Verify connectivity with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisDialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	return client, nil
}
