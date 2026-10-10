package main

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	// SMTP
	SMTPHost string
	SMTPPort string

	// PostgreSQL
	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPassword string
	DBSSLMode  string

	// PostgreSQL Connection Pool
	DBMaxOpenConns    int
	DBMaxIdleConns    int
	DBConnMaxLifetime time.Duration
	DBConnMaxIdleTime time.Duration

	// Redis
	RedisHost         string
	RedisPort         string
	RedisPassword     string
	RedisPoolSize     int
	RedisMinIdleConns int
	RedisDialTimeout  time.Duration

	// Rate Limiting
	EmailRateLimit  int
	EmailRateWindow time.Duration
}

// LoadConfig reads environment variables (with .env fallback) and returns a Config.
func LoadConfig() *Config {
	// Load .env file if it exists; ignore error if missing
	_ = godotenv.Load()

	return &Config{
		SMTPHost: getEnv("SMTP_HOST", "localhost"),
		SMTPPort: getEnv("SMTP_PORT", "1025"),

		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnv("DB_PORT", "5433"),
		DBName:     getEnv("DB_NAME", "email_dispatcher"),
		DBUser:     getEnv("DB_USER", "postgres"),
		DBPassword: getEnv("DB_PASSWORD", "postgres"),
		DBSSLMode:  getEnv("DB_SSLMODE", "disable"),

		DBMaxOpenConns:    getEnvInt("DB_MAX_OPEN_CONNS", 10),
		DBMaxIdleConns:    getEnvInt("DB_MAX_IDLE_CONNS", 5),
		DBConnMaxLifetime: getEnvDuration("DB_CONN_MAX_LIFETIME", 30*time.Minute),
		DBConnMaxIdleTime: getEnvDuration("DB_CONN_MAX_IDLE_TIME", 5*time.Minute),

		RedisHost:         getEnv("REDIS_HOST", "localhost"),
		RedisPort:         getEnv("REDIS_PORT", "6379"),
		RedisPassword:     getEnv("REDIS_PASSWORD", ""),
		RedisPoolSize:     getEnvInt("REDIS_POOL_SIZE", 5),
		RedisMinIdleConns: getEnvInt("REDIS_MIN_IDLE_CONNS", 2),
		RedisDialTimeout:  getEnvDuration("REDIS_DIAL_TIMEOUT", 5*time.Second),

		EmailRateLimit:  getEnvInt("EMAIL_RATE_LIMIT", 10),
		EmailRateWindow: getEnvDuration("EMAIL_RATE_WINDOW", 1*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
