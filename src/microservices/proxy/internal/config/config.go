package config

import (
	"os"
	"strconv"

	"golang.org/x/time/rate"
)

type Config struct {
	Port        string
	MetricsPort string
	JWTSecret   string

	MonolithURL      string
	MoviesServiceURL string
	EventsServiceURL string

	RedisURL     string
	KafkaBrokers string

	RateLimit  rate.Limit // ← тип rate.Limit
	RateBurst  int

	EnableGradualMigration bool
	MoviesMigrationPercent int
}

func Load() *Config {
	rateLimit, _ := strconv.Atoi(getEnv("RATE_LIMIT", "100"))
	rateBurst, _ := strconv.Atoi(getEnv("RATE_BURST", "200"))
	migrationPercent, _ := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50"))

	return &Config{
		Port:        getEnv("PORT", "8000"),
		MetricsPort: getEnv("METRICS_PORT", "8001"),
		JWTSecret:   getEnv("JWT_SECRET", "your-super-secret-key-change-me"),

		MonolithURL:      getEnv("MONOLITH_URL", "http://monolith:8080"),
		MoviesServiceURL: getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"),
		EventsServiceURL: getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"),

		RedisURL:     getEnv("REDIS_URL", "redis://redis:6379"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "kafka:9092"),

		RateLimit:  rate.Limit(rateLimit), // ← конвертация
		RateBurst:  rateBurst,

		EnableGradualMigration: getEnv("ENABLE_GRADUAL_MIGRATION", "true") == "true",
		MoviesMigrationPercent: migrationPercent,
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}