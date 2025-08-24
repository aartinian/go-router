package router

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

// Config holds all configuration values for the router service.
// Values are loaded from environment variables with sensible defaults.
type Config struct {
	// Service related
	IncomingQueue   string        // Main tasks queue name
	WorkerExchange  string        // Exchange for routing to workers
	SessionIdField  string        // Field name for session identification
	HeartbeatsKey   string        // Redis key for worker heartbeats
	RetryTTL        int           // Retry message TTL in milliseconds
	SessionTTL      int           // Session TTL in milliseconds
	ProcsPoolSize   int           // Number of message processing goroutines
	HealthCheckPort string        // HTTP port for health checks
	LogLevel        zerolog.Level // Log level

	// Redis related
	RedisURL string // Redis connection string

	// RabbitMQ related
	RabbitURL  string // RabbitMQ connection string
	RabbitAuth string // Auth method: external|plain (optional)
	TLSVerify  bool   // Whether to verify TLS certificates
	CAFile     string // CA certificate file
	CertFile   string // Client certificate file
	KeyFile    string // Client key file
}

// LoadConfig reads configuration from environment variables and returns a
// Config struct.
func LoadConfig() Config {
	config := Config{
		IncomingQueue:   getEnv("QUEUE_INCOMING", "router.incoming"),
		WorkerExchange:  getEnv("EXCHANGE_WORKER", "worker.incoming"),
		SessionIdField:  getEnv("SESSION_ID_FIELD", "session_id"),
		HeartbeatsKey:   getEnv("HEARTBEAT_KEY_PRX", "heartbeat:"),
		RetryTTL:        getEnvAsInt("RETRY_TTL_MS", 30000),
		SessionTTL:      getEnvAsInt("SESSION_TTL_MS", 30000),
		ProcsPoolSize:   getEnvAsInt("PROCS_POOL_SIZE", 10),
		HealthCheckPort: getEnv("HEALTH_CHECK_PORT", "8080"),
		LogLevel:        getLogLevel(getEnv("LOG_LEVEL", "info")),
		RedisURL:        resolveRedisURL(),
		RabbitURL:       resolveRabbitURL(),
		RabbitAuth:      getEnv("RABBITMQ_AUTH", ""),
		TLSVerify:       getEnvAsBool("TLS_VERIFY", false),
		CAFile:          getEnv("TLS_CA_CERT", ""),
		CertFile:        getEnv("TLS_CLIENT_CERT", ""),
		KeyFile:         getEnv("TLS_CLIENT_KEY", ""),
	}

	return config
}

// resolveRedisURL returns explicit REDIS_URL
func resolveRedisURL() string {
	if url := getEnv("REDIS_URL", ""); url != "" {
		return url
	}
	host := getEnv("REDIS_HOST", "localhost")
	port := getEnv("REDIS_PORT", "6379")
	// Default DB 0; todo: add REDIS_DB
	return fmt.Sprintf("redis://%s:%s/0", host, port)
}

// resolveRabbitURL returns explicit RABBITMQ_URL
func resolveRabbitURL() string {
	if url := getEnv("RABBITMQ_URL", ""); url != "" {
		return url
	}
	return buildRabbitURLFromParts()
}

func buildRabbitURLFromParts() string {
	ssl := getEnvAsBool("RABBITMQ_TLS", false)
	host := getEnv("RABBITMQ_HOST", "localhost")
	port := getEnv("RABBITMQ_PORT", "")
	if port == "" {
		if ssl {
			port = "5671"
		} else {
			port = "5672"
		}
	}

	vhost := getEnv("RABBITMQ_VHOST", "/")
	path := vhost
	if path != "/" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if ssl {
		// With TLS, prefer EXTERNAL auth via client certs
		return fmt.Sprintf("amqps://%s:%s%s", host, port, path)
	}

	user := getEnv("RABBITMQ_USERNAME", "guest")
	pass := getEnv("RABBITMQ_PASSWORD", "guest")
	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", user, pass, host, port, path)
}

// getEnv retrieves an environment variable by key, returning a fallback value
// if not found.
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// getEnvAsInt retrieves an environment variable as an integer, returning a
// fallback value if not found or invalid.
func getEnvAsInt(key string, fallback int) int {
	if val := getEnv(key, ""); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return fallback
}

// getEnvAsBool retrieves an environment variable as a boolean, returning a
// fallback value if not found or invalid.
func getEnvAsBool(key string, fallback bool) bool {
	if val := getEnv(key, ""); val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			return boolVal
		}
	}
	return fallback
}

// getLogLevel parses a log level string and returns the corresponding
// zerolog.Level.
func getLogLevel(levelStr string) zerolog.Level {
	level, err := zerolog.ParseLevel(levelStr)
	if err != nil {
		return zerolog.InfoLevel
	}
	return level
}
