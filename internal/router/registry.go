package router

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
)

const (
	// sessionKeyPrefix is the prefix for all session keys in Redis
	sessionKeyPrefix = "session:"
)

// WorkerRegistry abstracts operations for managing worker assignments and
// heartbeats, including assigning sessions to workers, retrieving assignments,
// and unassigning sessions.
type WorkerRegistry interface {
	// Connection management
	Connect(ctx context.Context) error
	Close() error
	IsConnected() bool
	Ping(ctx context.Context) error

	// Operations
	GetWorker(ctx context.Context, sessionID string) (string, error)
	GetHeartbeats(ctx context.Context) (map[string]Heartbeat, error)
	AssignSession(ctx context.Context, sessionID, workerID string) error
	UnassignSession(ctx context.Context, sessionID string) error
	RenewSession(ctx context.Context, sessionID string) error
}

// Heartbeat represents a worker's current status including load, capacity,
// and timestamp.
type Heartbeat struct {
	Load      int       `json:"load"`
	Capacity  int       `json:"capacity"`
	Timestamp time.Time `json:"timestamp"`
}

// RedisRegistry implements the WorkerRegistry interface for Redis.
type RedisRegistry struct {
	url           string
	heartbeatsKey string
	sessionTTL    time.Duration
	logger        zerolog.Logger
	client        *redis.Client
}

// NewRedisRegistry creates a new Redis-based worker registry.
func NewRedisRegistry(
	url string,
	heartbeatsKey string,
	sessionTTL int,
	logger zerolog.Logger,
) *RedisRegistry {
	return &RedisRegistry{
		url:           url,
		heartbeatsKey: heartbeatsKey,
		sessionTTL:    time.Duration(sessionTTL) * time.Millisecond,
		logger:        logger,
	}
}

// Connect establishes a connection to Redis.
func (r *RedisRegistry) Connect(ctx context.Context) error {
	// Parse the Redis URL
	opt, err := redis.ParseURL(r.url)
	if err != nil {
		return fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	// Create a new Redis client
	r.client = redis.NewClient(opt)
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to ping Redis: %w", err)
	}

	r.logger.Info().Msg("Connected to Redis")
	return nil
}

// Close gracefully closes the Redis connection.
func (r *RedisRegistry) Close() error {
	if r.client != nil {
		if err := r.client.Close(); err != nil {
			r.logger.Error().Err(err).Msg("Failed to close Redis connection")
		}
		r.client = nil
	}
	return nil
}

// IsConnected returns true if the adapter has an active Redis connection.
func (r *RedisRegistry) IsConnected() bool {
	return r.client != nil && r.client.Ping(context.Background()).Err() == nil
}

// Ping tests the Redis connection.
func (r *RedisRegistry) Ping(ctx context.Context) error {
	// Initialized client check
	if r.client == nil {
		return fmt.Errorf("not connected to Redis")
	}
	return r.client.Ping(ctx).Err()
}

// GetWorker retrieves the worker ID assigned to a specific session.
func (r *RedisRegistry) GetWorker(
	ctx context.Context, sessionID string,
) (string, error) {
	var workerID string
	err := r.executeRedisOperation(
		func() error {
			// Check if session exists and hasn't expired
			sessionKey := fmt.Sprintf("%s%s", sessionKeyPrefix, sessionID)
			var err error
			workerID, err = r.client.Get(ctx, sessionKey).Result()
			return err
		},
		fmt.Sprintf("get assigned worker for session %s", sessionID),
	)

	// Handle special case: no worker assigned or session expired
	if err == redis.Nil {
		return "", fmt.Errorf(
			"no worker assigned to session %s or session expired",
			sessionID,
		)
	}
	// Return any other errors from executeRedisOperation
	if err != nil {
		return "", err
	}
	return workerID, nil
}

// GetHeartbeats retrieves all worker heartbeats by scanning for pattern.
func (r *RedisRegistry) GetHeartbeats(
	ctx context.Context,
) (map[string]Heartbeat, error) {
	// Scan for all keys matching the worker heartbeat pattern
	pattern := r.heartbeatsKey + "*"
	var allKeys []string
	var cursor uint64
	var err error

	// Use SCAN to get all matching keys
	for {
		var keys []string
		keys, cursor, err = r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan Redis keys: %w", err)
		}

		allKeys = append(allKeys, keys...)

		if cursor == 0 {
			break
		}
	}

	// If no keys found, return empty map
	if len(allKeys) == 0 {
		return make(map[string]Heartbeat), nil
	}

	// Fetch all heartbeat data
	result := make(map[string]Heartbeat, len(allKeys))
	for _, key := range allKeys {
		// Extract worker ID from key (e.g., "heartbeat:worker-1" -> "worker-1")
		workerID := strings.TrimPrefix(key, r.heartbeatsKey)

		// Get the heartbeat data
		hbJSON, err := r.client.Get(ctx, key).Result()
		if err != nil {
			if err == redis.Nil {
				// Key expired, skip it
				continue
			}
			r.logger.Warn().
				Err(err).
				Str("worker_id", workerID).
				Msg("Failed to get worker heartbeat, skipping")
			continue
		}

		var hb Heartbeat
		if err := json.Unmarshal([]byte(hbJSON), &hb); err != nil {
			r.logger.Warn().
				Err(err).
				Str("worker_id", workerID).
				Msg("Failed to unmarshal worker heartbeat, skipping")
			continue
		}

		result[workerID] = hb
	}

	return result, nil
}

// AssignSession stores a session-to-worker mapping with individual TTL
func (r *RedisRegistry) AssignSession(
	ctx context.Context, sessionID, workerID string,
) error {
	return r.executeRedisOperation(
		func() error {
			// Store each session as a separate key with TTL
			sessionKey := fmt.Sprintf("%s%s", sessionKeyPrefix, sessionID)
			return r.client.Set(ctx, sessionKey, workerID, r.sessionTTL).Err()
		},
		fmt.Sprintf("assign session %s to worker %s", sessionID, workerID),
	)
}

// UnassignSession removes a session-to-worker mapping from Redis.
func (r *RedisRegistry) UnassignSession(
	ctx context.Context, sessionID string,
) error {
	return r.executeRedisOperation(
		func() error {
			// Delete the session key
			sessionKey := fmt.Sprintf("%s%s", sessionKeyPrefix, sessionID)
			return r.client.Del(ctx, sessionKey).Err()
		},
		fmt.Sprintf("unassign session %s", sessionID),
	)
}

// RenewSession extends the TTL for an active session.
func (r *RedisRegistry) RenewSession(
	ctx context.Context, sessionID string,
) error {
	return r.executeRedisOperation(
		func() error {
			// Extend TTL for the specific session key
			sessionKey := fmt.Sprintf("%s%s", sessionKeyPrefix, sessionID)
			return r.client.Expire(ctx, sessionKey, r.sessionTTL).Err()
		},
		fmt.Sprintf("renew TTL for session %s", sessionID),
	)
}

// executeRedisOperation executes a Redis operation with proper error handling.
func (r *RedisRegistry) executeRedisOperation(
	operation func() error, operationName string,
) error {
	// Initialized client check
	if r.client == nil {
		return fmt.Errorf("not connected to Redis")
	}

	if err := operation(); err != nil {
		return fmt.Errorf("failed to %s: %w", operationName, err)
	}

	return nil
}
