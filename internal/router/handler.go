package router

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// TaskMessage represents any task message
type TaskMessage struct {
	SessionID string         `json:"session_id"`
	TaskData  map[string]any `json:"-"`
}

// TaskHandler handles all types of tasks from a single queue.
type TaskHandler struct {
	registry WorkerRegistry
	broker   MessageBroker
	logger   zerolog.Logger
	balancer Balancer
	config   Config
}

// NewTaskHandler creates a new unified task handler.
func NewTaskHandler(
	registry WorkerRegistry,
	broker MessageBroker,
	logger zerolog.Logger,
	balancer Balancer,
	config Config,
) *TaskHandler {
	return &TaskHandler{
		registry: registry,
		broker:   broker,
		logger:   logger,
		balancer: balancer,
		config:   config,
	}
}

// Handle processes any type of task message.
func (h *TaskHandler) Handle(ctx context.Context, msg amqp.Delivery) error {
	// Extract session ID
	sessionID, err := h.extractSessionID(msg)
	if err != nil {
		return h.handleMessageError(msg, err)
	}

	// Get or assign worker
	workerID, err := h.getOrAssignWorker(ctx, sessionID)
	if err != nil {
		return h.handleMessageError(msg, err)
	}

	// Route message
	h.logger.Debug().
		Str("session_id", sessionID).
		Str("worker_id", workerID).
		Int("size", len(msg.Body)).
		Msg("Routing message to worker")
	return h.routeToWorker(ctx, msg, workerID, sessionID)
}

// getOrAssignWorker tries to get existing worker assignment or assigns a new
// one
func (h *TaskHandler) getOrAssignWorker(
	ctx context.Context, sessionID string,
) (string, error) {
	// Try existing assignment first
	if workerID, err := h.registry.GetWorker(ctx, sessionID); err == nil {
		// Renew TTL for active session
		if rsErr := h.registry.RenewSession(ctx, sessionID); rsErr != nil {
			h.logger.Warn().Err(rsErr).
				Str("session_id", sessionID).
				Str("worker_id", workerID).
				Msg("Failed to renew session TTL, will reassign to new worker")

			// Unassign from current worker and let the flow continue to get a new one
			if unassignErr := h.registry.UnassignSession(ctx, sessionID); unassignErr != nil {
				h.logger.Error().Err(unassignErr).
					Str("session_id", sessionID).
					Msg("Failed to unassign session after TTL renewal failure")
			}
			// Continue to the worker selection logic below
		} else {
			h.logger.Debug().
				Str("session_id", sessionID).
				Str("worker_id", workerID).
				Msg("Using existing session assignment and renewed TTL")
			return workerID, nil
		}
	}

	// Get available workers and select one
	workers, err := h.registry.GetHeartbeats(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get worker heartbeats: %w", err)
	}

	if len(workers) == 0 {
		return "", fmt.Errorf("no task workers found")
	}

	// Select worker using load balancer
	workerID, err := h.balancer.SelectWorker(ctx, workers)
	if err != nil {
		return "", fmt.Errorf("failed to select worker: %w", err)
	}

	// Assign session to selected worker
	if err := h.registry.AssignSession(ctx, sessionID, workerID); err != nil {
		return "", fmt.Errorf(
			"failed to assign session %s to worker %s: %w", sessionID, workerID, err,
		)
	}

	// Log new assignment for visibility at info level
	h.logger.Info().
		Str("session_id", sessionID).
		Str("worker_id", workerID).
		Msg("Assigned session to worker")

	return workerID, nil
}

// routeToWorker forwards a message to a specific worker
func (h *TaskHandler) routeToWorker(
	ctx context.Context,
	msg amqp.Delivery,
	workerID string,
	sessionID string,
) error {
	// Bound publish time to avoid hangs on broker issues
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Forward message to worker
	if err := h.broker.Publish(
		publishCtx, h.config.WorkerExchange, workerID, msg.Body,
	); err != nil {
		h.logger.Error().
			Err(err).
			Str("session_id", sessionID).
			Str("worker_id", workerID).
			Msg("Failed to publish message to worker")

		// Rollback session assignment on publish failure
		if rollbackErr := h.registry.UnassignSession(
			ctx, sessionID,
		); rollbackErr != nil {
			h.logger.Error().
				Err(rollbackErr).
				Str("session_id", sessionID).
				Msg("Failed to rollback session assignment")
		}

		return h.handleMessageError(
			msg, fmt.Errorf("failed to publish message to worker: %w", err),
		)
	}

	// Acknowledge successful processing
	if err := msg.Ack(false); err != nil {
		h.logger.Error().Err(err).Msg("Failed to acknowledge message")
	}

	h.logger.Debug().
		Str("session_id", sessionID).
		Str("worker_id", workerID).
		Msg("Message routed and ACKed")

	return nil
}

// extractSessionID extracts session ID from the message
func (h *TaskHandler) extractSessionID(msg amqp.Delivery) (string, error) {
	// Check for empty message body
	if len(msg.Body) == 0 {
		return "", fmt.Errorf("empty message body")
	}

	var taskData map[string]any
	if err := json.Unmarshal(msg.Body, &taskData); err != nil {
		return "", fmt.Errorf("failed to unmarshal task message: %w", err)
	}

	// Check for nil data after unmarshaling
	if taskData == nil {
		return "", fmt.Errorf("message unmarshaled to nil data")
	}

	sessionID := h.extractSessionIDFromData(taskData)
	if sessionID == "" {
		return "", fmt.Errorf(
			"missing or invalid session ID in field: %s",
			h.config.SessionIdField,
		)
	}

	return sessionID, nil
}

// extractSessionIDFromData extracts session ID from parsed data
func (h *TaskHandler) extractSessionIDFromData(data map[string]any) string {
	if data == nil {
		return ""
	}

	sessionID, exists := data[h.config.SessionIdField]
	if !exists {
		return ""
	}

	switch v := sessionID.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	default:
		return ""
	}
}

// handleMessageError handles message processing errors by sending them to the
// appropriate destination.
// - critical errors (malformed JSON, parsing errors) go to the final DLQ.
// - recoverable errors (no workers available) go to the retry queue.
func (h *TaskHandler) handleMessageError(msg amqp.Delivery, err error) error {
	// Defensive: if err is nil, treat as recoverable
	if err == nil {
		h.logger.Warn().
			Int("size", len(msg.Body)).
			Msg("handleMessageError called with nil error; NACKing for retry")
		return h.sendToRetryQueue(msg)
	}

	critical := h.isCriticalError(err)
	class := "recoverable"
	event := h.logger.Warn()
	if critical {
		class = "critical"
		event = h.logger.Error()
	}

	event.Err(err).
		Int("size", len(msg.Body)).
		Str("classification", class).
		Msg("Message processing error")

	if critical {
		return h.sendToCriticalDLQ(msg)
	}
	return h.sendToRetryQueue(msg)
}

// sendToCriticalDLQ sends critical errors directly to the final DLQ via the critical exchange.
// This bypasses the retry mechanism for errors that should never be retried.
func (h *TaskHandler) sendToCriticalDLQ(msg amqp.Delivery) error {
	h.logger.Warn().
		Int("size", len(msg.Body)).
		Msg("Critical error detected, sending to final DLQ")

	// Use a timeout context to prevent hanging indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.broker.Publish(
		ctx, h.config.IncomingQueue+".dlx", "", msg.Body,
	); err != nil {
		h.logger.Error().Err(err).
			Msg("Failed to publish to critical DLX, falling back to NACK")
	}

	// For critical errors, we MUST NOT retry - either succeed or lose the message
	return msg.Ack(false)
}

// sendToRetryQueue triggers the retry mechanism via RabbitMQ DLX topology.
// When we NACK the message, RabbitMQ automatically routes it to the retry queue
// based on the DLX configuration. After the TTL expires, the message returns
// to the main queue for retry.
func (h *TaskHandler) sendToRetryQueue(msg amqp.Delivery) error {
	h.logger.Warn().
		Int("size", len(msg.Body)).
		Int("retry_ttl_ms", h.config.RetryTTL).
		Msg("Recoverable error detected, NACKing message to trigger retry")
	return msg.Nack(false, false)
}

// isCriticalError determines if an error is critical and should bypass retry.
func (h *TaskHandler) isCriticalError(err error) bool {
	if err == nil {
		return false
	}

	errorMsg := err.Error()

	// Critical errors that should never retry
	criticalPatterns := []string{
		"failed to unmarshal task message",       // Custom error
		"missing or invalid session ID in field", // Custom error
		"empty message body",                     // Custom error
		"message unmarshaled to nil data",        // Custom error
		"invalid character",                      // JSON parsing
		"unexpected end of JSON input",           // JSON parsing
		"unexpected end of input",                // JSON parsing
		"invalid syntax",                         // JSON parsing
	}

	for _, pattern := range criticalPatterns {
		if strings.Contains(errorMsg, pattern) {
			return true
		}
	}

	return false
}
