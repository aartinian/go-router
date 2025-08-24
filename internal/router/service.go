// Package router implements a capacity-aware, session-affine task router.
//
// The service consumes task messages from RabbitMQ, maintains session->worker
// assignment in Redis, and forwards messages to workers via a direct exchange
// keyed by worker_id. See README and docs for details.
package router

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// Service configuration constants
const (
	ConnectionRetryDelay = time.Second * 2
	ConnectionMaxDelay   = time.Second * 30
	HealthCheckInterval  = time.Second * 10
	ShutdownTimeout      = time.Second * 10
)

// IsReadyFlag is a thread-safe wrapper around atomic.Bool for readiness
// tracking, used by the health check server.
type IsReadyFlag struct{ v atomic.Bool }

// Load returns the current readiness state.
func (f *IsReadyFlag) Load() bool { return f.v.Load() }

// Store sets the readiness state to the given boolean value.
func (f *IsReadyFlag) Store(b bool) { f.v.Store(b) }

// ConnectionManager handles connections to external services with retry logic.
type ConnectionManager struct {
	broker   MessageBroker
	registry WorkerRegistry
	logger   zerolog.Logger
	backoff  *exponentialBackoff
}

// NewConnectionManager creates a new connection manager.
func NewConnectionManager(
	broker MessageBroker,
	registry WorkerRegistry,
	logger zerolog.Logger,
) *ConnectionManager {
	return &ConnectionManager{
		broker:   broker,
		registry: registry,
		logger:   logger,
		backoff: newExponentialBackoff(
			ConnectionRetryDelay,
			ConnectionMaxDelay,
		),
	}
}

// Connect establishes connections to all external services.
func (cm *ConnectionManager) Connect(ctx context.Context) error {
	// Connect to message broker
	if err := cm.broker.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}

	// Connect to session registry
	if err := cm.registry.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to registry: %w", err)
	}

	// Setup broker topology
	if err := cm.broker.DeclareTopology(ctx); err != nil {
		return fmt.Errorf("failed to setup broker topology: %w", err)
	}

	cm.logger.Info().Msg("Successfully connected to external services")
	return nil
}

// Disconnect closes all external connections.
func (cm *ConnectionManager) Disconnect() {
	if cm.broker != nil {
		cm.broker.Close()
	}

	if cm.registry != nil {
		cm.registry.Close()
	}
}

// IsHealthy checks if all connections are healthy.
func (cm *ConnectionManager) IsHealthy(ctx context.Context) error {
	// Check Redis connection
	if err := cm.registry.Ping(ctx); err != nil {
		return fmt.Errorf("Redis connection health check failed: %w", err)
	}

	// Check RabbitMQ connection
	if !cm.broker.IsConnected() {
		return fmt.Errorf("RabbitMQ connection health check failed")
	}

	return nil
}

// NextRetryDelay returns the next retry delay and increments the attempt
// counter.
func (cm *ConnectionManager) NextRetryDelay() time.Duration {
	return cm.backoff.Next()
}

// ResetBackoff resets the backoff attempt counter.
func (cm *ConnectionManager) ResetBackoff() {
	cm.backoff.Reset()
}

// MessageProcessor handles message consumption and processing.
type MessageProcessor struct {
	broker  MessageBroker
	handler MessageHandler
	queue   string
	logger  zerolog.Logger
}

// NewMessageProcessor creates a new message processor.
func NewMessageProcessor(
	broker MessageBroker,
	handler MessageHandler,
	queue string,
	logger zerolog.Logger,
) *MessageProcessor {
	return &MessageProcessor{
		broker:  broker,
		handler: handler,
		queue:   queue,
		logger:  logger,
	}
}

// Start begins consuming messages from the queue.
func (mp *MessageProcessor) Start(ctx context.Context) error {
	mp.logger.Info().Str("queue", mp.queue).Msg("Starting message processing")

	errCh := make(chan error, 1)

	// Start consuming messages
	go func() {
		err := mp.broker.Consume(ctx, mp.queue, mp.handler)
		if err != nil {
			mp.logger.Error().Err(err).Str("queue", mp.queue).Msg(
				"Message consumer failed",
			)
		}
		errCh <- err
	}()

	// Wait for consumer to fail or context to be cancelled
	select {
	case err := <-errCh:
		if err != nil {
			mp.logger.Error().Err(err).Msg("Message processing failed")
		}
		return err
	case <-ctx.Done():
		mp.logger.Info().Msg("Message processing cancelled")
		return ctx.Err()
	}
}

// RouterService coordinates all router functionality using dependency injection
// to work with different implementations of its dependencies.
type RouterService struct {
	config   Config
	broker   MessageBroker
	registry WorkerRegistry
	balancer Balancer
	logger   zerolog.Logger

	// Internal state
	isReady *IsReadyFlag
	health  *healthServer
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Other components
	connManager   *ConnectionManager
	msgProcessor  *MessageProcessor
	healthMonitor *HealthMonitor
}

// NewRouterService creates a new router service with the given dependencies.
func NewRouterService(
	config Config,
	broker MessageBroker,
	registry WorkerRegistry,
	Balancer Balancer,
	taskHandler MessageHandler,
	logger zerolog.Logger,
) *RouterService {
	connManager := NewConnectionManager(broker, registry, logger)
	msgProcessor := NewMessageProcessor(
		broker,
		taskHandler,
		config.IncomingQueue,
		logger,
	)
	healthMonitor := NewHealthMonitorWithConfig(connManager, logger)

	return &RouterService{
		config:        config,
		broker:        broker,
		registry:      registry,
		balancer:      Balancer,
		logger:        logger,
		isReady:       &IsReadyFlag{},
		connManager:   connManager,
		msgProcessor:  msgProcessor,
		healthMonitor: healthMonitor,
	}
}

// Start starts the router service and all its components.
func (svc *RouterService) Start(ctx context.Context) error {
	svc.logger.Info().Msg("Starting router service")

	// Create a cancellable context for internal operations
	ctx, svc.cancel = context.WithCancel(ctx)

	// Start health check server
	svc.health = NewHealthServer(
		svc.config.HealthCheckPort,
		svc.isReady,
		svc.logger,
	)
	svc.wg.Add(1)
	go func() {
		defer svc.wg.Done()
		if err := svc.health.Start(ctx); err != nil {
			svc.logger.Error().Err(err).Msg("Health server failed")
		}
	}()

	// Start the main service loop
	svc.wg.Add(1)
	go func() {
		defer svc.wg.Done()
		svc.runMainLoop(ctx)
	}()

	return nil
}

// Stop gracefully stops the router service.
func (svc *RouterService) Stop(ctx context.Context) error {
	svc.logger.Info().Msg("Stopping router service")

	if svc.cancel != nil {
		svc.cancel()
	}

	// Stop health monitoring
	if svc.healthMonitor != nil {
		svc.healthMonitor.Stop()
	}

	// Disconnect from external services
	if svc.connManager != nil {
		svc.connManager.Disconnect()
	}

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		svc.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		svc.logger.Info().Msg("Router service stopped gracefully")
	case <-ctx.Done():
		svc.logger.Warn().Msg("Router service stop timed out")
	}

	return nil
}

// Wait blocks until the service stops.
func (svc *RouterService) Wait() {
	svc.wg.Wait()
}

// IsReady returns whether the service is ready to handle requests.
func (svc *RouterService) IsReady() bool {
	return svc.isReady.Load()
}

// GetConfig returns the service configuration (for testing).
func (svc *RouterService) GetConfig() Config {
	return svc.config
}

// runMainLoop is the core resilience loop that manages connections and message
// processing.
func (svc *RouterService) runMainLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			svc.logger.Info().Msg("Main loop shutting down")
			return
		default:
		}

		// Try to connect to external services
		if err := svc.connManager.Connect(ctx); err != nil {
			svc.logger.Error().Err(err).Msg("Connection failed, retrying")
			svc.isReady.Store(false)
			svc.connManager.Disconnect()

			// Wait with exponential backoff before retry
			delay := svc.connManager.NextRetryDelay()
			svc.logger.Debug().Dur("delay", delay).Msg("Waiting before retry")

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
			continue
		}

		// Mark as ready and start message processing
		svc.isReady.Store(true)
		svc.connManager.ResetBackoff()

		// Start health monitoring in background
		svc.healthMonitor.Reset()
		svc.wg.Add(1)
		go func() {
			defer svc.wg.Done()
			if err := svc.healthMonitor.Start(ctx); err != nil {
				svc.logger.Warn().Err(err).Msg("Health monitoring failed")
			}
		}()

		// Start message processing (this will block until it fails or context
		// is cancelled)
		if err := svc.msgProcessor.Start(ctx); err != nil {
			svc.logger.Error().Err(err).Msg("Message processing failed")
		}

		// If we get here, message processing failed, so disconnect and retry
		svc.isReady.Store(false)
		svc.connManager.Disconnect()
		svc.healthMonitor.Stop()
	}
}
