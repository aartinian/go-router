// Command router implements a router service for distributing tasks to
// worker instances.
//
// The router service is responsible for listening for tasks on RabbitMQ
// queues and routing them to appropriate workers based on capacity and
// session affinity.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aartinian/go-router/internal/router"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// main is the entry point of the application; it creates all dependencies,
// wires them together, and manages the service lifecycle.
func main() {
	// Load environment variables and configuration
	if err := godotenv.Load(); err != nil {
		log.Warn().Err(err).Msg(
			"Failed to load .env file, continuing with environment variables",
		)
	}
	config := router.LoadConfig()

	// Create console writer with custom formatting
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
	}

	// Apply log level from config before creating final logger
	zerolog.SetGlobalLevel(config.LogLevel)
	// Create base logger with console output
	baseLogger := log.Output(consoleWriter).Level(config.LogLevel)
	// Add service context and create final logger
	logger := baseLogger.With().Str("service", "router").Logger()

	// Create dependencies using dependency injection
	rabbitmq := router.NewRabbitMQ(config.RabbitURL, config, logger)
	registry := router.NewRedisRegistry(
		config.RedisURL,
		config.HeartbeatsKey,
		config.SessionTTL,
		logger,
	)

	// Router logic components
	loadBalancer := router.NewWeightedBalancer(logger)
	taskHandler := router.NewTaskHandler(
		registry, rabbitmq, logger, loadBalancer, config,
	)

	// Create and wire the router service
	svc := router.NewRouterService(
		config, rabbitmq, registry, loadBalancer, taskHandler.Handle, logger,
	)

	// Create cancellable context for (graceful) shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for shutdown signals in a separate goroutine
	go handleShutdownSignals(cancel, logger)

	// Start the service
	logger.Info().Msg("Starting router service...")
	if err := svc.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Failed to start router service")
	}

	// Wait for shutdown signal
	svc.Wait()

	// Perform graceful shutdown with timeout
	shutdownTimeout := 10 * time.Second
	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer shutdownCancel()

	if err := svc.Stop(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("Error during graceful shutdown")
	}

	logger.Info().Msg("Router service terminated gracefully")
}

// handleShutdownSignals listens for shutdown signals and triggers graceful
// shutdown.
func handleShutdownSignals(cancel context.CancelFunc, logger zerolog.Logger) {
	// Create a channel to listen for shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait here until signal arrives
	<-sigChan

	logger.Warn().Msg(
		"Shutdown signal received, initiating graceful shutdown...",
	)
	cancel()
}
