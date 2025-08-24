package router

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	// MaxConsecutiveFailures is the maximum number of consecutive failures
	MaxConsecutiveFailures = 3
)

// HealthMonitor periodically checks the health of external service connections.
type HealthMonitor struct {
	connManager *ConnectionManager
	interval    time.Duration
	logger      zerolog.Logger
	stopCh      chan struct{}
	stopOnce    sync.Once
}

// NewHealthMonitor creates a new health monitor.
func NewHealthMonitor(
	connManager *ConnectionManager,
	interval time.Duration,
	logger zerolog.Logger,
) *HealthMonitor {
	return &HealthMonitor{
		connManager: connManager,
		interval:    interval,
		logger:      logger,
		stopCh:      make(chan struct{}),
		stopOnce:    sync.Once{},
	}
}

// NewHealthMonitorWithConfig creates a new health monitor with default
// configuration.
func NewHealthMonitorWithConfig(
	connManager *ConnectionManager, logger zerolog.Logger,
) *HealthMonitor {
	return NewHealthMonitor(connManager, HealthCheckInterval, logger)
}

// Start begins monitoring connections at the specified interval.
func (hm *HealthMonitor) Start(ctx context.Context) error {
	ticker := time.NewTicker(hm.interval)
	defer ticker.Stop()

	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hm.stopCh:
			return nil
		case <-ticker.C:
			if err := hm.connManager.IsHealthy(ctx); err != nil {
				consecutiveFailures++
				hm.logger.Warn().Err(err).
					Int("consecutive_failures", consecutiveFailures).
					Msg("Health check failed")

				// Only exit after multiple consecutive failures
				if consecutiveFailures >= MaxConsecutiveFailures {
					hm.logger.Error().
						Int("consecutive_failures", consecutiveFailures).
						Msg("Health monitor exiting due to consecutive failures")
					return err
				}
			} else {
				// Reset failure counter on success
				if consecutiveFailures > 0 {
					hm.logger.Info().
						Int("previous_failures", consecutiveFailures).
						Msg("Health check recovered")
					consecutiveFailures = 0
				}
			}
		}
	}
}

// Stop stops the health monitor.
func (hm *HealthMonitor) Stop() {
	hm.stopOnce.Do(func() { close(hm.stopCh) })
}

// Reset prepares the health monitor for another Start cycle.
func (hm *HealthMonitor) Reset() {
	// Reset stopOnce and create a new stop channel
	hm.stopOnce = sync.Once{}
	hm.stopCh = make(chan struct{})
}

type healthServer struct {
	port    string
	isReady *IsReadyFlag
	logger  zerolog.Logger
	server  *http.Server
}

// NewHealthServer creates a new health check server for Kubernetes probes.
func NewHealthServer(
	port string, isReady *IsReadyFlag, logger zerolog.Logger,
) *healthServer {
	return &healthServer{
		port:    port,
		isReady: isReady,
		logger:  logger,
	}
}

// Start starts the health check server with liveness and readiness endpoints.
func (hs *healthServer) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// Health endpoint (always returns OK)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Readiness endpoint (checks if service is ready)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if hs.isReady.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Ready"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Not Ready"))
		}
	})

	hs.server = &http.Server{
		Addr:    ":" + hs.port,
		Handler: mux,
	}

	// Start the health check server in a goroutine
	errCh := make(chan error, 1)
	go func() {
		if err := hs.server.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			errCh <- err
		} else {
			errCh <- nil
		}
	}()

	// Wait for the health check server to start or for the context to be done
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		ctx2, cancel := context.WithTimeout(
			context.Background(),
			ShutdownTimeout,
		)
		defer cancel()
		_ = hs.server.Shutdown(ctx2)
		return nil
	}
}
