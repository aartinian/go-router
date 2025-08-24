package router

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Configuration structs
type ExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type BindingConfig struct {
	QueueName    string
	RoutingKey   string
	ExchangeName string
	NoWait       bool
	Args         amqp.Table
}

// Configuration variables
var (
	// Default exchange configuration
	DefaultExchangeConfig = ExchangeConfig{
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
	}

	// Default queue configuration
	DefaultQueueConfig = QueueConfig{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
	}

	// Default binding configuration
	DefaultBindingConfig = BindingConfig{
		NoWait: false,
	}

	// Message configuration
	MessageContentType  = "application/json"
	MessageDeliveryMode = amqp.Persistent

	// QoS configuration
	QosPrefetchSize = 0     // No limit on message size
	QosGlobal       = false // Apply QoS per channel, not globally

	// Consumer configuration
	ConsumerTagAuto   = ""    // Empty string means auto-generated tag
	ConsumerAutoAck   = false // Manual acknowledgment
	ConsumerExclusive = false // Not exclusive to connection
	ConsumerNoLocal   = false // Can receive messages published by same connection
	ConsumerNoWait    = false // Wait for server confirmation

	// Publisher configuration
	PublishMandatory = false // Don't return unroutable messages
	PublishImmediate = false // Don't wait for confirmation
)

// MessageBroker abstracts RabbitMQ operations, providing methods for connection
// management, message consumption and publishing, and topology declaration.
type MessageBroker interface {
	// Connection management
	Connect(ctx context.Context) error
	Close() error
	IsConnected() bool

	// Message operations
	Consume(ctx context.Context, queueName string, handler MessageHandler) error
	Publish(ctx context.Context, exchange, routingKey string, body []byte) error

	// Infrastructure setup
	DeclareTopology(ctx context.Context) error
}

// RabbitMQ implements the MessageBroker interface for RabbitMQ.
// It manages AMQP connections, topology declaration, and message operations.
type RabbitMQ struct {
	url    string
	config Config
	conn   *amqp.Connection
	mutex  sync.RWMutex
	logger zerolog.Logger
}

// NewRabbitMQ creates a new RabbitMQ broker with the given configuration.
func NewRabbitMQ(url string, config Config, logger zerolog.Logger) *RabbitMQ {
	return &RabbitMQ{
		url:    url,
		config: config,
		logger: logger,
	}
}

// Connect establishes a connection to RabbitMQ using the configured URL.
func (r *RabbitMQ) Connect(ctx context.Context) error {
	// Lock the mutex to prevent concurrent connections
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var conn *amqp.Connection
	var err error

	if strings.HasPrefix(r.url, "amqps://") {
		conn, err = r.connectWithSSL()
	} else {
		conn, err = amqp.Dial(r.url)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	r.conn = conn

	// Sanitize URL for logs (remove credentials)
	loggedURL := r.url
	if u, parseErr := url.Parse(r.url); parseErr == nil {
		u.User = nil
		loggedURL = u.String()
	}
	r.logger.Info().Str("url", loggedURL).Msg("Connected to RabbitMQ")
	return nil
}

// connectWithSSL establishes an SSL connection to RabbitMQ using client certificates
func (r *RabbitMQ) connectWithSSL() (*amqp.Connection, error) {
	parsed, err := url.Parse(r.url)
	if err != nil {
		return nil, fmt.Errorf("invalid RabbitMQ URL: %w", err)
	}

	tlsConfig, err := r.buildTLSConfig(parsed)
	if err != nil {
		return nil, err
	}

	sasl := r.selectSASL(parsed)

	cfg := amqp.Config{
		TLSClientConfig: tlsConfig,
		SASL:            sasl,
		Heartbeat:       30 * time.Second,
	}

	conn, err := amqp.DialConfig(parsed.String(), cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to establish SSL connection: %w", err)
	}
	return conn, nil
}

func (r *RabbitMQ) buildTLSConfig(parsed *url.URL) (*tls.Config, error) {
	// Load client certificates
	cert, err := tls.LoadX509KeyPair(r.config.CertFile, r.config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificates: %w", err)
	}
	// Load CA certificate
	caCert, err := os.ReadFile(r.config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA certificate to pool")
	}

	cfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: !r.config.TLSVerify,
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS13,
	}
	if r.config.TLSVerify {
		if host := parsed.Hostname(); host != "" {
			cfg.ServerName = host
		}
	}
	return cfg, nil
}

func (r *RabbitMQ) selectSASL(parsed *url.URL) []amqp.Authentication {
	auth := strings.ToLower(strings.TrimSpace(r.config.RabbitAuth))
	switch auth {
	case "plain":
		if parsed.User != nil {
			username := parsed.User.Username()
			password, _ := parsed.User.Password()
			r.logger.Info().
				Str("auth", "plain").
				Str("username", username).
				Msg("Using RabbitMQ TLS with PLAIN auth")
			return []amqp.Authentication{&amqp.PlainAuth{Username: username, Password: password}}
		}
		r.logger.Info().
			Str("auth", "plain").
			Msg("Using RabbitMQ TLS with PLAIN auth (guest default)")
		return []amqp.Authentication{&amqp.PlainAuth{Username: "guest", Password: "guest"}}
	case "external":
		parsed.User = nil
		r.logger.Info().
			Str("auth", "external").
			Msg("Using RabbitMQ TLS with EXTERNAL auth")
		return []amqp.Authentication{&amqp.ExternalAuth{}}
	default:
		if parsed.User != nil {
			username := parsed.User.Username()
			password, _ := parsed.User.Password()
			r.logger.Info().
				Str("auth", "plain").
				Str("username", username).
				Msg("Using RabbitMQ TLS with PLAIN auth (auto)")
			return []amqp.Authentication{&amqp.PlainAuth{Username: username, Password: password}}
		}
		parsed.User = nil
		r.logger.Info().
			Str("auth", "external").
			Msg("Using RabbitMQ TLS with EXTERNAL auth (auto)")
		return []amqp.Authentication{&amqp.ExternalAuth{}}
	}
}

// Close gracefully closes the RabbitMQ connection.
func (r *RabbitMQ) Close() error {
	// Lock the mutex to prevent concurrent connections
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Close the connection
	if r.conn != nil && !r.conn.IsClosed() {
		if err := r.conn.Close(); err != nil {
			r.logger.Error().Err(err).Msg("Error closing RabbitMQ connection")
			return err
		}
	}

	r.conn = nil
	r.logger.Info().Msg("RabbitMQ connection closed")
	return nil
}

// IsConnected returns true if the adapter has an active RabbitMQ connection.
func (r *RabbitMQ) IsConnected() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.conn != nil && !r.conn.IsClosed()
}

// DeclareTopology creates the required exchanges, queues, and bindings.
func (r *RabbitMQ) DeclareTopology(ctx context.Context) error {
	r.mutex.RLock()
	conn := r.conn
	r.mutex.RUnlock()
	if conn == nil {
		return fmt.Errorf("not connected to RabbitMQ")
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to get channel for topology: %w", err)
	}
	defer ch.Close()

	// Declare exchanges
	if err := r.declareExchanges(
		ch, r.config.IncomingQueue, r.config.WorkerExchange,
	); err != nil {
		return err
	}

	// Declare queues with retry logic
	if err := r.declareQueues(ch, r.config.IncomingQueue); err != nil {
		return err
	}

	// Setup bindings
	if err := r.setupBindings(ch, r.config.IncomingQueue); err != nil {
		return err
	}

	return nil
}

// declareExchanges declares all required exchanges.
func (r *RabbitMQ) declareExchanges(
	ch *amqp.Channel,
	incomingQueue string,
	workerExchange string,
) error {
	// NOTE: All exchanges use "direct" type
	exchangeNames := []string{
		workerExchange,         // routes to specific workers using workerID as routing key
		incomingQueue + ".rtx", // used as DLX with specific routing keys for retry logic
		incomingQueue + ".dlx", // used as DLX with empty routing key for critical errors
	}

	for _, name := range exchangeNames {
		config := ExchangeConfig{
			Name:       name,
			Type:       DefaultExchangeConfig.Type,
			Durable:    DefaultExchangeConfig.Durable,
			AutoDelete: DefaultExchangeConfig.AutoDelete,
			Internal:   DefaultExchangeConfig.Internal,
			NoWait:     DefaultExchangeConfig.NoWait,
		}
		if err := r.declareExchange(ch, config); err != nil {
			return err
		}
	}

	return nil
}

// declareExchange declares a single exchange using the provided configuration.
func (r *RabbitMQ) declareExchange(
	ch *amqp.Channel,
	config ExchangeConfig,
) error {
	err := ch.ExchangeDeclare(
		config.Name,
		config.Type,
		config.Durable,
		config.AutoDelete,
		config.Internal,
		config.NoWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange %s: %w", config.Name, err)
	}
	return nil
}

// declareQueues creates all required queues with retry capabilities.
func (r *RabbitMQ) declareQueues(ch *amqp.Channel, queueName string) error {
	queueConfigs := []struct {
		name string
		args amqp.Table
	}{
		{
			name: queueName,
			args: amqp.Table{
				"x-dead-letter-exchange":    queueName + ".rtx",
				"x-dead-letter-routing-key": queueName + ".rtq",
			},
		},
		{
			name: queueName + ".rtq",
			args: amqp.Table{
				"x-dead-letter-exchange":    queueName + ".rtx",
				"x-dead-letter-routing-key": queueName,
				"x-message-ttl":             r.config.RetryTTL,
			},
		},
		{
			name: queueName + ".dlq",
			args: nil, // No DLX - final destination
		},
	}

	for _, qc := range queueConfigs {
		config := QueueConfig{
			Name:       qc.name,
			Durable:    DefaultQueueConfig.Durable,
			AutoDelete: DefaultQueueConfig.AutoDelete,
			Exclusive:  DefaultQueueConfig.Exclusive,
			NoWait:     DefaultQueueConfig.NoWait,
			Args:       qc.args,
		}
		if err := r.declareQueue(ch, config); err != nil {
			return err
		}
	}

	return nil
}

// declareQueue declares a single queue using the provided configuration.
func (r *RabbitMQ) declareQueue(ch *amqp.Channel, config QueueConfig) error {
	_, err := ch.QueueDeclare(
		config.Name,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", config.Name, err)
	}
	return nil
}

// setupBindings creates all required queue and exchange bindings.
func (r *RabbitMQ) setupBindings(ch *amqp.Channel, queueName string) error {
	bindingConfigs := []struct {
		queueName    string
		routingKey   string
		exchangeName string
	}{
		{
			// Binding: Main queue → Retry exchange
			// Scenario: When a message is NACKed from the main queue, it goes to the retry exchange
			// with routing key "queueName", which then routes it to the retry queue
			queueName:    queueName,
			routingKey:   queueName,
			exchangeName: queueName + ".rtx",
		},
		{
			// Binding: Retry queue → Retry exchange
			// Scenario: When retry queue TTL expires, the message goes to the retry exchange
			// with routing key "queueName.rtq", which then routes it back to the main queue
			queueName:    queueName + ".rtq",
			routingKey:   queueName + ".rtq",
			exchangeName: queueName + ".rtx",
		},
		{
			// Binding: DLQ → Critical exchange
			// Scenario: When critical errors occur, they are published to the critical exchange
			// with empty routing key "", which then routes them to the final DLQ
			queueName:    queueName + ".dlq",
			routingKey:   "", // Empty routing key for critical errors
			exchangeName: queueName + ".dlx",
		},
	}

	for _, bc := range bindingConfigs {
		config := BindingConfig{
			QueueName:    bc.queueName,
			RoutingKey:   bc.routingKey,
			ExchangeName: bc.exchangeName,
			NoWait:       DefaultBindingConfig.NoWait,
			Args:         nil,
		}
		if err := r.createBinding(ch, config); err != nil {
			return err
		}
	}

	return nil
}

// createBinding creates a single queue-to-exchange binding using the provided
// configuration.
func (r *RabbitMQ) createBinding(ch *amqp.Channel, config BindingConfig) error {
	err := ch.QueueBind(
		config.QueueName,
		config.RoutingKey,
		config.ExchangeName,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue %s to exchange %s: %w",
			config.QueueName, config.ExchangeName, err)
	}
	return nil
}

// Publish sends a message to the specified exchange with the given routing key.
func (r *RabbitMQ) Publish(
	ctx context.Context,
	exchange, routingKey string,
	body []byte,
) error {
	r.mutex.RLock()
	conn := r.conn
	r.mutex.RUnlock()
	if conn == nil {
		return fmt.Errorf("not connected to RabbitMQ")
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to get channel for publishing: %w", err)
	}
	defer ch.Close()

	pub := amqp.Publishing{
		ContentType:  MessageContentType,
		Body:         body,
		DeliveryMode: MessageDeliveryMode,
		Timestamp:    time.Now(),
	}
	err = ch.PublishWithContext(
		ctx, exchange, routingKey, PublishMandatory, PublishImmediate, pub,
	)
	if err != nil {
		r.logger.Warn().
			Str("exchange", exchange).
			Str("routing_key", routingKey).
			Int("size", len(body)).
			Err(err).
			Msg("Publish failed")
		return err
	}

	r.logger.Debug().
		Str("exchange", exchange).
		Str("routing_key", routingKey).
		Int("size", len(body)).
		Msg("Published message")
	return nil
}

// Consume starts consuming messages from the specified queue using a worker
// pool.
func (r *RabbitMQ) Consume(
	ctx context.Context, queueName string, handler MessageHandler,
) error {
	r.mutex.RLock()
	conn := r.conn
	r.mutex.RUnlock()
	if conn == nil {
		return fmt.Errorf("not connected to RabbitMQ")
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to get channel for consumption: %w", err)
	}
	defer ch.Close()

	if err := ch.Qos(
		r.config.ProcsPoolSize, QosPrefetchSize, QosGlobal,
	); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := ch.Consume(
		queueName,
		ConsumerTagAuto,
		ConsumerAutoAck,
		ConsumerExclusive,
		ConsumerNoLocal,
		ConsumerNoWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	// Start worker pool
	r.startWorkerPool(ctx, msgs, handler, queueName)

	// Wait for either context cancellation or connection loss
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.conn.NotifyClose(make(chan *amqp.Error)):
		return fmt.Errorf("RabbitMQ connection lost")
	}
}

// startWorkerPool creates and manages the worker goroutines for message
// processing.
func (r *RabbitMQ) startWorkerPool(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	handler MessageHandler,
	queueName string,
) {
	// Create a context that can be cancelled to stop all workers
	var wg sync.WaitGroup
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < r.config.ProcsPoolSize; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r.consumeWorker(workerCtx, msgs, handler, workerID, queueName)
		}(i)
	}

	// Wait for either context cancellation or all workers to finish
	go func() {
		wg.Wait()
		// Signal other workers to stop
		cancel()
	}()

	<-workerCtx.Done()
}

// consumeWorker processes messages from the message channel using the provided
// handler.
func (r *RabbitMQ) consumeWorker(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	handler MessageHandler,
	workerID int,
	queueName string,
) {
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				// Channel closed, connection might be lost
				r.logger.Error().Msg("Message channel closed, connection lost")
				return
			}

			if err := handler(ctx, msg); err != nil {
				r.logger.Error().Err(err).
					Int("worker_id", workerID).
					Str("queue", queueName).
					Msg("Message processing failed")

				// Note: Message acknowledgment (ACK/NACK) is handled by the handler
				// - Success: handler calls msg.Ack() in routeToWorker
				// - Critical errors: handler calls msg.Ack() after sending to DLQ
				// - Recoverable errors: handler calls msg.Nack() to trigger retry
			}

		case <-ctx.Done():
			return
		}
	}
}

// MessageHandler processes individual messages consumed from RabbitMQ queues.
// It receives a context for cancellation and the message to process.
// Returning an error will cause the message to be rejected.
type MessageHandler func(ctx context.Context, msg amqp.Delivery) error
