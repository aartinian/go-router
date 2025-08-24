# go-router

A lightweight Go microservice that routes tasks to workers while maintaining session affinity.

## Features

- **Session Affinity**: Tasks from the same session always go to the same worker
- **Load Balancing**: Worker selection based on capacity and load
- **Auto Retry**: Failed messages automatically retry with configurable delays
- **Health Checks**: Built-in health endpoints for monitoring

## Quick Start

### Requirements

- Go 1.23+
- RabbitMQ
- Redis
- Docker (optional)

### Installation

```bash
# Clone the repository
git clone https://github.com/aartinian/go-router.git
cd go-router

# Download dependencies
go mod download

# Build
go build -o router ./cmd/router
```

### Configuration

Copy the example environment file and configure:

```bash
cp env.example .env
# Edit .env with your RabbitMQ and Redis settings
```

### Run

```bash
# Start the router
./router

# Or with Docker
docker build -t go-router .
docker run --env-file .env go-router
```

### Health Check

```bash
curl http://localhost:8080/healthz
```

## Message Format

Send JSON messages to your configured queue:

```json
{
  "session_id": "user-123",
  "payload": "your task data"
}
```

## License

MIT License - see [LICENSE](LICENSE) file for details.