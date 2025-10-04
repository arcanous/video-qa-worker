# Video Worker - Adapter-Based Architecture

A flexible, production-ready video processing worker with support for multiple job sources and storage backends.

## Architecture Overview

The video worker has been refactored to use the **Adapter Pattern**, making it highly flexible and extensible:

- **Job Sources**: Postgres, AWS SQS, Webhooks
- **Storage Backends**: Postgres, AWS S3, Hybrid
- **Configurable Processing**: Frame limits, pipeline steps, retry logic
- **OOP Design**: Clear separation of concerns, easy to test and maintain

## Key Features

### 🔄 **Adapter Pattern**
- Easy to swap job sources (Postgres → SQS → Webhooks)
- Flexible storage backends (Postgres → S3 → Hybrid)
- New adapters can be added without changing core logic

### ⚙️ **Configuration System**
- All settings controllable via environment variables
- Type-safe configuration with validation
- Backward compatible with existing deployments

### 🎯 **Configurable Frame Processing**
- `MAX_FRAMES_PER_VIDEO` environment variable
- Smart scene selection algorithm
- Always includes first and last scene
- Distributes remaining frames evenly

### 🏗️ **Clean Architecture**
- **VideoProcessor**: Encapsulates pipeline logic
- **PipelineOrchestrator**: Manages execution flow
- **WorkerService**: Main service coordination
- **Adapters**: Pluggable job sources and storage

## Quick Start

### Basic Usage (Postgres)

```bash
# Set required environment variables
export DATABASE_URL="postgresql://user:pass@localhost:5432/videoqa"
export OPENAI_API_KEY="your-openai-key"

# Optional: Configure frame limits
export MAX_FRAMES_PER_VIDEO=50

# Run the worker
python -m worker.run
```

### Advanced Configuration

```bash
# Job source configuration
export JOB_SOURCE_TYPE="postgres"  # postgres, sqs, webhook
export STORAGE_TYPE="postgres"     # postgres, s3, hybrid

# Processing configuration
export MAX_FRAMES_PER_VIDEO=100
export ENABLE_TRANSCRIPTION=true
export ENABLE_VISION_ANALYSIS=true
export ENABLE_EMBEDDINGS=true

# Worker configuration
export WORKER_POLL_MS=1500
export WORKER_MAX_ATTEMPTS=3
export LOG_LEVEL=INFO

# HTTP server (development)
export WORKER_DEV_HTTP=true
export WORKER_HTTP_PORT=8000
```

## Configuration Options

### Job Source Types

#### Postgres (Default)
```bash
export JOB_SOURCE_TYPE="postgres"
export DATABASE_URL="postgresql://user:pass@host:port/db"
export POSTGRES_POOL_SIZE=5
export POSTGRES_TIMEOUT=10
```

#### AWS SQS
```bash
export JOB_SOURCE_TYPE="sqs"
export AWS_SQS_QUEUE_URL="https://sqs.region.amazonaws.com/account/queue"
export AWS_REGION="us-east-1"
export SQS_MAX_MESSAGES=1
export SQS_WAIT_TIME=20
```

#### Webhook (Push-based)
```bash
export JOB_SOURCE_TYPE="webhook"
export WEBHOOK_URL="https://your-service.com/webhook"
export WEBHOOK_SECRET="your-secret"
export WEBHOOK_PORT=8080
```

### Storage Types

#### Postgres (Default)
```bash
export STORAGE_TYPE="postgres"
export DATABASE_URL="postgresql://user:pass@host:port/db"
```

#### AWS S3
```bash
export STORAGE_TYPE="s3"
export AWS_S3_BUCKET="your-bucket"
export AWS_REGION="us-east-1"
export S3_PREFIX="video-processing/"
```

#### Hybrid (Postgres + S3)
```bash
export STORAGE_TYPE="hybrid"
export METADATA_STORAGE="postgres"
export FILE_STORAGE="s3"
export DATABASE_URL="postgresql://user:pass@host:port/db"
export AWS_S3_BUCKET="your-bucket"
```

### Processing Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_FRAMES_PER_VIDEO` | 50 | Maximum frames to extract per video |
| `ENABLE_TRANSCRIPTION` | true | Enable audio transcription |
| `ENABLE_VISION_ANALYSIS` | true | Enable AI vision analysis |
| `ENABLE_EMBEDDINGS` | true | Enable embedding generation |
| `WORKER_POLL_MS` | 1500 | Polling interval in milliseconds |
| `WORKER_MAX_ATTEMPTS` | 3 | Maximum retry attempts |
| `LOG_LEVEL` | INFO | Logging level |

## Architecture Details

### File Structure

```
worker/
├── adapters/                 # Adapter implementations
│   ├── base.py              # Abstract base classes
│   ├── postgres_adapter.py  # Postgres job source & storage
│   ├── sqs_adapter.py       # AWS SQS job source
│   ├── s3_adapter.py        # AWS S3 storage
│   └── webhook_adapter.py   # Webhook job source
├── config.py                # Configuration management
├── models.py                 # Domain models
├── service.py                # Main worker service
├── processor.py              # Video processing pipeline
├── orchestrator.py           # Pipeline orchestration
├── pipeline/                 # Processing modules
│   ├── normalize.py
│   ├── transcribe.py
│   ├── scenes.py
│   ├── frames.py            # Updated with frame limits
│   ├── vision.py
│   └── embed.py
├── run.py                    # Entry point
└── http_server.py            # Health monitoring
```

### Class Hierarchy

```
WorkerService
├── JobSourceAdapter (interface)
│   ├── PostgresJobSourceAdapter
│   ├── SQSJobSourceAdapter
│   └── WebhookJobSourceAdapter
├── StorageAdapter (interface)
│   ├── PostgresStorageAdapter
│   └── S3StorageAdapter
├── PipelineOrchestrator
└── VideoProcessor
```

## Frame Processing

The worker now supports configurable frame extraction:

### Smart Scene Selection

When `MAX_FRAMES_PER_VIDEO < len(scenes)`:

1. **Always include first and last scene**
2. **Distribute remaining frames evenly** across video duration
3. **Maintain scene order** for consistent processing

### Example

```python
# Video with 100 scenes, MAX_FRAMES_PER_VIDEO=10
# Selected scenes: [0, 11, 22, 33, 44, 55, 66, 77, 88, 99]
# Always includes scene 0 (start) and scene 99 (end)
```

## Monitoring & Health Checks

### HTTP Endpoints (Development)

```bash
# Health check
curl http://localhost:8000/healthz

# Job queue status
curl http://localhost:8000/jobs/peek

# Worker statistics
curl http://localhost:8000/stats
```

### Statistics Available

- Jobs processed/failed
- Processing times
- Success rates
- Storage statistics
- Queue status

## Migration Guide

### From Legacy Worker

The new architecture is **backward compatible**:

1. **Existing environment variables** still work
2. **Default configuration** uses Postgres (same as before)
3. **No breaking changes** to external interfaces

### Migration Steps

1. **Update environment variables** (optional):
   ```bash
   # Old way (still works)
   export DATABASE_URL="postgresql://..."
   
   # New way (more explicit)
   export JOB_SOURCE_TYPE="postgres"
   export STORAGE_TYPE="postgres"
   export DATABASE_URL="postgresql://..."
   ```

2. **Add frame configuration** (optional):
   ```bash
   export MAX_FRAMES_PER_VIDEO=50
   ```

3. **Deploy and test** - no code changes required!

## Development

### Adding New Adapters

1. **Create adapter class** in `worker/adapters/`
2. **Implement base interfaces** (`JobSourceAdapter` or `StorageAdapter`)
3. **Add configuration** in `WorkerConfig`
4. **Update service factory** in `WorkerService._create_*_adapter()`

### Example: Redis Adapter

```python
# worker/adapters/redis_adapter.py
class RedisJobSourceAdapter(JobSourceAdapter):
    def claim_job(self) -> Optional[Job]:
        # Redis LPOP implementation
        pass
```

## Production Deployment

### Docker

```dockerfile
FROM python:3.11-slim
# ... existing Dockerfile
CMD ["python", "-m", "worker.run"]
```

### Environment Variables

```bash
# Production configuration
JOB_SOURCE_TYPE=postgres
STORAGE_TYPE=postgres
DATABASE_URL=postgresql://user:pass@db:5432/videoqa
MAX_FRAMES_PER_VIDEO=100
WORKER_POLL_MS=1000
LOG_LEVEL=INFO
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: video-worker
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: worker
        image: videoqa-worker:latest
        env:
        - name: JOB_SOURCE_TYPE
          value: "postgres"
        - name: STORAGE_TYPE
          value: "postgres"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: url
```

## Troubleshooting

### Common Issues

1. **Missing environment variables**:
   ```bash
   # Check required variables
   python -c "from worker.config import WorkerConfig; WorkerConfig.from_env().validate()"
   ```

2. **Adapter connection failures**:
   - Check network connectivity
   - Verify credentials
   - Review adapter-specific logs

3. **Frame processing issues**:
   - Verify `MAX_FRAMES_PER_VIDEO` setting
   - Check scene detection results
   - Review frame extraction logs

### Debug Mode

```bash
export LOG_LEVEL=DEBUG
export WORKER_DEV_HTTP=true
python -m worker.run
```

## Future Enhancements

- **Redis adapter** for high-performance job queues
- **Kafka adapter** for event streaming
- **Metrics adapters** (Prometheus, DataDog)
- **Distributed tracing** support
- **Rate limiting** and backpressure handling
- **Multi-region** deployment support

## License

[Your License Here]