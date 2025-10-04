# Video Worker

A Python worker that processes video files using OpenAI APIs and stores results in PostgreSQL.

## Features

- Polls PostgreSQL for pending jobs using `FOR UPDATE SKIP LOCKED`
- Processes videos through a complete pipeline:
  - Video normalization (720p, 30fps)
  - Audio transcription (OpenAI Whisper)
  - Scene detection (PySceneDetect)
  - Frame extraction with deduplication
  - Vision analysis (OpenAI GPT-4o)
  - Embeddings generation (OpenAI text-embedding-3-small)
- Stores results in PostgreSQL with pgvector support
- Rotating log files
- Health check endpoint

## Processing Pipeline

1. **Normalize**: Convert video to 720p, 30fps and extract 16kHz mono audio
2. **Transcribe**: Use OpenAI Whisper to generate transcript segments and SRT files
3. **Scenes**: Detect scene boundaries using PySceneDetect AdaptiveDetector
4. **Frames**: Extract midpoint frames from each scene and deduplicate by perceptual hash
5. **Vision**: Analyze frames with GPT-4o structured outputs for captions, controls, and text
6. **Embeddings**: Generate 1536-dimensional embeddings for transcripts and frame captions
7. **Store**: Persist all results to PostgreSQL with HNSW vector indexes

## Database Schema

The worker creates/updates these tables:
- `scenes(id, video_id, idx, t_start, t_end)`
- `frames(id, video_id, scene_idx, phash, path)`
- `transcript_segments(id, video_id, t_start, t_end, text, embedding VECTOR(1536))`
- `frame_captions(id, video_id, frame_id, caption_json JSONB, embedding VECTOR(1536))`
- Updates `videos.normalized_path` and `videos.duration_sec`

## Environment Variables

- `DATABASE_URL` - PostgreSQL connection string (required)
- `OPENAI_API_KEY` - OpenAI API key (required)
- `DATA_DIR` - Data directory path (default: `/app/data`)
- `WORKER_POLL_MS` - Polling interval in milliseconds (default: 1500)
- `WORKER_MAX_ATTEMPTS` - Max retry attempts (default: 3)
- `LOG_LEVEL` - Logging level (default: INFO)
- `WORKER_DEV_HTTP` - Enable HTTP endpoints (default: false)
- `WORKER_HTTP_PORT` - HTTP server port (default: 8000)

## Docker Compose Integration

Add to your docker-compose.yml:

```yaml
worker:
  build: ./video-worker
  environment:
    - DATABASE_URL=${DATABASE_URL}
    - OPENAI_API_KEY=${OPENAI_KEY}
    - DATA_DIR=/app/data
    - WORKER_POLL_MS=1500
    - WORKER_MAX_ATTEMPTS=3
    - LOG_LEVEL=INFO
    - WORKER_DEV_HTTP=true
  volumes:
    - ./data:/app/data
  depends_on:
    - postgres
```

## HTTP Endpoints (when WORKER_DEV_HTTP=true)

- `GET /healthz` - Health check (returns `{ok: true}` if healthy)
- `GET /jobs/peek` - Show pending jobs (dev only)
- `GET /stats` - Worker statistics (job counts, processing stats)

## Logging

Logs are written to `/app/data/worker/log.log` with rotation:
- Max file size: 5MB
- Backup count: 3
- Log levels: CLAIMED → NORMALIZED → TRANSCRIBED → SCENES → FRAMES → VISION → EMBEDDINGS → READY/FAILED

## Development

To run locally:

```bash
cd video-worker
pip install -r requirements.txt
export DATABASE_URL="postgresql://user:pass@localhost:5432/db"
export OPENAI_API_KEY="your-key"
export WORKER_DEV_HTTP=true
python -m worker.run
```

## Requirements

- Python 3.11+
- PostgreSQL with pgvector extension
- FFmpeg
- OpenAI API key
- Docker (for containerized deployment)
