# Video Worker

A Python worker that processes video files using OpenAI APIs and stores results in PostgreSQL. The worker implements a 6-stage pipeline that transforms raw video uploads into searchable, AI-analyzed content.

## Pipeline Architecture

The worker processes videos through a sequential 6-stage pipeline:

```
Input: uploads/{id}_{name}.mp4
  │
  ├─▶ [1. NORMALIZE] → processed/{id}/normalized.mp4
  │                  → processed/{id}/audio.wav
  │
  ├─▶ [2. TRANSCRIBE] → transcript_segments table
  │                    → subs/{id}.srt
  │
  ├─▶ [3. SCENES] → scenes table (t_start, t_end)
  │
  ├─▶ [4. FRAMES] → frames/{id}/scene_*.jpg
  │               → frames table (phash, path)
  │
  ├─▶ [5. VISION] → frame_captions table (caption, entities)
  │
  └─▶ [6. EMBEDDINGS] → UPDATE embeddings (1536-dim vectors)

Output: video.status = 'ready'
```

## Processing Pipeline

### Stage 1: Normalize
**Purpose**: Convert video to standard format and extract audio
- **Input**: Original video file (any format)
- **Output**: 720p/30fps video + 16kHz mono audio
- **Tools**: FFmpeg
- **Database**: Updates `videos.normalized_path`, `videos.duration_sec`

### Stage 2: Transcribe
**Purpose**: Generate accurate transcript from audio
- **Input**: 16kHz mono audio file
- **Output**: Timestamped transcript segments
- **Tools**: OpenAI Whisper API
- **Database**: Inserts `transcript_segments` records
- **Files**: Generates SRT subtitle file

### Stage 3: Scenes
**Purpose**: Detect scene boundaries for frame extraction
- **Input**: Normalized video file
- **Output**: Scene time boundaries
- **Tools**: PySceneDetect AdaptiveDetector
- **Database**: Inserts `scenes` records with `t_start`, `t_end`

### Stage 4: Frames
**Purpose**: Extract representative frames and deduplicate
- **Input**: Normalized video + scene boundaries
- **Output**: Frame images with perceptual hashes
- **Tools**: FFmpeg + imagehash
- **Database**: Inserts `frames` records with `phash` for deduplication
- **Files**: Saves frame images to `frames/{video_id}/`

### Stage 5: Vision
**Purpose**: Analyze frames with AI vision
- **Input**: Frame images
- **Output**: Captions, controls, text detection
- **Tools**: OpenAI GPT-4o Vision API
- **Database**: Inserts `frame_captions` records
- **Features**: Structured output for consistent data

### Stage 6: Embeddings
**Purpose**: Generate searchable vector embeddings
- **Input**: Transcript text + frame captions
- **Output**: 1536-dimensional vectors
- **Tools**: OpenAI text-embedding-3-small
- **Database**: Updates embedding columns in `transcript_segments` and `frame_captions`

## Database Coupling

The worker is **tightly coupled** to the PostgreSQL schema:

### Read Operations
- `videos.original_path` - Input video location
- `jobs` table - Job queue polling
- Existing records for idempotency

### Write Operations
- `scenes` - Scene boundaries
- `frames` - Extracted frames with hashes
- `transcript_segments` - Audio transcription
- `frame_captions` - Vision analysis results

### Update Operations
- `videos.status` - Processing status
- `videos.normalized_path` - Processed video location
- `videos.duration_sec` - Video duration
- `jobs.status` - Job completion status

### ID Generation Patterns
The worker follows strict ID patterns for consistency:

```python
# Scene ID: "{video_id}_scene_{idx:03d}"
scene_id = f"{video_id}_scene_{i:03d}"

# Frame ID: "{video_id}_frame_{idx:03d}"  
frame_id = f"{video_id}_frame_{i:03d}"

# Segment ID: "{video_id}_segment_{idx:03d}"
segment_id = f"{video_id}_segment_{i:03d}"

# Caption ID: "{frame_id}_caption"
caption_id = f"{frame_id}_caption"
```

## Job Processing Model

### Polling Mechanism
- **Interval**: 1.5 seconds (configurable via `WORKER_POLL_MS`)
- **Strategy**: `FOR UPDATE SKIP LOCKED` for atomic job claiming
- **Backoff**: Exponential backoff when no jobs available
- **Retry**: Up to 3 attempts per job (configurable via `WORKER_MAX_ATTEMPTS`)

### Status Flow
```
pending → processing → done/failed
```

### Error Handling
- **Job Failures**: Marked as `failed` with error message
- **Video Failures**: Status remains `processing` until retry
- **Logging**: Comprehensive error logging with stack traces
- **Recovery**: Jobs can be retried manually

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | ✅ | - | PostgreSQL connection string |
| `OPENAI_API_KEY` | ✅ | - | OpenAI API key |
| `DATA_DIR` | ❌ | `/app/data` | Data directory path |
| `WORKER_POLL_MS` | ❌ | `1500` | Polling interval (milliseconds) |
| `WORKER_MAX_ATTEMPTS` | ❌ | `3` | Max retry attempts |
| `LOG_LEVEL` | ❌ | `INFO` | Logging level |
| `WORKER_DEV_HTTP` | ❌ | `false` | Enable HTTP endpoints |
| `WORKER_HTTP_PORT` | ❌ | `8000` | HTTP server port |

## HTTP Endpoints (Development)

When `WORKER_DEV_HTTP=true`:

| Endpoint | Method | Purpose | Response |
|----------|--------|---------|----------|
| `/healthz` | GET | Health check | `{ok: true, status: "healthy"}` |
| `/jobs/peek` | GET | View pending jobs | `{pending_jobs: number, jobs: [...]}` |
| `/stats` | GET | Processing statistics | `{jobs: {...}, videos: {...}, processing: {...}}` |

## Logging

### Log Format
```
2024-01-15 10:30:00 - video_worker - INFO - [run.py:75] - CLAIMED: Processing job abc123 for video def456
```

### Log Levels
- **CLAIMED**: Job claimed for processing
- **NORMALIZED**: Video normalization complete
- **TRANSCRIBED**: Audio transcription complete
- **SCENES**: Scene detection complete
- **FRAMES**: Frame extraction complete
- **VISION**: Vision analysis complete
- **EMBEDDINGS**: Embedding generation complete
- **READY**: Pipeline completed successfully
- **FAILED**: Pipeline failed with error

### Log Files
- **Location**: `{DATA_DIR}/worker/log.log`
- **Rotation**: 5MB max size, 3 backup files
- **Format**: Structured logging with timestamps

## Performance Characteristics

### Processing Times
| Video Length | Normalize | Transcribe | Scenes | Frames | Vision | Embeddings | Total |
|--------------|-----------|------------|--------|--------|--------|------------|-------|
| 1 minute | 5s | 10s | 2s | 3s | 15s | 5s | 40s |
| 5 minutes | 15s | 30s | 5s | 10s | 60s | 20s | 2.5min |
| 30 minutes | 60s | 3min | 20s | 45s | 5min | 2min | 12min |

### Resource Usage
- **CPU**: High during FFmpeg operations and AI API calls
- **Memory**: Moderate (image processing, embeddings)
- **Storage**: 2-3x original video size
- **Network**: OpenAI API calls for transcription and vision

## Development

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DATABASE_URL="postgresql://user:pass@localhost:5432/videoqa"
export OPENAI_API_KEY="your-key"
export DATA_DIR="/path/to/data"
export WORKER_DEV_HTTP=true

# Run worker
python -m worker.run
```

### Testing Single Video
```bash
# Insert test job
psql $DATABASE_URL -c "INSERT INTO jobs (id, video_id) VALUES ('test-job', 'test-video');"

# Monitor logs
tail -f data/worker/log.log
```

### Debugging
```bash
# Check worker health
curl http://localhost:8000/healthz

# View pending jobs
curl http://localhost:8000/jobs/peek

# Check processing stats
curl http://localhost:8000/stats
```

## Docker Integration

### Dockerfile
```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y ffmpeg tesseract-ocr
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY worker ./worker
CMD ["python", "-m", "worker.run"]
```

### Docker Compose
```yaml
worker:
  image: videoqa-worker:0.0.17
  environment:
    - DATABASE_URL=postgresql://postgres:postgres@postgres:5432/videoqa
    - OPENAI_API_KEY=${OPENAI_KEY}
    - DATA_DIR=/app/data
  volumes:
    - ./data:/app/data
  depends_on:
    - postgres
```

## Troubleshooting

### Common Issues

1. **"Video path not found"**
   - Check `DATA_DIR` environment variable
   - Verify file exists at resolved path
   - Check database `videos.original_path` value

2. **"OpenAI API error"**
   - Verify `OPENAI_API_KEY` is valid
   - Check API key has sufficient credits
   - Monitor API rate limits

3. **"Database connection failed"**
   - Check `DATABASE_URL` format
   - Verify PostgreSQL is running
   - Check network connectivity

4. **"FFmpeg not found"**
   - Ensure FFmpeg is installed in container
   - Check Dockerfile includes FFmpeg installation

### Debug Commands
```bash
# Check worker logs
docker-compose logs worker

# Check database connection
docker-compose exec worker python -c "from worker.db import Database; db = Database('$DATABASE_URL'); db.connect()"

# Test OpenAI API
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/models
```

## See Also

- [PIPELINE.md](./PIPELINE.md) - Detailed pipeline documentation
- [DATA_MODEL.md](./DATA_MODEL.md) - Database schema documentation
- [QUICKSTART.md](./QUICKSTART.md) - 5-minute setup guide
- [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) - Common issues and solutions
- [../video-qa/README.md](../video-qa/README.md) - Frontend documentation