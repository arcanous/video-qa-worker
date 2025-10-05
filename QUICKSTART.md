# Worker Quick Start

Understand the video worker in 5 minutes and get it running locally.

## What the Worker Does

The video worker is a Python service that processes uploaded videos through a 6-stage AI pipeline:

1. **Normalize** video to 720p/30fps and extract mono audio
2. **Transcribe** audio using OpenAI Whisper
3. **Detect scenes** using PySceneDetect
4. **Extract frames** and deduplicate by perceptual hash
5. **Analyze frames** with GPT-4o Vision for captions and controls
6. **Generate embeddings** for semantic search

The worker polls a PostgreSQL database for jobs, processes videos, and stores all results back to the database.

## How to Run Locally

### 1. Prerequisites
```bash
# Python 3.11+
python --version

# FFmpeg installed
ffmpeg -version

# PostgreSQL running
psql --version
```

### 2. Install Dependencies
```bash
# Install Python packages
pip install -r requirements.txt

# Verify FFmpeg
which ffmpeg
```

### 3. Set Environment Variables
```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/videoqa"
export OPENAI_API_KEY="your-openai-api-key"
export DATA_DIR="/path/to/your/data/directory"
export WORKER_DEV_HTTP=true
export LOG_LEVEL=INFO
```

### 4. Run the Worker
```bash
# Start the worker
python -m worker.run

# You should see:
# 2024-01-15 10:30:00 - video_worker - INFO - [run.py:75] - Worker started, polling for jobs...
```

## How to Test a Single Video

### 1. Prepare Test Data
```bash
# Create data directory
mkdir -p /path/to/data/uploads
mkdir -p /path/to/data/processed
mkdir -p /path/to/data/frames
mkdir -p /path/to/data/subs
mkdir -p /path/to/data/worker

# Copy a test video
cp your-test-video.mp4 /path/to/data/uploads/
```

### 2. Insert Test Job
```bash
# Connect to database
psql $DATABASE_URL

# Insert test video record
INSERT INTO videos (id, original_path, original_name, size_bytes, status)
VALUES ('test-video-123', 'uploads/your-test-video.mp4', 'test.mp4', 1000000, 'uploaded');

# Insert test job
INSERT INTO jobs (id, video_id, status)
VALUES ('test-job-123', 'test-video-123', 'pending');

# Exit
\q
```

### 3. Monitor Processing
```bash
# Watch worker logs
tail -f /path/to/data/worker/log.log

# Check job status
psql $DATABASE_URL -c "SELECT id, video_id, status, attempts FROM jobs WHERE video_id = 'test-video-123';"

# Check video status
psql $DATABASE_URL -c "SELECT id, status, normalized_path FROM videos WHERE id = 'test-video-123';"
```

## How to View Logs

### Worker Logs
```bash
# Real-time logs
tail -f /path/to/data/worker/log.log

# Search for specific video
grep "test-video-123" /path/to/data/worker/log.log

# Search for errors
grep "ERROR" /path/to/data/worker/log.log
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

### Log Format
```
2024-01-15 10:30:00 - video_worker - INFO - [run.py:75] - CLAIMED: Processing job abc123 for video def456
```

## How to Debug Common Issues

### Issue: "Video path not found"
**Check**: Database has correct path
```bash
psql $DATABASE_URL -c "SELECT original_path FROM videos WHERE id = 'your-video-id';"
```

**Check**: File exists at resolved path
```bash
# Resolve path manually
echo "$DATA_DIR/$(psql $DATABASE_URL -t -c "SELECT original_path FROM videos WHERE id = 'your-video-id';")"
ls -la "$DATA_DIR/$(psql $DATABASE_URL -t -c "SELECT original_path FROM videos WHERE id = 'your-video-id';")"
```

### Issue: "OpenAI API error"
**Check**: API key is valid
```bash
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/models
```

**Check**: API key has credits
```bash
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/usage
```

### Issue: "Database connection failed"
**Check**: Database is running
```bash
psql $DATABASE_URL -c "SELECT 1;"
```

**Check**: Connection string format
```bash
echo $DATABASE_URL
# Should be: postgresql://user:pass@host:port/database
```

### Issue: "FFmpeg not found"
**Check**: FFmpeg is installed
```bash
which ffmpeg
ffmpeg -version
```

**Install FFmpeg**:
```bash
# macOS
brew install ffmpeg

# Ubuntu/Debian
sudo apt update && sudo apt install ffmpeg

# CentOS/RHEL
sudo yum install ffmpeg
```

### Issue: "Permission denied"
**Check**: Data directory permissions
```bash
ls -la $DATA_DIR
chmod 755 $DATA_DIR
```

**Check**: Write permissions
```bash
touch $DATA_DIR/test.txt && rm $DATA_DIR/test.txt
```

## Health Check Endpoints

When `WORKER_DEV_HTTP=true`, the worker exposes HTTP endpoints:

### Health Check
```bash
curl http://localhost:8000/healthz
# Response: {"ok": true, "status": "healthy"}
```

### Pending Jobs
```bash
curl http://localhost:8000/jobs/peek
# Response: {"pending_jobs": 1, "jobs": [...]}
```

### Processing Stats
```bash
curl http://localhost:8000/stats
# Response: {"jobs": {...}, "videos": {...}, "processing": {...}}
```

## Performance Testing

### Test with Different Video Sizes
```bash
# Small video (1-2 minutes)
# Expected processing time: 30-60 seconds

# Medium video (5-10 minutes)
# Expected processing time: 2-5 minutes

# Large video (30+ minutes)
# Expected processing time: 10-20 minutes
```

### Monitor Resource Usage
```bash
# CPU usage
top -p $(pgrep -f "python -m worker.run")

# Memory usage
ps aux | grep "python -m worker.run"

# Disk usage
du -sh $DATA_DIR
```

## Next Steps

- [ ] Read [README.md](./README.md) for complete documentation
- [ ] Explore [PIPELINE.md](./PIPELINE.md) for pipeline details
- [ ] Check [DATA_MODEL.md](./DATA_MODEL.md) for database schema
- [ ] Review [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) for common issues
- [ ] Try processing different video types and sizes
- [ ] Monitor logs to understand the pipeline flow

## Getting Help

If you encounter issues:

1. **Check logs first**: `tail -f /path/to/data/worker/log.log`
2. **Verify environment**: Ensure all required variables are set
3. **Test database**: `psql $DATABASE_URL -c "SELECT 1;"`
4. **Test OpenAI API**: `curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/models`
5. **Check file permissions**: Ensure data directory is writable
6. **Review troubleshooting guides**: See TROUBLESHOOTING.md
