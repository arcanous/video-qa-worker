# Troubleshooting Guide

Common issues, solutions, and debugging techniques for the video worker.

## Common Issues

### 1. "Video path not found"

**Error**: `Video path not found for video {video_id}`

**Causes**:
- Database `videos.original_path` is NULL or incorrect
- File doesn't exist at resolved path
- `DATA_DIR` environment variable is wrong
- File permissions issue

**Solutions**:
```bash
# Check database path
psql $DATABASE_URL -c "SELECT id, original_path FROM videos WHERE id = 'your-video-id';"

# Check file exists
ls -la "$DATA_DIR/$(psql $DATABASE_URL -t -c "SELECT original_path FROM videos WHERE id = 'your-video-id';")"

# Check DATA_DIR
echo $DATA_DIR
ls -la $DATA_DIR

# Fix permissions
chmod 755 $DATA_DIR
chmod 644 "$DATA_DIR/$(psql $DATABASE_URL -t -c "SELECT original_path FROM videos WHERE id = 'your-video-id';")"
```

### 2. "OpenAI API error"

**Error**: `OpenAI API error: {error_message}`

**Causes**:
- Invalid API key
- Insufficient API credits
- Rate limiting
- Network connectivity issues

**Solutions**:
```bash
# Test API key
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/models

# Check API usage
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/usage

# Verify API key format
echo $OPENAI_API_KEY | wc -c
# Should be 51 characters (sk-...)

# Check rate limits
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/usage
```

### 3. "Database connection failed"

**Error**: `Failed to connect to database: {error}`

**Causes**:
- PostgreSQL not running
- Incorrect connection string
- Network connectivity issues
- Authentication problems

**Solutions**:
```bash
# Test database connection
psql $DATABASE_URL -c "SELECT 1;"

# Check PostgreSQL status
sudo systemctl status postgresql
# or
brew services list | grep postgres

# Verify connection string format
echo $DATABASE_URL
# Should be: postgresql://user:pass@host:port/database

# Test with psql directly
psql -h localhost -U postgres -d videoqa
```

### 4. "FFmpeg not found"

**Error**: `FFmpeg not found` or `No such file or directory: 'ffmpeg'`

**Causes**:
- FFmpeg not installed
- FFmpeg not in PATH
- Wrong FFmpeg version

**Solutions**:
```bash
# Check FFmpeg installation
which ffmpeg
ffmpeg -version

# Install FFmpeg
# macOS
brew install ffmpeg

# Ubuntu/Debian
sudo apt update && sudo apt install ffmpeg

# CentOS/RHEL
sudo yum install ffmpeg

# Add to PATH if needed
export PATH="/usr/local/bin:$PATH"
```

### 5. "Permission denied"

**Error**: `Permission denied` or `Access denied`

**Causes**:
- Insufficient file permissions
- Directory not writable
- User doesn't own files

**Solutions**:
```bash
# Check permissions
ls -la $DATA_DIR
ls -la $DATA_DIR/uploads/
ls -la $DATA_DIR/processed/

# Fix permissions
chmod 755 $DATA_DIR
chmod 755 $DATA_DIR/uploads/
chmod 755 $DATA_DIR/processed/
chmod 755 $DATA_DIR/frames/
chmod 755 $DATA_DIR/subs/
chmod 755 $DATA_DIR/worker/

# Fix ownership
sudo chown -R $USER:$USER $DATA_DIR
```

### 6. "PySceneDetect error"

**Error**: `Error detecting scenes for video {video_id}: {error}`

**Causes**:
- Corrupted video file
- Unsupported video format
- PySceneDetect version issues
- Insufficient memory

**Solutions**:
```bash
# Check video file
ffprobe your-video.mp4

# Test with different video
cp working-video.mp4 $DATA_DIR/uploads/test.mp4

# Check PySceneDetect version
python -c "import scenedetect; print(scenedetect.__version__)"

# Reinstall PySceneDetect
pip install --upgrade scenedetect
```

### 7. "Out of memory"

**Error**: `MemoryError` or `Out of memory`

**Causes**:
- Large video file
- Insufficient system memory
- Memory leak in processing

**Solutions**:
```bash
# Check memory usage
free -h
# or
vm_stat

# Check video file size
ls -lh $DATA_DIR/uploads/your-video.mp4

# Process smaller video first
# Consider upgrading system memory
# Check for memory leaks in logs
```

### 8. "Rate limit exceeded"

**Error**: `Rate limit exceeded` or `Too many requests`

**Causes**:
- OpenAI API rate limits
- Too many concurrent requests
- API key limits

**Solutions**:
```bash
# Check rate limits
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/usage

# Reduce concurrent processing
export WORKER_POLL_MS=5000  # Increase polling interval

# Check API key limits
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/usage
```

## Debugging Techniques

### 1. Enable Debug Logging
```bash
export LOG_LEVEL=DEBUG
python -m worker.run
```

### 2. Check Worker Health
```bash
# Enable HTTP endpoints
export WORKER_DEV_HTTP=true
export WORKER_HTTP_PORT=8000

# Check health
curl http://localhost:8000/healthz

# Check pending jobs
curl http://localhost:8000/jobs/peek

# Check processing stats
curl http://localhost:8000/stats
```

### 3. Monitor Logs in Real-Time
```bash
# Watch all logs
tail -f $DATA_DIR/worker/log.log

# Filter for specific video
grep "your-video-id" $DATA_DIR/worker/log.log

# Filter for errors
grep "ERROR" $DATA_DIR/worker/log.log

# Filter for specific stage
grep "NORMALIZED" $DATA_DIR/worker/log.log
```

### 4. Test Individual Components
```bash
# Test database connection
python -c "from worker.db import Database; db = Database('$DATABASE_URL'); db.connect(); print('Database OK')"

# Test OpenAI API
python -c "import openai; openai.api_key = '$OPENAI_API_KEY'; print(openai.models.list())"

# Test FFmpeg
python -c "import subprocess; subprocess.run(['ffmpeg', '-version'])"

# Test file access
python -c "import os; print(os.listdir('$DATA_DIR'))"
```

### 5. Check Database State
```bash
# Check video status
psql $DATABASE_URL -c "SELECT id, status, original_path FROM videos ORDER BY created_at DESC LIMIT 5;"

# Check job status
psql $DATABASE_URL -c "SELECT id, video_id, status, attempts, error FROM jobs ORDER BY created_at DESC LIMIT 5;"

# Check processing results
psql $DATABASE_URL -c "SELECT COUNT(*) FROM scenes WHERE video_id = 'your-video-id';"
psql $DATABASE_URL -c "SELECT COUNT(*) FROM frames f JOIN scenes s ON f.scene_id = s.id WHERE s.video_id = 'your-video-id';"
psql $DATABASE_URL -c "SELECT COUNT(*) FROM transcript_segments WHERE video_id = 'your-video-id';"
```

### 6. Verify File System
```bash
# Check data directory structure
ls -la $DATA_DIR/
ls -la $DATA_DIR/uploads/
ls -la $DATA_DIR/processed/
ls -la $DATA_DIR/frames/
ls -la $DATA_DIR/subs/
ls -la $DATA_DIR/worker/

# Check file sizes
du -sh $DATA_DIR/*
du -sh $DATA_DIR/uploads/*
du -sh $DATA_DIR/processed/*
```

## Performance Issues

### 1. Slow Processing
**Symptoms**: Videos take much longer than expected to process

**Solutions**:
```bash
# Check system resources
top
htop
iostat

# Check video file size
ls -lh $DATA_DIR/uploads/your-video.mp4

# Check network connectivity
ping api.openai.com

# Check API response times
curl -w "@curl-format.txt" -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/models
```

### 2. High Memory Usage
**Symptoms**: System becomes slow, out of memory errors

**Solutions**:
```bash
# Monitor memory usage
free -h
ps aux --sort=-%mem | head

# Check for memory leaks
python -c "import gc; gc.collect(); print('Memory cleaned')"

# Process smaller videos
# Consider upgrading system memory
```

### 3. Disk Space Issues
**Symptoms**: "No space left on device" errors

**Solutions**:
```bash
# Check disk usage
df -h
du -sh $DATA_DIR/*

# Clean up old files
find $DATA_DIR -name "*.tmp" -delete
find $DATA_DIR -name "*.log" -mtime +7 -delete

# Move processed files to archive
mkdir -p $DATA_DIR/archive
mv $DATA_DIR/processed/* $DATA_DIR/archive/
```

## Recovery Procedures

### 1. Reset Failed Jobs
```bash
# Mark failed jobs as pending
psql $DATABASE_URL -c "UPDATE jobs SET status = 'pending', attempts = 0, error = NULL WHERE status = 'failed';"

# Reset video status
psql $DATABASE_URL -c "UPDATE videos SET status = 'uploaded' WHERE status = 'processing';"
```

### 2. Clean Up Corrupted Data
```bash
# Remove corrupted video files
rm $DATA_DIR/uploads/corrupted-video.mp4

# Remove incomplete processing data
psql $DATABASE_URL -c "DELETE FROM scenes WHERE video_id = 'corrupted-video-id';"
psql $DATABASE_URL -c "DELETE FROM frames WHERE scene_id IN (SELECT id FROM scenes WHERE video_id = 'corrupted-video-id');"
psql $DATABASE_URL -c "DELETE FROM transcript_segments WHERE video_id = 'corrupted-video-id';"
psql $DATABASE_URL -c "DELETE FROM frame_captions WHERE frame_id IN (SELECT id FROM frames WHERE scene_id IN (SELECT id FROM scenes WHERE video_id = 'corrupted-video-id'));"
```

### 3. Restart Worker
```bash
# Stop worker
pkill -f "python -m worker.run"

# Clear any locks
psql $DATABASE_URL -c "UPDATE jobs SET status = 'pending' WHERE status = 'processing';"

# Restart worker
python -m worker.run
```

## Prevention

### 1. Regular Maintenance
```bash
# Clean up old logs
find $DATA_DIR/worker -name "*.log" -mtime +7 -delete

# Vacuum database
psql $DATABASE_URL -c "VACUUM ANALYZE;"

# Check disk space
df -h
```

### 2. Monitoring
```bash
# Set up log monitoring
tail -f $DATA_DIR/worker/log.log | grep -E "(ERROR|FAILED)"

# Set up health checks
curl -f http://localhost:8000/healthz || echo "Worker unhealthy"

# Monitor resource usage
watch -n 5 'ps aux | grep worker'
```

### 3. Backup Strategy
```bash
# Backup database
pg_dump $DATABASE_URL > backup.sql

# Backup processed files
tar -czf processed-backup.tar.gz $DATA_DIR/processed/

# Backup configuration
cp .env.local .env.local.backup
```

## Getting Help

If you can't resolve an issue:

1. **Check logs**: `tail -f $DATA_DIR/worker/log.log`
2. **Verify environment**: Ensure all required variables are set
3. **Test components**: Database, OpenAI API, FFmpeg
4. **Check system resources**: Memory, disk space, CPU
5. **Review documentation**: README.md, PIPELINE.md, DATA_MODEL.md
6. **Search issues**: Look for similar problems in GitHub issues
7. **Create issue**: Provide logs, environment details, and steps to reproduce
