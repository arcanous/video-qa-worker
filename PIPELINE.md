# Pipeline Documentation

Detailed documentation of the 6-stage video processing pipeline, including inputs, outputs, dependencies, and performance characteristics.

## Pipeline Overview

The video processing pipeline transforms raw video uploads into searchable, AI-analyzed content through 6 sequential stages:

```
[1. NORMALIZE] → [2. TRANSCRIBE] → [3. SCENES] → [4. FRAMES] → [5. VISION] → [6. EMBEDDINGS]
```

Each stage is **idempotent** - safe to re-run without side effects.

## Stage 1: Normalize

### Purpose
Convert video to standard format and extract mono audio for processing.

### Inputs
- **File**: `uploads/{id}_{name}.mp4` (any video format)
- **Database**: `videos.original_path` (relative path)

### Processing
```python
# FFmpeg command (simplified)
ffmpeg -i input.mp4 \
  -vf "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2" \
  -r 30 \
  -c:v libx264 \
  -preset fast \
  -crf 23 \
  -c:a aac \
  -ac 1 \
  -ar 16000 \
  -b:a 128k \
  output.mp4
```

### Outputs
- **Files**: 
  - `processed/{video_id}/normalized.mp4` (720p, 30fps)
  - `processed/{video_id}/audio.wav` (16kHz mono)
- **Database**: Updates `videos.normalized_path`, `videos.duration_sec`

### Dependencies
- **FFmpeg**: Video processing
- **File System**: Write access to processed directory

### Performance
- **Time**: 5-60 seconds (depends on video length and complexity)
- **CPU**: High (video encoding)
- **Memory**: Moderate (video frames in memory)

### Error Handling
- **Invalid Video**: FFmpeg validation before processing
- **Corrupted Files**: Graceful failure with error logging
- **Disk Space**: Check available space before processing

## Stage 2: Transcribe

### Purpose
Generate accurate, timestamped transcript from audio using OpenAI Whisper.

### Inputs
- **File**: `processed/{video_id}/audio.wav` (16kHz mono)
- **API**: OpenAI Whisper API

### Processing
```python
# OpenAI Whisper API call
response = openai.Audio.transcribe(
    model="whisper-1",
    file=audio_file,
    response_format="verbose_json",
    timestamp_granularities=["segment"]
)
```

### Outputs
- **Database**: `transcript_segments` table
  - `id`: `{video_id}_segment_{idx:03d}`
  - `video_id`: Video identifier
  - `t_start`, `t_end`: Timestamp boundaries
  - `text`: Transcript text
- **Files**: `subs/{video_id}.srt` (SRT subtitle file)

### Dependencies
- **OpenAI API**: Whisper model access
- **Network**: Stable internet connection
- **Database**: Write access to transcript_segments

### Performance
- **Time**: 10 seconds - 3 minutes (depends on audio length)
- **API Calls**: 1 per video
- **Cost**: ~$0.006 per minute of audio

### Error Handling
- **API Limits**: Rate limiting and retry logic
- **Network Issues**: Timeout and retry mechanisms
- **Invalid Audio**: Validation before API call

## Stage 3: Scenes

### Purpose
Detect scene boundaries using PySceneDetect for intelligent frame extraction.

### Inputs
- **File**: `processed/{video_id}/normalized.mp4`
- **Library**: PySceneDetect 0.6.7+ (new API)

### Processing
```python
# PySceneDetect 0.6.7+ scene detection (new API)
from scenedetect import open_video, SceneManager
from scenedetect.detectors import AdaptiveDetector

# Open video with new API
video = open_video(video_path)
scene_manager = SceneManager()
scene_manager.add_detector(AdaptiveDetector())

# Detect scenes
scene_manager.detect_scenes(video)
scene_list = scene_manager.get_scene_list()
```

### Outputs
- **Database**: `scenes` table
  - `id`: `{video_id}_scene_{idx:03d}`
  - `video_id`: Video identifier
  - `idx`: Scene index (0-based)
  - `t_start`, `t_end`: Scene boundaries in seconds

### Dependencies
- **PySceneDetect**: Scene detection library
- **FFmpeg**: Video reading capabilities
- **Database**: Write access to scenes table

### Performance
- **Time**: 2-20 seconds (depends on video length)
- **CPU**: Moderate (video analysis)
- **Memory**: Low (scene detection)

### Algorithm Details
- **AdaptiveDetector**: Automatically adjusts sensitivity
- **Threshold**: 30.0 (default, configurable)
- **Min Scene Length**: 15 seconds (default, configurable)

## Stage 4: Frames

### Purpose
Extract representative frames from each scene and deduplicate using perceptual hashing.

### Inputs
- **File**: `processed/{video_id}/normalized.mp4`
- **Database**: `scenes` table (scene boundaries)

### Processing
```python
# Extract midpoint frame from each scene
for scene in scenes:
    midpoint = (scene['t_start'] + scene['t_end']) / 2
    frame_path = extract_frame(video_path, midpoint)
    
    # Generate perceptual hash
    phash = generate_phash(frame_path)
    
    # Deduplicate similar frames
    if not is_duplicate(phash, seen_hashes):
        frames.append({
            'scene_idx': scene['idx'],
            'timestamp': midpoint,
            'path': frame_path,
            'phash': phash
        })
```

### Outputs
- **Files**: `frames/{video_id}/scene_*.jpg` (frame images)
- **Database**: `frames` table
  - `id`: `{video_id}_frame_{idx:03d}`
  - `scene_id`: Reference to scenes table
  - `t_frame`: Frame timestamp
  - `path`: Frame image path
  - `phash`: Perceptual hash for deduplication

### Dependencies
- **FFmpeg**: Frame extraction
- **imagehash**: Perceptual hashing
- **PIL**: Image processing
- **Database**: Write access to frames table

### Performance
- **Time**: 3-45 seconds (depends on number of scenes)
- **CPU**: Moderate (image processing)
- **Memory**: Low (one frame at a time)
- **Storage**: ~50KB per frame image

### Deduplication Algorithm
- **Perceptual Hash**: 64-bit hash of image content
- **Hamming Distance**: Threshold of 6 for similarity
- **Order Preservation**: Maintains scene order
- **First/Last**: Always includes first and last scene

## Stage 5: Vision

### Purpose
Analyze frames with GPT-4o Vision to extract captions, controls, and text.

### Inputs
- **Files**: `frames/{video_id}/scene_*.jpg` (frame images)
- **API**: OpenAI GPT-4o Vision API

### Processing
```python
# Parallel GPT-4o Vision API calls with semaphore
import asyncio
from asyncio import Semaphore

async def analyze_frame_with_vision_async(frame_path, semaphore):
    async with semaphore:
        # GPT-4o Vision API call with structured output
        response = await openai.ChatCompletion.acreate(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Analyze this frame..."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]
            }],
            response_format={"type": "json_schema", "json_schema": vision_schema}
        )
        return response

# Process frames in parallel
semaphore = Semaphore(VISION_MAX_CONCURRENT)
tasks = [analyze_frame_with_vision_async(frame, semaphore) for frame in frames]
results = await asyncio.gather(*tasks)
```

### Outputs
- **Database**: `frame_captions` table
  - `id`: `{frame_id}_caption`
  - `frame_id`: Reference to frames table
  - `caption`: AI-generated caption
  - `entities`: JSONB with controls and text detection
  - `embedding`: 1536-dimensional vector (added in stage 6)

### Dependencies
- **OpenAI API**: GPT-4o Vision access
- **Network**: Stable internet connection
- **Database**: Write access to frame_captions

### Performance
- **Time**: 8 seconds - 2 minutes (parallel processing, 3-5x improvement)
- **API Calls**: Concurrent requests with semaphore limiting
- **Cost**: ~$0.01 per frame
- **Rate Limits**: Configurable concurrency (default: 5 concurrent)
- **Fallback**: Sequential processing if parallel fails

### Structured Output Schema
```json
{
  "caption": "string",
  "controls": [
    {
      "type": "button",
      "label": "Start",
      "position": "center"
    }
  ],
  "text_on_screen": [
    {
      "text": "Welcome to the tutorial",
      "position": "top-left"
    }
  ]
}
```

## Stage 6: Embeddings

### Purpose
Generate 1536-dimensional vector embeddings for semantic search.

### Inputs
- **Database**: `transcript_segments.text`, `frame_captions.caption`
- **API**: OpenAI text-embedding-3-small

### Processing
```python
# Batch embedding generation
texts = [segment['text'] for segment in segments]
embeddings = openai.Embedding.create(
    model="text-embedding-3-small",
    input=texts
)
```

### Outputs
- **Database**: Updates embedding columns
  - `transcript_segments.embedding` (VECTOR(1536))
  - `frame_captions.embedding` (VECTOR(1536))

### Dependencies
- **OpenAI API**: Embedding model access
- **pgvector**: Vector storage and indexing
- **Database**: Update access to embedding columns

### Performance
- **Time**: 5 seconds - 2 minutes (depends on text volume)
- **API Calls**: 1 per batch (up to 100 texts)
- **Cost**: ~$0.0001 per 1K tokens
- **Storage**: 1536 floats per embedding (~6KB)

## Pipeline Orchestration

### Execution Flow
```python
def process_video(job_id, video_id):
    # Stage 1: Normalize
    normalized_path, audio_path, duration = normalize_video(video_path, video_id)
    db.update_video_normalized(video_id, normalized_path, duration)
    
    # Stage 2: Transcribe
    segments = transcribe_audio(audio_path, video_id)
    db.insert_transcript_segments(video_id, segments)
    
    # Stage 3: Scenes
    scenes = detect_scenes(normalized_path, video_id)
    db.insert_scenes(video_id, scenes)
    
    # Stage 4: Frames
    frames = extract_scene_frames(normalized_path, scenes, video_id)
    db.insert_frames(video_id, frames)
    
    # Stage 5: Vision
    frame_analyses = batch_analyze_frames(frames, video_id)
    db.insert_frame_captions(video_id, frame_analyses)
    
    # Stage 6: Embeddings
    embed_transcript_segments(video_id, segments)
    embed_frame_captions(video_id, frame_analyses)
    
    # Complete
    db.complete_job(job_id, video_id)
```

### Error Handling
- **Stage Failures**: Mark job as failed with error message
- **Partial Failures**: Previous stages remain completed
- **Retry Logic**: Up to 3 attempts per job
- **Logging**: Comprehensive error logging with stack traces

### Idempotency
- **Database**: `ON CONFLICT DO NOTHING` for all inserts
- **Files**: Overwrite existing files
- **API Calls**: Safe to re-run (no side effects)
- **Status**: Clear success/failure states

## Performance Optimization

### Batch Processing
- **Frames**: Process multiple frames in single API call
- **Embeddings**: Batch text inputs for efficiency
- **Database**: Bulk inserts where possible

### Caching
- **Perceptual Hashes**: Avoid re-processing identical frames
- **API Responses**: Cache expensive API calls
- **Database**: Connection pooling

### Resource Management
- **Memory**: Process one video at a time
- **CPU**: Parallel processing where possible
- **Network**: Rate limiting and retry logic
- **Storage**: Cleanup temporary files

## Monitoring and Metrics

### Key Metrics
- **Processing Time**: Per stage and total
- **Success Rate**: Jobs completed vs failed
- **API Usage**: OpenAI API calls and costs
- **Resource Usage**: CPU, memory, disk, network

### Logging
- **Stage Transitions**: Clear log messages for each stage
- **Performance**: Timing information
- **Errors**: Detailed error messages with context
- **Progress**: Percentage completion where possible

### Health Checks
- **Database**: Connection and query performance
- **APIs**: OpenAI API availability
- **Storage**: Disk space and file access
- **Worker**: Job processing status

