# Database Schema Documentation

Comprehensive documentation of the database schema, table relationships, ID patterns, and query patterns used by the video processing system.

## Schema Overview

The database uses PostgreSQL with the `pgvector` extension for vector embeddings. The schema is designed for:

- **Video Processing**: Track videos through the pipeline
- **Job Management**: Queue and monitor processing jobs
- **Content Analysis**: Store scenes, frames, transcripts, and captions
- **Search**: Vector embeddings for semantic search

## Table Relationships

```
videos (1) ──→ (many) jobs
    │
    ├─→ (many) scenes
    │       │
    │       └─→ (many) frames
    │               │
    │               └─→ (1) frame_captions
    │
    └─→ (many) transcript_segments
```

## Core Tables

### videos
**Purpose**: Video metadata and processing status

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `id` | TEXT | Primary key (nanoid) | PRIMARY KEY |
| `original_path` | TEXT | Relative path to uploaded file | NOT NULL |
| `original_name` | TEXT | Original filename | NOT NULL |
| `size_bytes` | INT | File size in bytes | NOT NULL |
| `status` | TEXT | Processing status | NOT NULL, DEFAULT 'uploaded' |
| `normalized_path` | TEXT | Path to processed video | NULL |
| `duration_sec` | REAL | Video duration in seconds | NULL |
| `created_at` | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |
| `updated_at` | TIMESTAMPTZ | Last update timestamp | DEFAULT NOW() |

**Status Values**:
- `uploaded`: Video uploaded, not yet processed
- `processing`: Currently being processed by worker
- `ready`: Processing complete, ready for querying
- `failed`: Processing failed

**Indexes**:
- Primary key on `id`
- Index on `status` for status queries

### jobs
**Purpose**: Processing queue with status tracking

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `id` | TEXT | Primary key (nanoid) | PRIMARY KEY |
| `video_id` | TEXT | Reference to videos.id | FOREIGN KEY, NOT NULL |
| `status` | TEXT | Job status | NOT NULL, DEFAULT 'pending' |
| `attempts` | INT | Number of processing attempts | NOT NULL, DEFAULT 0 |
| `error` | TEXT | Error message if failed | NULL |
| `created_at` | TIMESTAMPTZ | Job creation time | DEFAULT NOW() |
| `updated_at` | TIMESTAMPTZ | Last update time | DEFAULT NOW() |

**Status Values**:
- `pending`: Waiting to be processed
- `processing`: Currently being processed
- `done`: Successfully completed
- `failed`: Processing failed

**Indexes**:
- Primary key on `id`
- Index on `(video_id, status)` for job queries
- Index on `status` for worker polling

### scenes
**Purpose**: Scene boundaries detected in videos

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `id` | TEXT | Primary key | PRIMARY KEY |
| `video_id` | TEXT | Reference to videos.id | FOREIGN KEY, NOT NULL |
| `idx` | INT | Scene index (0-based) | NOT NULL |
| `t_start` | REAL | Start time in seconds | NOT NULL |
| `t_end` | REAL | End time in seconds | NOT NULL |
| `clip_path` | TEXT | Path to scene clip (future) | NULL |

**ID Pattern**: `{video_id}_scene_{idx:03d}`
**Example**: `abc123def456_scene_001`

**Indexes**:
- Primary key on `id`
- Unique constraint on `(video_id, idx)`
- Index on `video_id` for video queries

### frames
**Purpose**: Extracted frames with perceptual hashes

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `id` | TEXT | Primary key | PRIMARY KEY |
| `scene_id` | TEXT | Reference to scenes.id | FOREIGN KEY, NOT NULL |
| `t_frame` | REAL | Frame timestamp in seconds | NOT NULL |
| `path` | TEXT | Path to frame image | NOT NULL |
| `phash` | TEXT | Perceptual hash for deduplication | NOT NULL |

**ID Pattern**: `{video_id}_frame_{idx:03d}`
**Example**: `abc123def456_frame_001`

**Indexes**:
- Primary key on `id`
- Index on `scene_id` for scene queries
- Index on `phash` for deduplication

### transcript_segments
**Purpose**: Audio transcription with embeddings

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `id` | TEXT | Primary key | PRIMARY KEY |
| `video_id` | TEXT | Reference to videos.id | FOREIGN KEY, NOT NULL |
| `t_start` | REAL | Segment start time | NOT NULL |
| `t_end` | REAL | Segment end time | NOT NULL |
| `text` | TEXT | Transcript text | NOT NULL |
| `embedding` | VECTOR(1536) | Semantic embedding | NULL |

**ID Pattern**: `{video_id}_segment_{idx:03d}`
**Example**: `abc123def456_segment_001`

**Indexes**:
- Primary key on `id`
- Unique constraint on `(video_id, t_start, t_end)`
- HNSW index on `embedding` for vector search

### frame_captions
**Purpose**: Vision analysis results with embeddings

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `id` | TEXT | Primary key | PRIMARY KEY |
| `frame_id` | TEXT | Reference to frames.id | FOREIGN KEY, NOT NULL |
| `caption` | TEXT | AI-generated caption | NOT NULL |
| `entities` | JSONB | Controls and text detection | NOT NULL |
| `embedding` | VECTOR(1536) | Semantic embedding | NULL |

**ID Pattern**: `{frame_id}_caption`
**Example**: `abc123def456_frame_001_caption`

**Indexes**:
- Primary key on `id`
- Index on `frame_id` for frame queries
- HNSW index on `embedding` for vector search

## ID Generation Patterns

The system uses consistent ID patterns for easy debugging and relationship tracking:

### Pattern Rules
1. **Video ID**: `nanoid()` - e.g., `abc123def456`
2. **Job ID**: `nanoid()` - e.g., `xyz789uvw012`
3. **Scene ID**: `{video_id}_scene_{idx:03d}` - e.g., `abc123def456_scene_001`
4. **Frame ID**: `{video_id}_frame_{idx:03d}` - e.g., `abc123def456_frame_001`
5. **Segment ID**: `{video_id}_segment_{idx:03d}` - e.g., `abc123def456_segment_001`
6. **Caption ID**: `{frame_id}_caption` - e.g., `abc123def456_frame_001_caption`

### Benefits
- **Debugging**: Easy to trace relationships
- **Consistency**: Predictable ID structure
- **Uniqueness**: No ID collisions
- **Readability**: Human-readable IDs

## Vector Embeddings

### Embedding Model
- **Model**: `text-embedding-3-small`
- **Dimensions**: 1536
- **Usage**: Semantic search and similarity

### Vector Operations
```sql
-- Find similar transcript segments
SELECT id, text, 1 - (embedding <=> query_embedding) as similarity
FROM transcript_segments
WHERE embedding <=> query_embedding < 0.8
ORDER BY embedding <=> query_embedding
LIMIT 10;

-- Find similar frame captions
SELECT id, caption, 1 - (embedding <=> query_embedding) as similarity
FROM frame_captions
WHERE embedding <=> query_embedding < 0.8
ORDER BY embedding <=> query_embedding
LIMIT 10;
```

### HNSW Indexes
```sql
-- Transcript segments embedding index
CREATE INDEX transcript_segments_embedding_hnsw
ON transcript_segments USING hnsw (embedding vector_cosine_ops);

-- Frame captions embedding index
CREATE INDEX frame_captions_embedding_hnsw
ON frame_captions USING hnsw (embedding vector_cosine_ops);
```

## Query Patterns

### Worker Queries

#### Job Claiming
```sql
-- Atomically claim a job
WITH j AS (
    SELECT id, video_id
    FROM jobs
    WHERE status = 'pending'
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET status = 'processing', attempts = COALESCE(attempts, 0) + 1
FROM j
WHERE jobs.id = j.id
RETURNING jobs.id, j.video_id;
```

#### Video Path Resolution
```sql
-- Get video path for processing
SELECT original_path FROM videos WHERE id = %s;
```

#### Scene Insertion
```sql
-- Insert scenes with idempotency
INSERT INTO scenes (id, video_id, idx, t_start, t_end)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (video_id, idx) DO NOTHING;
```

#### Frame Insertion
```sql
-- Insert frames with scene references
INSERT INTO frames (id, scene_id, t_frame, path, phash)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
```

### Frontend Queries

#### Video Status
```sql
-- Get video processing status
SELECT 
    v.status,
    COALESCE(j.attempts, 0) as attempts,
    j.updated_at as updatedAt
FROM videos v
LEFT JOIN jobs j ON v.id = j.video_id
WHERE v.id = %s
ORDER BY j.updated_at DESC
LIMIT 1;
```

#### Video Summary
```sql
-- Get processing results summary
SELECT 
    (SELECT COUNT(*)::int FROM scenes WHERE video_id = %s) as scenes,
    (SELECT COUNT(*)::int FROM frames f JOIN scenes s ON f.scene_id = s.id WHERE s.video_id = %s) as frames,
    (SELECT COUNT(*)::int FROM transcript_segments WHERE video_id = %s) as transcriptSegments,
    (SELECT COALESCE(SUM(LENGTH(text)), 0)::int FROM transcript_segments WHERE video_id = %s) as transcriptChars;
```

## Data Integrity

### Foreign Key Constraints
- `jobs.video_id` → `videos.id`
- `scenes.video_id` → `videos.id`
- `frames.scene_id` → `scenes.id`
- `transcript_segments.video_id` → `videos.id`
- `frame_captions.frame_id` → `frames.id`

### Unique Constraints
- `scenes(video_id, idx)` - One scene per index per video
- `transcript_segments(video_id, t_start, t_end)` - One segment per time range
- `frames(id)` - Unique frame IDs
- `frame_captions(id)` - Unique caption IDs

### Check Constraints
- `videos.status` IN ('uploaded', 'processing', 'ready', 'failed')
- `jobs.status` IN ('pending', 'processing', 'done', 'failed')
- `jobs.attempts` >= 0
- `scenes.t_start` < `scenes.t_end`
- `transcript_segments.t_start` < `transcript_segments.t_end`

## Performance Optimization

### Indexes
- **Primary Keys**: All tables have primary key indexes
- **Foreign Keys**: Indexes on all foreign key columns
- **Status Queries**: Indexes on status columns
- **Vector Search**: HNSW indexes on embedding columns

### Query Optimization
- **Connection Pooling**: psycopg connection pool
- **Batch Inserts**: Bulk insert operations
- **Idempotency**: `ON CONFLICT DO NOTHING` for safe re-runs
- **Atomic Operations**: `FOR UPDATE SKIP LOCKED` for job claiming

### Storage Optimization
- **Vector Compression**: Efficient vector storage
- **JSONB**: Efficient JSON storage for entities
- **Text Compression**: PostgreSQL text compression
- **Index Maintenance**: Regular VACUUM and ANALYZE

## Migration Strategy

### Schema Evolution
- **Additive Changes**: New columns with defaults
- **Backward Compatibility**: Old code continues to work
- **Data Migration**: Safe column type changes
- **Index Creation**: Non-blocking index creation

### Example Migration
```sql
-- Add new column with default
ALTER TABLE videos ADD COLUMN IF NOT EXISTS new_field TEXT DEFAULT 'default_value';

-- Migrate existing data
UPDATE videos SET new_field = 'computed_value' WHERE new_field = 'default_value';

-- Add index (non-blocking)
CREATE INDEX CONCURRENTLY videos_new_field_idx ON videos(new_field);
```
