"""
Postgres adapter implementations for job source and storage.

Refactors the existing db.py logic into separate job source and storage adapters
that implement the abstract base classes.
"""

import os
import json
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime

from .base import JobSourceAdapter, StorageAdapter, Job, VideoMetadata, ProcessingResult
from ..logging_setup import log_exception

logger = logging.getLogger("video_worker")


class PostgresJobSourceAdapter(JobSourceAdapter):
    """Postgres implementation of job source adapter"""
    
    def __init__(self, database_url: str, pool_size: int = 5, timeout: int = 10):
        self.database_url = database_url
        self.pool_size = pool_size
        self.timeout = timeout
        self.pool = None
    
    def connect(self):
        """Initialize connection pool"""
        try:
            self.pool = ConnectionPool(
                self.database_url,
                min_size=1,
                max_size=self.pool_size,
                kwargs={
                    "connect_timeout": self.timeout,
                    "application_name": "video_worker"
                }
            )
            logger.info("Postgres job source connection pool initialized")
            self._bootstrap_schema()
        except Exception as e:
            log_exception(logger, f"Failed to connect to Postgres job source: {e}")
            raise
    
    def _bootstrap_schema(self):
        """Validate schema compatibility and run migrations"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Just verify pgvector extension exists
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                
                # Migrate phash column from BIGINT to TEXT if needed
                cur.execute("""
                    SELECT data_type FROM information_schema.columns 
                    WHERE table_name = 'frames' AND column_name = 'phash'
                """)
                result = cur.fetchone()
                if result and result[0] == 'bigint':
                    logger.info("Migrating phash column from BIGINT to TEXT")
                    cur.execute("ALTER TABLE frames ALTER COLUMN phash TYPE TEXT;")
                    logger.info("Successfully migrated phash column to TEXT")
                
                conn.commit()
                logger.info("Postgres job source schema validated and migrated")
    
    def claim_job(self) -> Optional[Job]:
        """Atomically claim a pending job and set video status to processing"""
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    WITH j AS (
                        SELECT id, video_id, created_at, attempts
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
                    RETURNING jobs.id, j.video_id, j.created_at, j.attempts;
                """)
                result = cur.fetchone()
                if result:
                    # Set video status to processing
                    cur.execute("UPDATE videos SET status = 'processing' WHERE id = %s", (result['video_id'],))
                    conn.commit()
                    logger.info(f"Claimed job {result['id']} for video {result['video_id']}")
                    
                    return Job(
                        id=result['id'],
                        video_id=result['video_id'],
                        status='processing',
                        metadata={},
                        created_at=result['created_at'],
                        attempts=result['attempts'] or 0
                    )
                return None
    
    def complete_job(self, job_id: str, video_id: str) -> None:
        """Mark job as done and video as ready"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'done' WHERE id = %s", (job_id,))
                cur.execute("UPDATE videos SET status = 'ready' WHERE id = %s", (video_id,))
                conn.commit()
                logger.info(f"Job {job_id} completed, video {video_id} ready")
    
    def fail_job(self, job_id: str, error: str) -> None:
        """Mark job as failed"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'failed', error = %s WHERE id = %s", (error, job_id))
                conn.commit()
                logger.error(f"Job {job_id} failed: {error}")
    
    def get_job_info(self, job_id: str) -> Optional[Job]:
        """Get information about a specific job"""
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT id, video_id, status, created_at, attempts, error
                    FROM jobs WHERE id = %s
                """, (job_id,))
                result = cur.fetchone()
                if result:
                    return Job(
                        id=result['id'],
                        video_id=result['video_id'],
                        status=result['status'],
                        metadata={},
                        created_at=result['created_at'],
                        attempts=result['attempts'] or 0,
                        error=result['error']
                    )
                return None
    
    def get_pending_jobs(self) -> List[Job]:
        """Get pending jobs for monitoring"""
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT id, video_id, status, created_at, attempts, error
                    FROM jobs
                    WHERE status = 'pending'
                    ORDER BY created_at
                    LIMIT 10
                """)
                results = cur.fetchall()
                return [
                    Job(
                        id=row['id'],
                        video_id=row['video_id'],
                        status=row['status'],
                        metadata={},
                        created_at=row['created_at'],
                        attempts=row['attempts'] or 0,
                        error=row['error']
                    )
                    for row in results
                ]
    
    def close(self):
        """Close connection pool"""
        if self.pool:
            self.pool.close()
            logger.info("Postgres job source connection pool closed")


class PostgresStorageAdapter(StorageAdapter):
    """Postgres implementation of storage adapter"""
    
    def __init__(self, database_url: str, pool_size: int = 5, timeout: int = 10):
        self.database_url = database_url
        self.pool_size = pool_size
        self.timeout = timeout
        self.pool = None
    
    def connect(self):
        """Initialize connection pool"""
        try:
            self.pool = ConnectionPool(
                self.database_url,
                min_size=1,
                max_size=self.pool_size,
                kwargs={
                    "connect_timeout": self.timeout,
                    "application_name": "video_worker_storage"
                }
            )
            logger.info("Postgres storage connection pool initialized")
        except Exception as e:
            log_exception(logger, f"Failed to connect to Postgres storage: {e}")
            raise
    
    def get_video_path(self, video_id: str) -> Optional[str]:
        """Get original_path for a video"""
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                try:
                    cur.execute("SELECT original_path FROM videos WHERE id = %s", (video_id,))
                    result = cur.fetchone()
                    if result and result.get('original_path'):
                        return result['original_path']
                    
                    logger.error(f"No original_path found for video {video_id}")
                    return None
                    
                except Exception as e:
                    log_exception(logger, f"Error getting video path for {video_id}: {e}")
                    return None
    
    def update_video_normalized(self, video_id: str, normalized_path: str, duration_sec: float) -> None:
        """Update video with normalized path and duration"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE videos 
                    SET normalized_path = %s, duration_sec = %s 
                    WHERE id = %s
                """, (normalized_path, duration_sec, video_id))
                conn.commit()
    
    def store_scenes(self, video_id: str, scenes: List[Dict[str, Any]]) -> None:
        """Store scene boundaries with idempotency"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Generate scene IDs and insert
                scene_data = []
                for i, scene in enumerate(scenes):
                    scene_id = f"{video_id}_scene_{i:03d}"
                    scene_data.append((
                        scene_id, video_id, scene['idx'], 
                        scene['t_start'], scene['t_end']
                    ))
                
                cur.executemany("""
                    INSERT INTO scenes (id, video_id, idx, t_start, t_end)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (video_id, idx) DO NOTHING
                """, scene_data)
                conn.commit()
    
    def store_frames(self, video_id: str, frames: List[Dict[str, Any]]) -> None:
        """Store frame records"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Get scene IDs for this video
                cur.execute("SELECT id, idx FROM scenes WHERE video_id = %s ORDER BY idx", (video_id,))
                scene_map = {row[1]: row[0] for row in cur.fetchall()}
                
                # Insert frames with correct scene_id references
                frame_data = []
                for i, frame in enumerate(frames):
                    frame_id = f"{video_id}_frame_{i:03d}"
                    scene_idx = frame['scene_idx']
                    scene_id = scene_map.get(scene_idx)
                    if scene_id:
                        # Store phash as TEXT (no conversion needed)
                        phash_value = frame.get('phash', '')
                        
                        frame_data.append((
                            frame_id, scene_id, frame.get('timestamp', 0.0),
                            frame['path'], phash_value
                        ))
                
                if frame_data:
                    cur.executemany("""
                        INSERT INTO frames (id, scene_id, t_frame, path, phash)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                    """, frame_data)
                    conn.commit()
                    logger.info(f"Inserted {len(frame_data)} frames for video {video_id}")
                else:
                    logger.warning(f"No frames to insert for video {video_id}")
    
    def store_transcripts(self, video_id: str, segments: List[Dict[str, Any]]) -> None:
        """Store transcript segments with idempotency"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Generate segment IDs and insert
                segment_data = []
                for i, segment in enumerate(segments):
                    segment_id = f"{video_id}_segment_{i:03d}"
                    segment_data.append((
                        segment_id, video_id, segment['t_start'], 
                        segment['t_end'], segment['text']
                    ))
                
                cur.executemany("""
                    INSERT INTO transcript_segments (id, video_id, t_start, t_end, text)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (video_id, t_start, t_end) DO NOTHING
                """, segment_data)
                conn.commit()
    
    def store_frame_caption(self, frame_id: str, caption_json: Dict[str, Any]) -> Optional[str]:
        """Store a frame caption"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Generate caption ID
                caption_id = f"{frame_id}_caption"
                
                # Extract caption and entities from JSON
                caption = caption_json.get('caption', '')
                entities = {
                    'controls': caption_json.get('controls', []),
                    'text_on_screen': caption_json.get('text_on_screen', [])
                }
                
                cur.execute("""
                    INSERT INTO frame_captions (id, frame_id, caption, entities)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (caption_id, frame_id, caption, json.dumps(entities)))
                result = cur.fetchone()
                conn.commit()
                return result[0] if result else None
    
    def update_transcript_embedding(self, segment_id: str, embedding: List[float]) -> None:
        """Update transcript segment with embedding"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE transcript_segments 
                    SET embedding = %s 
                    WHERE id = %s
                """, (embedding, segment_id))
                conn.commit()
    
    def update_frame_caption_embedding(self, caption_id: str, embedding: List[float]) -> None:
        """Update frame caption with embedding"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE frame_captions 
                    SET embedding = %s 
                    WHERE id = %s
                """, (embedding, caption_id))
                conn.commit()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics for monitoring"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Get job counts by status
                cur.execute("""
                    SELECT status, COUNT(*) as count
                    FROM jobs
                    GROUP BY status
                """)
                job_counts = {row[0]: row[1] for row in cur.fetchall()}
                
                # Get video counts by status
                cur.execute("""
                    SELECT status, COUNT(*) as count
                    FROM videos
                    GROUP BY status
                """)
                video_counts = {row[0]: row[1] for row in cur.fetchall()}
                
                # Get processing stats
                cur.execute("""
                    SELECT 
                        COUNT(DISTINCT ts.video_id) as videos_with_transcripts,
                        COUNT(ts.id) as total_segments,
                        COUNT(f.id) as total_frames,
                        COUNT(fc.id) as total_captions
                    FROM transcript_segments ts
                    LEFT JOIN scenes s ON ts.video_id = s.video_id
                    LEFT JOIN frames f ON s.id = f.scene_id
                    LEFT JOIN frame_captions fc ON f.id = fc.frame_id
                """)
                stats_row = cur.fetchone()
                
                return {
                    "jobs": job_counts,
                    "videos": video_counts,
                    "processing": {
                        "videos_with_transcripts": stats_row[0] or 0,
                        "total_segments": stats_row[1] or 0,
                        "total_frames": stats_row[2] or 0,
                        "total_captions": stats_row[3] or 0
                    }
                }
    
    def close(self):
        """Close connection pool"""
        if self.pool:
            self.pool.close()
            logger.info("Postgres storage connection pool closed")
