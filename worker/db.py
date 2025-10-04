import os
import json
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from typing import Optional, Dict, Any
import logging
from .logging_setup import log_exception

logger = logging.getLogger("video_worker")


class Database:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None
    
    def connect(self):
        """Initialize connection pool"""
        try:
            self.pool = ConnectionPool(
                self.database_url,
                min_size=1,
                max_size=5,
                kwargs={
                    "connect_timeout": 10,
                    "application_name": "video_worker"
                }
            )
            logger.info("Database connection pool initialized")
            self._bootstrap_schema()
        except Exception as e:
            log_exception(logger, f"Failed to connect to database: {e}")
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
                logger.info("Database schema validated and migrated")
    
    def claim_job(self) -> Optional[Dict[str, Any]]:
        """Atomically claim a pending job and set video status to processing"""
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
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
                """)
                result = cur.fetchone()
                if result:
                    # Set video status to processing
                    cur.execute("UPDATE videos SET status = 'processing' WHERE id = %s", (result['video_id'],))
                    conn.commit()
                    logger.info(f"Claimed job {result['id']} for video {result['video_id']}")
                return result
    
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
    
    def update_video_normalized(self, video_id: str, normalized_path: str, duration_sec: float):
        """Update video with normalized path and duration"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE videos 
                    SET normalized_path = %s, duration_sec = %s 
                    WHERE id = %s
                """, (normalized_path, duration_sec, video_id))
                conn.commit()
    
    def insert_scenes(self, video_id: str, scenes: list):
        """Insert scene boundaries with idempotency"""
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
    
    def insert_frames(self, video_id: str, frames: list):
        """Insert frame records"""
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
    
    def insert_transcript_segments(self, video_id: str, segments: list):
        """Insert transcript segments with idempotency"""
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
    
    def update_transcript_embedding(self, segment_id: str, embedding: list):
        """Update transcript segment with embedding"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE transcript_segments 
                    SET embedding = %s 
                    WHERE id = %s
                """, (embedding, segment_id))
                conn.commit()
    
    def insert_frame_caption(self, frame_id: str, caption_json: dict):
        """Insert frame caption"""
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
    
    def update_frame_caption_embedding(self, caption_id: str, embedding: list):
        """Update frame caption with embedding"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE frame_captions 
                    SET embedding = %s 
                    WHERE id = %s
                """, (embedding, caption_id))
                conn.commit()
    
    def complete_job(self, job_id: str, video_id: str):
        """Mark job as done and video as ready"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'done' WHERE id = %s", (job_id,))
                cur.execute("UPDATE videos SET status = 'ready' WHERE id = %s", (video_id,))
                conn.commit()
                logger.info(f"Job {job_id} completed, video {video_id} ready")
    
    def fail_job(self, job_id: str, error: str):
        """Mark job as failed"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'failed', error = %s WHERE id = %s", (error, job_id))
                conn.commit()
                logger.error(f"Job {job_id} failed: {error}")
    
    def get_pending_jobs(self) -> list:
        """Get pending jobs for peek endpoint"""
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT j.id, j.video_id, j.created_at, v.original_path
                    FROM jobs j
                    JOIN videos v ON j.video_id = v.id
                    WHERE j.status = 'pending'
                    ORDER BY j.created_at
                    LIMIT 10
                """)
                return cur.fetchall()
    
    def close(self):
        """Close connection pool"""
        if self.pool:
            self.pool.close()
            logger.info("Database connection pool closed")
