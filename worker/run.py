import os
import time
import logging
import signal
import sys
from typing import Optional
from threading import Thread

from .db import Database
from .logging_setup import setup_logging, log_exception
from .http_server import start_health_server
from .pipeline.normalize import normalize_video, validate_video_file
from .pipeline.transcribe import transcribe_audio
from .pipeline.scenes import detect_scenes
from .pipeline.frames import extract_scene_frames
from .pipeline.vision import batch_analyze_frames
from .pipeline.embed import embed_transcript_by_scenes, embed_frame_captions
from .pipeline.util import resolve_video_path

logger = logging.getLogger("video_worker")


class VideoWorker:
    def __init__(self):
        self.db = None
        self.health_server = None
        self.running = False
        self.poll_interval = int(os.getenv("WORKER_POLL_MS", "1500"))
        self.max_attempts = int(os.getenv("WORKER_MAX_ATTEMPTS", "3"))
        self.backoff_interval = self.poll_interval
        self.max_backoff = 12000  # 12 seconds max
        
    def initialize(self):
        """Initialize database connection and logging"""
        # Setup logging
        log_level = os.getenv("LOG_LEVEL", "INFO")
        setup_logging(log_level)
        
        # Check required environment variables
        required_env_vars = {
            "DATABASE_URL": os.getenv("DATABASE_URL"),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY")
        }
        
        missing_vars = [var for var, value in required_env_vars.items() if not value]
        if missing_vars:
            raise Exception(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Initialize database
        self.db = Database(required_env_vars["DATABASE_URL"])
        self.db.connect()
        
        # Start health server if enabled
        self.health_server = start_health_server(self.db)
        
        logger.info("Video worker initialized")
    
    def process_video(self, job_id: int, video_id: int) -> bool:
        """
        Process a video through the complete 6-stage pipeline.
        
        Pipeline stages (in order):
        1. NORMALIZE: Convert to 720p/30fps, extract mono audio
        2. TRANSCRIBE: Generate transcript segments using Whisper
        3. SCENES: Detect scene boundaries using PySceneDetect
        4. FRAMES: Extract representative frames, deduplicate by phash
        5. VISION: Analyze frames with GPT-4o for captions/entities
        6. EMBEDDINGS: Generate 1536-dim vectors for search
        
        Database coupling:
        - Reads: videos.original_path
        - Writes: All tables (scenes, frames, transcript_segments, frame_captions)
        - Updates: videos.status, videos.normalized_path, videos.duration_sec
        
        Returns:
            True if pipeline completed successfully, False otherwise
        """
        try:
            logger.info(f"CLAIMED: Processing job {job_id} for video {video_id}")
            
            # Fetch relative path from DB (e.g., "uploads/{id}_{name}.mp4")
            video_path = self.db.get_video_path(video_id)
            if not video_path:
                raise Exception(f"Video path not found for video {video_id}")
            
            # Resolve to absolute path using DATA_DIR environment variable
            abs_video_path = resolve_video_path(video_path, video_id)
            
            # Validate video file exists and is readable
            if not validate_video_file(abs_video_path):
                raise Exception(f"Invalid video file: {abs_video_path}")
            
            logger.info(f"NORMALIZED: Starting normalization for video {video_id}")
            
            # Stage 1: Normalize video and extract audio
            normalized_path, audio_path, duration = normalize_video(abs_video_path, video_id)
            
            # Update video with normalized path and duration
            self.db.update_video_normalized(video_id, normalized_path, duration)
            
            logger.info(f"TRANSCRIBED: Starting transcription for video {video_id}")
            
            # Stage 2: Transcribe audio using OpenAI Whisper
            segments = transcribe_audio(audio_path, video_id)
            
            # Insert transcript segments with idempotency
            self.db.insert_transcript_segments(video_id, segments)
            
            logger.info(f"SCENES: Starting scene detection for video {video_id}")
            
            # Stage 3: Detect scene boundaries using PySceneDetect
            scenes = detect_scenes(normalized_path, video_id)
            
            # Insert scenes with idempotency
            self.db.insert_scenes(video_id, scenes)
            
            logger.info(f"FRAMES: Starting frame extraction for video {video_id}")
            
            # Stage 4: Extract representative frames and deduplicate
            frames = extract_scene_frames(normalized_path, scenes, video_id)
            
            # Insert frames with perceptual hashes for deduplication
            self.db.insert_frames(video_id, frames)
            
            logger.info(f"VISION: Starting vision analysis for video {video_id}")
            
            # Stage 5: Analyze frames with GPT-4o Vision
            frame_analyses = batch_analyze_frames(frames, video_id)
            
            # Insert frame captions with structured output
            caption_ids = []
            for frame_analysis in frame_analyses:
                caption_json = frame_analysis['analysis']
                # Generate frame ID to match what was inserted
                frame_id = f"{video_id}_frame_{frame_analysis['frame_id']:03d}"
                caption_id = self.db.insert_frame_caption(frame_id, caption_json)
                if caption_id:
                    caption_ids.append(caption_id)
            
            logger.info(f"EMBEDDINGS: Starting embedding generation for video {video_id}")
            
            # Stage 6: Generate 1536-dimensional embeddings for search
            # Embed transcript by scenes
            scene_chunks = embed_transcript_by_scenes(segments, scenes, video_id)
            
            # Update transcript segments with embeddings
            for i, segment in enumerate(segments):
                if i < len(scene_chunks):
                    embedding = scene_chunks[i].get('embedding')
                    if embedding:
                        # Generate segment ID to match what was inserted
                        segment_id = f"{video_id}_segment_{i:03d}"
                        self.db.update_transcript_embedding(segment_id, embedding)
            
            # Embed frame captions
            embedded_frames = embed_frame_captions(frame_analyses, video_id)
            
            # Update frame captions with embeddings
            for i, embedded_frame in enumerate(embedded_frames):
                if i < len(caption_ids):
                    embedding = embedded_frame.get('embedding')
                    if embedding:
                        caption_id = caption_ids[i]
                        self.db.update_frame_caption_embedding(caption_id, embedding)
            
            logger.info(f"READY: Pipeline completed for video {video_id}")
            
            # Mark job as completed and video as ready
            self.db.complete_job(job_id, video_id)
            
            return True
            
        except Exception as e:
            error_msg = f"Pipeline failed for video {video_id}: {str(e)}"
            log_exception(logger, error_msg)
            
            # Mark job as failed
            self.db.fail_job(job_id, error_msg)
            
            return False
    
    def run_once(self) -> bool:
        """Run one iteration of the worker loop"""
        try:
            # Claim a job
            job = self.db.claim_job()
            if not job:
                return False
            
            job_id = job['id']
            video_id = job['video_id']
            
            # Reset backoff on successful job claim
            self.backoff_interval = self.poll_interval
            
            # Process the video
            success = self.process_video(job_id, video_id)
            
            return success
            
        except Exception as e:
            log_exception(logger, f"Error in worker loop: {str(e)}")
            return False
    
    def run(self):
        """Main worker loop"""
        self.running = True
        logger.info("Worker started, polling for jobs...")
        
        while self.running:
            try:
                # Try to process a job
                processed = self.run_once()
                
                if not processed:
                    # No job available, use exponential backoff
                    time.sleep(self.backoff_interval / 1000.0)
                    # Increase backoff interval (exponential backoff)
                    self.backoff_interval = min(self.backoff_interval * 1.5, self.max_backoff)
                else:
                    # Reset backoff on successful processing
                    self.backoff_interval = self.poll_interval
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                log_exception(logger, f"Unexpected error in worker loop: {str(e)}")
                # Use backoff for errors too
                time.sleep(self.backoff_interval / 1000.0)
                self.backoff_interval = min(self.backoff_interval * 1.5, self.max_backoff)
        
        logger.info("Worker stopped")
    
    def stop(self):
        """Stop the worker"""
        self.running = False
        if self.health_server:
            self.health_server.stop()
        if self.db:
            self.db.close()


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)


def main():
    """Main entry point"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker = VideoWorker()
    
    try:
        worker.initialize()
        worker.run()
    except Exception as e:
        log_exception(logger, f"Worker failed to start: {str(e)}")
        sys.exit(1)
    finally:
        worker.stop()


if __name__ == "__main__":
    main()
