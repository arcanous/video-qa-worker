"""
Video processing pipeline.

Encapsulates the entire video processing pipeline with clear separation
of concerns and configurable processing steps.
"""

import os
import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from .models import Job, ProcessingResult, VideoMetadata
from .adapters.base import JobSourceAdapter, StorageAdapter
from .config import WorkerConfig
from .pipeline.normalize import normalize_video, validate_video_file
from .pipeline.transcribe import transcribe_audio
from .pipeline.scenes import detect_scenes
from .pipeline.frames import extract_scene_frames
from .pipeline.vision import batch_analyze_frames
from .pipeline.embed import embed_transcript_by_scenes, embed_frame_captions
from .pipeline.util import resolve_video_path
from .logging_setup import log_exception

logger = logging.getLogger("video_worker")


class VideoProcessor:
    """Handles video processing pipeline execution"""
    
    def __init__(self, config: WorkerConfig, job_source: JobSourceAdapter, storage: StorageAdapter):
        self.config = config
        self.job_source = job_source
        self.storage = storage
        self.start_time = None
    
    def process_video(self, job: Job) -> ProcessingResult:
        """
        Process a single video through the complete pipeline.
        
        Args:
            job: Job object containing video processing information
            
        Returns:
            ProcessingResult with success status and metrics
        """
        self.start_time = time.time()
        stages_completed = []
        error = None
        
        try:
            logger.info(f"Processing job {job.id} for video {job.video_id}")
            
            # Get video path
            video_path = self.storage.get_video_path(job.video_id)
            if not video_path:
                raise Exception(f"Video path not found for video {job.video_id}")
            
            # Resolve absolute path
            abs_video_path = resolve_video_path(video_path, job.video_id)
            
            # Validate video file
            if not validate_video_file(abs_video_path):
                raise Exception(f"Invalid video file: {abs_video_path}")
            
            # Step 1: Normalize video and extract audio
            if self.config.ENABLE_TRANSCRIPTION:
                logger.info(f"NORMALIZE: Starting normalization for video {job.video_id}")
                normalized_path, audio_path, duration = self._normalize_video(abs_video_path, job.video_id)
                self.storage.update_video_normalized(job.video_id, normalized_path, duration)
                stages_completed.append("normalize")
            
            # Step 2: Transcribe audio
            if self.config.ENABLE_TRANSCRIPTION:
                logger.info(f"TRANSCRIBE: Starting transcription for video {job.video_id}")
                segments = self._transcribe_audio(audio_path, job.video_id)
                self.storage.store_transcripts(job.video_id, segments)
                stages_completed.append("transcribe")
            
            # Step 3: Detect scenes
            logger.info(f"SCENES: Starting scene detection for video {job.video_id}")
            scenes = self._detect_scenes(normalized_path, job.video_id)
            self.storage.store_scenes(job.video_id, scenes)
            stages_completed.append("scenes")
            
            # Step 4: Extract frames
            logger.info(f"FRAMES: Starting frame extraction for video {job.video_id}")
            frames = self._extract_frames(normalized_path, scenes, job.video_id)
            self.storage.store_frames(job.video_id, frames)
            stages_completed.append("frames")
            
            # Step 5: Analyze frames with vision
            if self.config.ENABLE_VISION_ANALYSIS:
                logger.info(f"VISION: Starting vision analysis for video {job.video_id}")
                frame_analyses = self._analyze_frames(frames, job.video_id)
                caption_ids = self._store_frame_captions(frame_analyses, job.video_id)
                stages_completed.append("vision")
            else:
                frame_analyses = []
                caption_ids = []
            
            # Step 6: Generate embeddings
            if self.config.ENABLE_EMBEDDINGS:
                logger.info(f"EMBEDDINGS: Starting embedding generation for video {job.video_id}")
                self._generate_embeddings(segments, scenes, frame_analyses, caption_ids, job.video_id)
                stages_completed.append("embeddings")
            
            # Calculate processing time
            processing_time = time.time() - self.start_time
            
            logger.info(f"READY: Pipeline completed for video {job.video_id} in {processing_time:.2f}s")
            
            return ProcessingResult(
                success=True,
                stages_completed=stages_completed,
                metrics={
                    'processing_time_sec': processing_time,
                    'scenes_count': len(scenes),
                    'frames_count': len(frames),
                    'transcript_segments': len(segments) if self.config.ENABLE_TRANSCRIPTION else 0,
                    'frame_captions': len(caption_ids) if self.config.ENABLE_VISION_ANALYSIS else 0
                }
            )
            
        except Exception as e:
            error_msg = f"Pipeline failed for video {job.video_id}: {str(e)}"
            log_exception(logger, error_msg)
            
            processing_time = time.time() - self.start_time if self.start_time else 0
            
            return ProcessingResult(
                success=False,
                stages_completed=stages_completed,
                error=error_msg,
                metrics={
                    'processing_time_sec': processing_time,
                    'failed_at_stage': stages_completed[-1] if stages_completed else 'start'
                }
            )
    
    def _normalize_video(self, video_path: str, video_id: str) -> tuple[str, str, float]:
        """Normalize video and extract audio"""
        return normalize_video(video_path, video_id)
    
    def _transcribe_audio(self, audio_path: str, video_id: str) -> List[Dict[str, Any]]:
        """Transcribe audio to text segments"""
        return transcribe_audio(audio_path, video_id)
    
    def _detect_scenes(self, video_path: str, video_id: str) -> List[Dict[str, Any]]:
        """Detect scene boundaries in video"""
        return detect_scenes(video_path, video_id)
    
    def _extract_frames(self, video_path: str, scenes: List[Dict[str, Any]], video_id: str) -> List[Dict[str, Any]]:
        """Extract frames from scenes with configurable limits"""
        # Pass max_frames to the extraction function
        max_frames = self.config.MAX_FRAMES_PER_VIDEO
        return extract_scene_frames(video_path, scenes, video_id, max_frames)
    
    
    def _analyze_frames(self, frames: List[Dict[str, Any]], video_id: str) -> List[Dict[str, Any]]:
        """Analyze frames with vision AI"""
        return batch_analyze_frames(frames, video_id)
    
    def _store_frame_captions(self, frame_analyses: List[Dict[str, Any]], video_id: str) -> List[str]:
        """Store frame captions and return caption IDs"""
        caption_ids = []
        
        for frame_analysis in frame_analyses:
            caption_json = frame_analysis['analysis']
            # Generate frame ID to match what was inserted
            frame_id = f"{video_id}_frame_{frame_analysis['frame_id']:03d}"
            caption_id = self.storage.store_frame_caption(frame_id, caption_json)
            if caption_id:
                caption_ids.append(caption_id)
        
        return caption_ids
    
    def _generate_embeddings(self, segments: List[Dict[str, Any]], scenes: List[Dict[str, Any]], 
                           frame_analyses: List[Dict[str, Any]], caption_ids: List[str], video_id: str) -> None:
        """Generate embeddings for transcripts and frame captions"""
        
        # Embed transcript by scenes
        if self.config.ENABLE_TRANSCRIPTION and segments:
            scene_chunks = embed_transcript_by_scenes(segments, scenes, video_id)
            
            # Update transcript segments with embeddings
            for i, segment in enumerate(segments):
                if i < len(scene_chunks):
                    embedding = scene_chunks[i].get('embedding')
                    if embedding:
                        # Generate segment ID to match what was inserted
                        segment_id = f"{video_id}_segment_{i:03d}"
                        self.storage.update_transcript_embedding(segment_id, embedding)
        
        # Embed frame captions
        if self.config.ENABLE_VISION_ANALYSIS and frame_analyses:
            embedded_frames = embed_frame_captions(frame_analyses, video_id)
            
            # Update frame captions with embeddings
            for i, embedded_frame in enumerate(embedded_frames):
                if i < len(caption_ids):
                    embedding = embedded_frame.get('embedding')
                    if embedding:
                        caption_id = caption_ids[i]
                        self.storage.update_frame_caption_embedding(caption_id, embedding)
