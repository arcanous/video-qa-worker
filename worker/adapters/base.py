"""
Abstract base classes for job sources and storage adapters.

Defines the interface that all adapters must implement, enabling
easy swapping between different job sources (Postgres, SQS, etc.)
and storage backends (Postgres, S3, etc.).
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from dataclasses import dataclass


@dataclass
class Job:
    """Represents a video processing job"""
    id: str
    video_id: str
    status: str
    metadata: Dict[str, Any]
    created_at: Optional[str] = None
    attempts: int = 0


@dataclass
class VideoMetadata:
    """Represents video metadata"""
    id: str
    original_path: str
    normalized_path: Optional[str] = None
    duration_sec: Optional[float] = None
    status: str = "pending"


@dataclass
class ProcessingResult:
    """Represents the result of video processing"""
    success: bool
    stages_completed: List[str]
    error: Optional[str] = None
    metrics: Dict[str, Any] = None


class JobSourceAdapter(ABC):
    """Abstract base class for job source adapters"""
    
    @abstractmethod
    def claim_job(self) -> Optional[Job]:
        """
        Atomically claim a pending job.
        
        Returns:
            Job object if available, None if no jobs pending
        """
        pass
    
    @abstractmethod
    def complete_job(self, job_id: str, video_id: str) -> None:
        """
        Mark a job as completed.
        
        Args:
            job_id: ID of the job to complete
            video_id: ID of the processed video
        """
        pass
    
    @abstractmethod
    def fail_job(self, job_id: str, error: str) -> None:
        """
        Mark a job as failed.
        
        Args:
            job_id: ID of the failed job
            error: Error message describing the failure
        """
        pass
    
    @abstractmethod
    def get_job_info(self, job_id: str) -> Optional[Job]:
        """
        Get information about a specific job.
        
        Args:
            job_id: ID of the job to retrieve
            
        Returns:
            Job object if found, None otherwise
        """
        pass
    
    @abstractmethod
    def get_pending_jobs(self) -> List[Job]:
        """
        Get list of pending jobs (for monitoring/debugging).
        
        Returns:
            List of pending job objects
        """
        pass


class StorageAdapter(ABC):
    """Abstract base class for storage adapters"""
    
    @abstractmethod
    def get_video_path(self, video_id: str) -> Optional[str]:
        """
        Get the original path for a video.
        
        Args:
            video_id: ID of the video
            
        Returns:
            Original video path if found, None otherwise
        """
        pass
    
    @abstractmethod
    def update_video_normalized(self, video_id: str, normalized_path: str, duration_sec: float) -> None:
        """
        Update video with normalized path and duration.
        
        Args:
            video_id: ID of the video
            normalized_path: Path to normalized video file
            duration_sec: Duration in seconds
        """
        pass
    
    @abstractmethod
    def store_scenes(self, video_id: str, scenes: List[Dict[str, Any]]) -> None:
        """
        Store scene boundaries for a video.
        
        Args:
            video_id: ID of the video
            scenes: List of scene dictionaries with idx, t_start, t_end
        """
        pass
    
    @abstractmethod
    def store_frames(self, video_id: str, frames: List[Dict[str, Any]]) -> None:
        """
        Store frame records for a video.
        
        Args:
            video_id: ID of the video
            frames: List of frame dictionaries with scene_idx, phash, path, timestamp
        """
        pass
    
    @abstractmethod
    def store_transcripts(self, video_id: str, segments: List[Dict[str, Any]]) -> None:
        """
        Store transcript segments for a video.
        
        Args:
            video_id: ID of the video
            segments: List of transcript segments with t_start, t_end, text
        """
        pass
    
    @abstractmethod
    def store_frame_caption(self, frame_id: str, caption_json: Dict[str, Any]) -> Optional[str]:
        """
        Store a frame caption.
        
        Args:
            frame_id: ID of the frame
            caption_json: Caption data with caption, controls, text_on_screen
            
        Returns:
            Caption ID if successful, None otherwise
        """
        pass
    
    @abstractmethod
    def update_transcript_embedding(self, segment_id: str, embedding: List[float]) -> None:
        """
        Update transcript segment with embedding.
        
        Args:
            segment_id: ID of the transcript segment
            embedding: Embedding vector
        """
        pass
    
    @abstractmethod
    def update_frame_caption_embedding(self, caption_id: str, embedding: List[float]) -> None:
        """
        Update frame caption with embedding.
        
        Args:
            caption_id: ID of the frame caption
            embedding: Embedding vector
        """
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics for monitoring.
        
        Returns:
            Dictionary with statistics
        """
        pass
