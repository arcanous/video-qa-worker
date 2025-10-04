"""
Domain models for the video worker.

Defines the core data structures used throughout the system,
providing type safety and clear interfaces between components.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime


@dataclass
class Job:
    """Represents a video processing job"""
    id: str
    video_id: str
    status: str
    metadata: Dict[str, Any]
    created_at: Optional[datetime] = None
    attempts: int = 0
    error: Optional[str] = None


@dataclass
class VideoMetadata:
    """Represents video metadata"""
    id: str
    original_path: str
    original_name: Optional[str] = None
    size_bytes: Optional[int] = None
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
    processing_time_sec: Optional[float] = None


@dataclass
class Scene:
    """Represents a video scene"""
    idx: int
    t_start: float
    t_end: float
    scene_id: Optional[str] = None


@dataclass
class Frame:
    """Represents a video frame"""
    scene_idx: int
    phash: str
    path: str
    timestamp: float
    frame_id: Optional[str] = None


@dataclass
class TranscriptSegment:
    """Represents a transcript segment"""
    t_start: float
    t_end: float
    text: str
    segment_id: Optional[str] = None
    embedding: Optional[List[float]] = None


@dataclass
class FrameCaption:
    """Represents a frame caption with analysis"""
    frame_id: str
    caption: str
    controls: List[Dict[str, Any]]
    text_on_screen: List[Dict[str, Any]]
    caption_id: Optional[str] = None
    embedding: Optional[List[float]] = None


@dataclass
class WorkerStats:
    """Represents worker statistics"""
    jobs_processed: int
    jobs_failed: int
    videos_processed: int
    total_processing_time: float
    average_processing_time: float
    current_status: str
