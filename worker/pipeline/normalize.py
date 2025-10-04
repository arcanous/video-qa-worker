import os
import ffmpeg
import logging
from typing import Tuple, Optional
from .util import get_video_output_dir, ensure_dir

logger = logging.getLogger("video_worker")


def normalize_video(input_path: str, video_id: int) -> Tuple[str, str, float]:
    """
    Normalize video to 720p, 30fps and extract audio as WAV
    
    Returns:
        Tuple of (normalized_video_path, audio_path, duration_sec)
    """
    output_dir = get_video_output_dir(video_id)
    ensure_dir(output_dir)
    
    normalized_path = os.path.join(output_dir, "normalized.mp4")
    audio_path = os.path.join(output_dir, "audio.wav")
    
    try:
        # Get video info first
        probe = ffmpeg.probe(input_path)
        video_stream = next(stream for stream in probe['streams'] if stream['codec_type'] == 'video')
        duration = float(video_stream.get('duration', 0))
        
        logger.info(f"Normalizing video {video_id}: {input_path} -> {normalized_path}")
        
        # Normalize video: 720p, 30fps, libx264 crf=22
        (
            ffmpeg
            .input(input_path)
            .video
            .filter('scale', -2, 720)  # Scale to 720p height, maintain aspect ratio
            .filter('fps', 30)         # 30fps
            .output(
                normalized_path,
                vcodec='libx264',
                crf=22,
                preset='medium'
            )
            .overwrite_output()
            .run(quiet=True)
        )
        
        logger.info(f"Extracting audio for video {video_id}: {audio_path}")
        
        # Extract audio: mono, 16kHz, PCM 16-bit
        (
            ffmpeg
            .input(input_path)
            .audio
            .filter('aresample', 16000)  # 16kHz
            .filter('ac', 1)              # mono
            .output(
                audio_path,
                acodec='pcm_s16le',       # 16-bit PCM
                ac=1,                     # mono
                ar=16000                  # 16kHz sample rate
            )
            .overwrite_output()
            .run(quiet=True)
        )
        
        # Verify files exist and get actual duration
        if not os.path.exists(normalized_path) or not os.path.exists(audio_path):
            raise Exception("Normalization failed - output files not created")
        
        # Get actual duration from normalized video
        probe_normalized = ffmpeg.probe(normalized_path)
        video_stream_norm = next(stream for stream in probe_normalized['streams'] if stream['codec_type'] == 'video')
        actual_duration = float(video_stream_norm.get('duration', duration))
        
        logger.info(f"Video {video_id} normalized successfully. Duration: {actual_duration:.2f}s")
        
        return normalized_path, audio_path, actual_duration
        
    except ffmpeg.Error as e:
        error_msg = f"FFmpeg error normalizing video {video_id}: {e.stderr.decode()}"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Error normalizing video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def get_video_duration(video_path: str) -> float:
    """Get video duration in seconds"""
    try:
        probe = ffmpeg.probe(video_path)
        video_stream = next(stream for stream in probe['streams'] if stream['codec_type'] == 'video')
        return float(video_stream.get('duration', 0))
    except Exception as e:
        logger.error(f"Error getting duration for {video_path}: {e}")
        return 0.0


def validate_video_file(video_path: str) -> bool:
    """Validate that video file exists and is readable"""
    if not os.path.exists(video_path):
        logger.error(f"Video file not found: {video_path}")
        return False
    
    try:
        # Try to probe the file
        ffmpeg.probe(video_path)
        return True
    except Exception as e:
        logger.error(f"Video file validation failed for {video_path}: {e}")
        return False
