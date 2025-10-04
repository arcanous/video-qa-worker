import os
import cv2
import ffmpeg
import logging
from typing import List, Dict, Any
from PIL import Image
from .util import get_frames_dir, generate_phash, dedupe_frames_by_phash, clean_filename

logger = logging.getLogger("video_worker")


def extract_scene_frames(video_path: str, scenes: List[Dict[str, Any]], video_id: str) -> List[Dict[str, Any]]:
    """
    Extract midpoint frames from each scene and deduplicate by perceptual hash
    
    Returns:
        List of frame dictionaries with scene_idx, phash, path
    """
    try:
        frames_dir = get_frames_dir(video_id)
        frames = []
        
        logger.info(f"Extracting frames for video {video_id}: {len(scenes)} scenes")
        
        for scene in scenes:
            scene_idx = scene['idx']
            t_start = scene['t_start']
            t_end = scene['t_end']
            
            # Calculate midpoint
            midpoint = (t_start + t_end) / 2
            
            # Extract frame at midpoint
            frame_path = os.path.join(frames_dir, f"scene_{scene_idx:03d}.jpg")
            
            try:
                # Use ffmpeg to extract frame
                (
                    ffmpeg
                    .input(video_path, ss=midpoint)
                    .output(frame_path, vframes=1, format='image2', vcodec='mjpeg')
                    .overwrite_output()
                    .run(quiet=True)
                )
                
                if os.path.exists(frame_path):
                    # Generate perceptual hash
                    phash = generate_phash(frame_path)
                    
                    frames.append({
                        'scene_idx': scene_idx,
                        'phash': phash,
                        'path': frame_path,
                        'timestamp': midpoint
                    })
                    
                    logger.debug(f"Extracted frame for scene {scene_idx} at {midpoint:.2f}s")
                else:
                    logger.warning(f"Failed to extract frame for scene {scene_idx}")
                    
            except Exception as e:
                logger.warning(f"Error extracting frame for scene {scene_idx}: {str(e)}")
                continue
        
        # Deduplicate frames by perceptual hash
        unique_frames = dedupe_frames_by_phash(frames, threshold=6)
        
        logger.info(f"Frame extraction completed for video {video_id}: {len(frames)} total, {len(unique_frames)} unique")
        
        return unique_frames
        
    except Exception as e:
        error_msg = f"Error extracting frames for video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def extract_frames_by_scene_filter(video_path: str, video_id: str) -> List[Dict[str, Any]]:
    """
    Alternative method: Use FFmpeg scene filter to detect and extract frames
    This is more robust but requires more processing
    """
    try:
        frames_dir = get_frames_dir(video_id)
        frames = []
        
        logger.info(f"Extracting frames with scene filter for video {video_id}")
        
        # Use FFmpeg scene filter to detect scene changes
        # This will output frames when scene change is detected
        output_pattern = os.path.join(frames_dir, "frame_%03d.jpg")
        
        try:
            (
                ffmpeg
                .input(video_path)
                .filter('select', 'gt(scene,0.4)')
                .output(output_pattern, vframes=10)  # Limit to 10 frames max
                .overwrite_output()
                .run(quiet=True)
            )
            
            # Find generated frame files
            frame_files = []
            for i in range(10):  # Check for up to 10 frames
                frame_path = os.path.join(frames_dir, f"frame_{i:03d}.jpg")
                if os.path.exists(frame_path):
                    frame_files.append(frame_path)
            
            # Process each frame
            for i, frame_path in enumerate(frame_files):
                phash = generate_phash(frame_path)
                frames.append({
                    'scene_idx': i,
                    'phash': phash,
                    'path': frame_path,
                    'timestamp': 0.0  # Scene filter doesn't provide timestamps
                })
            
            logger.info(f"Scene filter extracted {len(frames)} frames for video {video_id}")
            
        except Exception as e:
            logger.warning(f"Scene filter method failed for video {video_id}: {str(e)}")
            return []
        
        # Deduplicate
        unique_frames = dedupe_frames_by_phash(frames, threshold=6)
        
        return unique_frames
        
    except Exception as e:
        error_msg = f"Error with scene filter extraction for video {video_id}: {str(e)}"
        logger.error(error_msg)
        return []


def validate_frame_file(frame_path: str) -> bool:
    """Validate that frame file exists and is readable"""
    if not os.path.exists(frame_path):
        return False
    
    try:
        # Try to open with PIL
        with Image.open(frame_path) as img:
            img.verify()
        return True
    except Exception:
        return False


def get_frame_info(frame_path: str) -> Dict[str, Any]:
    """Get frame information (dimensions, file size, etc.)"""
    try:
        with Image.open(frame_path) as img:
            return {
                'width': img.width,
                'height': img.height,
                'format': img.format,
                'mode': img.mode,
                'size_bytes': os.path.getsize(frame_path)
            }
    except Exception as e:
        logger.warning(f"Error getting frame info for {frame_path}: {str(e)}")
        return {}


def cleanup_invalid_frames(frames: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove frames that failed validation"""
    valid_frames = []
    
    for frame in frames:
        if validate_frame_file(frame['path']):
            valid_frames.append(frame)
        else:
            logger.warning(f"Removing invalid frame: {frame['path']}")
            # Try to delete the file
            try:
                os.remove(frame['path'])
            except OSError:
                pass
    
    return valid_frames
