import os
import logging
from typing import List, Dict, Any
from scenedetect import VideoManager, SceneManager, AdaptiveDetector
from .util import get_video_output_dir

logger = logging.getLogger("video_worker")


def detect_scenes(video_path: str, video_id: int) -> List[Dict[str, Any]]:
    """
    Detect scene boundaries using PySceneDetect AdaptiveDetector
    
    Returns:
        List of scene dictionaries with idx, t_start, t_end
    """
    try:
        logger.info(f"Detecting scenes for video {video_id}: {video_path}")
        
        # Create video manager and scene manager
        video_manager = VideoManager([video_path])
        scene_manager = SceneManager()
        
        # Add adaptive detector
        scene_manager.add_detector(AdaptiveDetector())
        
        # Start video manager
        video_manager.start()
        
        # Detect scenes
        scene_manager.detect_scenes(frame_source=video_manager)
        
        # Get scene list
        scene_list = scene_manager.get_scene_list()
        
        # Convert to our format
        scenes = []
        for i, (start_time, end_time) in enumerate(scene_list):
            scenes.append({
                'idx': i,
                't_start': start_time.get_seconds(),
                't_end': end_time.get_seconds()
            })
        
        logger.info(f"Scene detection completed for video {video_id}: {len(scenes)} scenes")
        
        # Clean up
        video_manager.release()
        
        return scenes
        
    except Exception as e:
        error_msg = f"Error detecting scenes for video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def export_scene_clips(video_path: str, scenes: List[Dict[str, Any]], video_id: int, max_duration: float = 10.0) -> List[str]:
    """
    Export short clips for each scene (optional, for debugging/preview)
    
    Returns:
        List of exported clip paths
    """
    try:
        output_dir = get_video_output_dir(video_id)
        clips_dir = os.path.join(output_dir, "clips")
        os.makedirs(clips_dir, exist_ok=True)
        
        clip_paths = []
        
        for scene in scenes:
            scene_idx = scene['idx']
            t_start = scene['t_start']
            t_end = scene['t_end']
            
            # Limit clip duration
            duration = min(t_end - t_start, max_duration)
            actual_end = t_start + duration
            
            clip_path = os.path.join(clips_dir, f"scene_{scene_idx:03d}.mp4")
            
            # Use ffmpeg to extract clip
            import ffmpeg
            
            (
                ffmpeg
                .input(video_path, ss=t_start, t=duration)
                .output(clip_path, vcodec='libx264', acodec='aac')
                .overwrite_output()
                .run(quiet=True)
            )
            
            clip_paths.append(clip_path)
        
        logger.info(f"Exported {len(clip_paths)} scene clips for video {video_id}")
        return clip_paths
        
    except Exception as e:
        logger.warning(f"Error exporting scene clips for video {video_id}: {str(e)}")
        return []


def validate_scenes(scenes: List[Dict[str, Any]]) -> bool:
    """Validate scene detection results"""
    if not scenes:
        return False
    
    for scene in scenes:
        if scene.get('t_start', 0) < 0 or scene.get('t_end', 0) <= scene.get('t_start', 0):
            return False
        if 'idx' not in scene:
            return False
    
    # Check for proper ordering
    for i in range(1, len(scenes)):
        if scenes[i]['t_start'] < scenes[i-1]['t_end']:
            return False
    
    return True


def get_scene_at_time(scenes: List[Dict[str, Any]], timestamp: float) -> int:
    """Get scene index for a given timestamp"""
    for scene in scenes:
        if scene['t_start'] <= timestamp < scene['t_end']:
            return scene['idx']
    
    # If timestamp is beyond all scenes, return last scene
    if scenes and timestamp >= scenes[-1]['t_end']:
        return scenes[-1]['idx']
    
    return 0  # Default to first scene
