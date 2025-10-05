import os
import re
from pathlib import Path
from typing import List, Dict, Any
import hashlib


# Environment variable constants
DEFAULT_DATA_DIR = "/app/data"
DEFAULT_UPLOADS_DIR = "uploads"
DEFAULT_PROCESSED_DIR = "processed"
DEFAULT_FRAMES_DIR = "frames"
DEFAULT_SUBS_DIR = "subs"

def get_data_dir() -> str:
    """Get data directory from environment"""
    return os.getenv("DATA_DIR", DEFAULT_DATA_DIR)


def resolve_video_path(stored_path: str, video_id: str) -> str:
    """Resolve storedPath to absolute path under DATA_DIR"""
    data_dir = get_data_dir()
    
    # If stored_path is already absolute, use it
    if os.path.isabs(stored_path):
        return stored_path
    
    # Otherwise, resolve relative to data directory
    return os.path.join(data_dir, stored_path.lstrip("/"))


def get_video_output_dir(video_id: str) -> str:
    """Get output directory for a video"""
    data_dir = get_data_dir()
    return os.path.join(data_dir, "processed", str(video_id))


def get_frames_dir(video_id: str) -> str:
    """Get frames directory for a video"""
    data_dir = get_data_dir()
    frames_dir = os.path.join(data_dir, "frames", str(video_id))
    os.makedirs(frames_dir, exist_ok=True)
    return frames_dir


def get_subs_dir() -> str:
    """Get subtitles directory"""
    data_dir = get_data_dir()
    subs_dir = os.path.join(data_dir, "subs")
    os.makedirs(subs_dir, exist_ok=True)
    return subs_dir


def format_timecode(seconds: float) -> str:
    """Format seconds as HH:MM:SS.mmm"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"


def parse_timecode(timecode: str) -> float:
    """Parse HH:MM:SS.mmm to seconds"""
    # Handle SRT format: 00:00:01,500 -> 00:00:01.500
    timecode = timecode.replace(',', '.')
    
    # Match HH:MM:SS.mmm
    match = re.match(r'(\d{2}):(\d{2}):(\d{2})\.(\d{3})', timecode)
    if match:
        hours, minutes, seconds, milliseconds = map(int, match.groups())
        return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000.0
    
    return 0.0


def chunk_text_by_tokens(text: str, max_tokens: int = 500, overlap: int = 50) -> List[str]:
    """Split text into chunks with token approximation and overlap"""
    # Rough approximation: 1 token ≈ 4 characters
    max_chars = max_tokens * 4
    overlap_chars = overlap * 4
    
    if len(text) <= max_chars:
        return [text]
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + max_chars
        
        # Try to break at sentence boundary
        if end < len(text):
            # Look for sentence endings within the last 100 characters
            search_start = max(start + max_chars - 100, start)
            for i in range(end, search_start, -1):
                if text[i] in '.!?':
                    end = i + 1
                    break
        
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        
        # Move start with overlap
        start = end - overlap_chars
        if start >= len(text):
            break
    
    return chunks


def generate_phash(image_path: str) -> str:
    """Generate perceptual hash for an image"""
    import imagehash
    from PIL import Image
    
    try:
        with Image.open(image_path) as img:
            hash_value = imagehash.phash(img)
            return str(hash_value)
    except Exception as e:
        print(f"Error generating phash for {image_path}: {e}")
        return ""


def hamming_distance(hash1: str, hash2: str) -> int:
    """Calculate Hamming distance between two hashes"""
    if len(hash1) != len(hash2):
        return float('inf')
    
    return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))


def dedupe_frames_by_phash(frames: List[Dict[str, Any]], threshold: int = 6) -> List[Dict[str, Any]]:
    """Remove near-duplicate frames based on perceptual hash"""
    if not frames:
        return frames
    
    # Sort by scene index to maintain order
    frames.sort(key=lambda x: x.get('scene_idx', 0))
    
    unique_frames = []
    seen_hashes = set()
    
    for frame in frames:
        phash = frame.get('phash', '')
        if not phash:
            continue
        
        # Check against all seen hashes
        is_duplicate = False
        for seen_hash in seen_hashes:
            if hamming_distance(phash, seen_hash) <= threshold:
                is_duplicate = True
                break
        
        if not is_duplicate:
            unique_frames.append(frame)
            seen_hashes.add(phash)
    
    return unique_frames


def ensure_dir(path: str):
    """Ensure directory exists"""
    os.makedirs(path, exist_ok=True)


def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB"""
    try:
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 * 1024)
    except OSError:
        return 0.0


def clean_filename(filename: str) -> str:
    """Clean filename for safe filesystem usage"""
    # Remove or replace unsafe characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove multiple underscores
    filename = re.sub(r'_+', '_', filename)
    # Remove leading/trailing underscores and dots
    filename = filename.strip('_.')
    return filename or 'unnamed'
