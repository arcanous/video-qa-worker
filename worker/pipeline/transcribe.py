import os
import json
import logging
from typing import List, Dict, Any
from openai import OpenAI
from .util import get_subs_dir, format_timecode

logger = logging.getLogger("video_worker")


def transcribe_audio(audio_path: str, video_id: str) -> List[Dict[str, Any]]:
    """
    Transcribe audio using OpenAI Whisper and return segments
    
    Returns:
        List of segment dictionaries with t_start, t_end, text
    """
    try:
        client = OpenAI()
        
        logger.info(f"Transcribing audio for video {video_id}: {audio_path}")
        
        with open(audio_path, 'rb') as audio_file:
            transcript = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                response_format="verbose_json",
                timestamp_granularities=["segment"]
            )
        
        segments = []
        if hasattr(transcript, 'segments') and transcript.segments:
            for segment in transcript.segments:
                segments.append({
                    't_start': segment.start,
                    't_end': segment.end,
                    'text': segment.text.strip()
                })
        else:
            # Fallback if no segments
            segments.append({
                't_start': 0.0,
                't_end': transcript.duration if hasattr(transcript, 'duration') else 0.0,
                'text': transcript.text
            })
        
        logger.info(f"Transcription completed for video {video_id}: {len(segments)} segments")
        
        # Save SRT file
        srt_path = save_srt_file(segments, video_id)
        logger.info(f"SRT file saved: {srt_path}")
        
        return segments
        
    except Exception as e:
        error_msg = f"Error transcribing audio for video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def save_srt_file(segments: List[Dict[str, Any]], video_id: str) -> str:
    """Save segments as SRT subtitle file"""
    subs_dir = get_subs_dir()
    srt_path = os.path.join(subs_dir, f"{video_id}.srt")
    
    with open(srt_path, 'w', encoding='utf-8') as f:
        for i, segment in enumerate(segments, 1):
            start_time = format_timecode(segment['t_start'])
            end_time = format_timecode(segment['t_end'])
            text = segment['text']
            
            f.write(f"{i}\n")
            f.write(f"{start_time} --> {end_time}\n")
            f.write(f"{text}\n\n")
    
    return srt_path


def chunk_transcript_by_scenes(segments: List[Dict[str, Any]], scenes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Group transcript segments by scenes for embedding generation
    
    Returns:
        List of chunked segments with scene context
    """
    if not scenes:
        # If no scenes, return all segments as one chunk
        return [{
            't_start': segments[0]['t_start'] if segments else 0,
            't_end': segments[-1]['t_end'] if segments else 0,
            'text': ' '.join(seg['text'] for seg in segments),
            'scene_idx': 0
        }]
    
    chunked_segments = []
    
    for scene_idx, scene in enumerate(scenes):
        scene_start = scene['t_start']
        scene_end = scene['t_end']
        
        # Find segments that overlap with this scene
        scene_segments = []
        for segment in segments:
            # Check for overlap
            if (segment['t_start'] < scene_end and segment['t_end'] > scene_start):
                scene_segments.append(segment)
        
        if scene_segments:
            # Combine text from overlapping segments
            combined_text = ' '.join(seg['text'] for seg in scene_segments)
            
            chunked_segments.append({
                't_start': scene_start,
                't_end': scene_end,
                'text': combined_text,
                'scene_idx': scene_idx
            })
    
    return chunked_segments


def get_transcript_text(segments: List[Dict[str, Any]]) -> str:
    """Get full transcript text from segments"""
    return ' '.join(segment['text'] for segment in segments)


def validate_transcription(segments: List[Dict[str, Any]]) -> bool:
    """Validate transcription results"""
    if not segments:
        return False
    
    for segment in segments:
        if not segment.get('text', '').strip():
            return False
        if segment.get('t_start', 0) < 0 or segment.get('t_end', 0) <= segment.get('t_start', 0):
            return False
    
    return True
