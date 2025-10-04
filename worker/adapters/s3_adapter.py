"""
AWS S3 adapter for storage.

Provides storage backend for video processing results in S3.
"""

import boto3
import json
import logging
from typing import Optional, Dict, Any, List
from botocore.exceptions import ClientError

from .base import StorageAdapter

logger = logging.getLogger("video_worker")


class S3StorageAdapter(StorageAdapter):
    """AWS S3 implementation of storage adapter"""
    
    def __init__(self, bucket: str, region: str = "us-east-1", prefix: str = "video-processing/"):
        self.bucket = bucket
        self.region = region
        self.prefix = prefix
        self.s3 = None
    
    def connect(self):
        """Initialize S3 client"""
        try:
            self.s3 = boto3.client('s3', region_name=self.region)
            logger.info(f"S3 storage connected to bucket: {self.bucket}")
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            raise
    
    def get_video_path(self, video_id: str) -> Optional[str]:
        """Get video path from S3 metadata"""
        try:
            key = f"{self.prefix}metadata/{video_id}/video_info.json"
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response['Body'].read())
            return data.get('original_path')
        except ClientError as e:
            logger.error(f"Error getting video path for {video_id}: {e}")
            return None
    
    def update_video_normalized(self, video_id: str, normalized_path: str, duration_sec: float) -> None:
        """Update video metadata with normalized info"""
        try:
            key = f"{self.prefix}metadata/{video_id}/video_info.json"
            
            # Get existing metadata
            try:
                response = self.s3.get_object(Bucket=self.bucket, Key=key)
                data = json.loads(response['Body'].read())
            except ClientError:
                data = {}
            
            # Update with normalized info
            data.update({
                'normalized_path': normalized_path,
                'duration_sec': duration_sec,
                'status': 'normalized'
            })
            
            # Store updated metadata
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            
            logger.info(f"Updated video {video_id} metadata in S3")
            
        except ClientError as e:
            logger.error(f"Error updating video metadata for {video_id}: {e}")
            raise
    
    def store_scenes(self, video_id: str, scenes: List[Dict[str, Any]]) -> None:
        """Store scene data in S3"""
        try:
            key = f"{self.prefix}metadata/{video_id}/scenes.json"
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(scenes),
                ContentType='application/json'
            )
            logger.info(f"Stored {len(scenes)} scenes for video {video_id} in S3")
        except ClientError as e:
            logger.error(f"Error storing scenes for {video_id}: {e}")
            raise
    
    def store_frames(self, video_id: str, frames: List[Dict[str, Any]]) -> None:
        """Store frame metadata in S3"""
        try:
            key = f"{self.prefix}metadata/{video_id}/frames.json"
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(frames),
                ContentType='application/json'
            )
            logger.info(f"Stored {len(frames)} frames for video {video_id} in S3")
        except ClientError as e:
            logger.error(f"Error storing frames for {video_id}: {e}")
            raise
    
    def store_transcripts(self, video_id: str, segments: List[Dict[str, Any]]) -> None:
        """Store transcript data in S3"""
        try:
            key = f"{self.prefix}metadata/{video_id}/transcript.json"
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(segments),
                ContentType='application/json'
            )
            logger.info(f"Stored {len(segments)} transcript segments for video {video_id} in S3")
        except ClientError as e:
            logger.error(f"Error storing transcripts for {video_id}: {e}")
            raise
    
    def store_frame_caption(self, frame_id: str, caption_json: Dict[str, Any]) -> Optional[str]:
        """Store frame caption in S3"""
        try:
            caption_id = f"{frame_id}_caption"
            key = f"{self.prefix}captions/{caption_id}.json"
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(caption_json),
                ContentType='application/json'
            )
            
            logger.info(f"Stored frame caption {caption_id} in S3")
            return caption_id
            
        except ClientError as e:
            logger.error(f"Error storing frame caption for {frame_id}: {e}")
            return None
    
    def update_transcript_embedding(self, segment_id: str, embedding: List[float]) -> None:
        """Update transcript embedding in S3"""
        try:
            key = f"{self.prefix}embeddings/transcript/{segment_id}.json"
            
            embedding_data = {
                'segment_id': segment_id,
                'embedding': embedding,
                'dimensions': len(embedding)
            }
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(embedding_data),
                ContentType='application/json'
            )
            
            logger.info(f"Stored transcript embedding for {segment_id} in S3")
            
        except ClientError as e:
            logger.error(f"Error storing transcript embedding for {segment_id}: {e}")
            raise
    
    def update_frame_caption_embedding(self, caption_id: str, embedding: List[float]) -> None:
        """Update frame caption embedding in S3"""
        try:
            key = f"{self.prefix}embeddings/frames/{caption_id}.json"
            
            embedding_data = {
                'caption_id': caption_id,
                'embedding': embedding,
                'dimensions': len(embedding)
            }
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(embedding_data),
                ContentType='application/json'
            )
            
            logger.info(f"Stored frame caption embedding for {caption_id} in S3")
            
        except ClientError as e:
            logger.error(f"Error storing frame caption embedding for {caption_id}: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics from S3"""
        try:
            # List objects in the prefix to get counts
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix
            )
            
            objects = response.get('Contents', [])
            
            # Count different types of objects
            stats = {
                'total_objects': len(objects),
                'metadata_objects': len([obj for obj in objects if 'metadata/' in obj['Key']]),
                'caption_objects': len([obj for obj in objects if 'captions/' in obj['Key']]),
                'embedding_objects': len([obj for obj in objects if 'embeddings/' in obj['Key']]),
                'total_size_bytes': sum(obj['Size'] for obj in objects)
            }
            
            return stats
            
        except ClientError as e:
            logger.error(f"Error getting S3 stats: {e}")
            return {}
    
    def close(self):
        """Close S3 connection"""
        self.s3 = None
        logger.info("S3 storage connection closed")
