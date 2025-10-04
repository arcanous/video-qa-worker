"""
AWS SQS adapter for job source.

Provides pull-based job polling from SQS queues.
"""

import boto3
import json
import logging
from typing import Optional, Dict, Any, List
from botocore.exceptions import ClientError

from .base import JobSourceAdapter, Job

logger = logging.getLogger("video_worker")


class SQSJobSourceAdapter(JobSourceAdapter):
    """AWS SQS implementation of job source adapter"""
    
    def __init__(self, queue_url: str, region: str = "us-east-1", max_messages: int = 1, wait_time: int = 20):
        self.queue_url = queue_url
        self.region = region
        self.max_messages = max_messages
        self.wait_time = wait_time
        self.sqs = None
    
    def connect(self):
        """Initialize SQS client"""
        try:
            self.sqs = boto3.client('sqs', region_name=self.region)
            logger.info(f"SQS job source connected to queue: {self.queue_url}")
        except Exception as e:
            logger.error(f"Failed to connect to SQS: {e}")
            raise
    
    def claim_job(self) -> Optional[Job]:
        """Poll SQS for messages and claim a job"""
        if not self.sqs:
            raise RuntimeError("SQS client not initialized. Call connect() first.")
        
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.max_messages,
                WaitTimeSeconds=self.wait_time,
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            if not messages:
                return None
            
            # Process first message
            message = messages[0]
            receipt_handle = message['ReceiptHandle']
            
            try:
                # Parse message body
                body = json.loads(message['Body'])
                job_data = body.get('job', {})
                
                # Create job object
                job = Job(
                    id=job_data.get('id', message['MessageId']),
                    video_id=job_data.get('video_id', ''),
                    status='processing',
                    metadata={
                        'receipt_handle': receipt_handle,
                        'message_id': message['MessageId'],
                        'sqs_message': message
                    }
                )
                
                logger.info(f"Claimed SQS job {job.id} for video {job.video_id}")
                return job
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse SQS message: {e}")
                # Delete malformed message
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle
                )
                return None
                
        except ClientError as e:
            logger.error(f"SQS error claiming job: {e}")
            return None
    
    def complete_job(self, job_id: str, video_id: str) -> None:
        """Delete message from SQS queue"""
        if not self.sqs:
            raise RuntimeError("SQS client not initialized")
        
        # Note: In a real implementation, you'd need to track receipt handles
        # This is a simplified version
        logger.info(f"Job {job_id} completed for video {video_id} (SQS message would be deleted)")
    
    def fail_job(self, job_id: str, error: str) -> None:
        """Handle failed job (could send to DLQ or retry)"""
        logger.error(f"Job {job_id} failed: {error}")
        # In a real implementation, you might:
        # 1. Send to dead letter queue
        # 2. Update message visibility timeout for retry
        # 3. Send notification
    
    def get_job_info(self, job_id: str) -> Optional[Job]:
        """Get job information (SQS doesn't support this directly)"""
        # SQS doesn't provide a way to retrieve specific messages
        # This would require external tracking
        logger.warning("SQS doesn't support job info retrieval")
        return None
    
    def get_pending_jobs(self) -> List[Job]:
        """Get pending jobs (SQS doesn't support this directly)"""
        # SQS doesn't provide a way to peek at messages
        # This would require external tracking
        logger.warning("SQS doesn't support pending jobs retrieval")
        return []
    
    def close(self):
        """Close SQS connection"""
        self.sqs = None
        logger.info("SQS job source connection closed")
