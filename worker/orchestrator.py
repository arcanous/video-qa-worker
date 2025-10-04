"""
Pipeline orchestration and execution management.

Handles pipeline execution flow, retries, error recovery, and progress tracking.
Coordinates between VideoProcessor and adapters.
"""

import time
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from .models import Job, ProcessingResult
from .adapters.base import JobSourceAdapter, StorageAdapter
from .processor import VideoProcessor
from .config import WorkerConfig
from .logging_setup import log_exception

logger = logging.getLogger("video_worker")


class PipelineOrchestrator:
    """Manages pipeline execution flow and coordination"""
    
    def __init__(self, config: WorkerConfig, job_source: JobSourceAdapter, storage: StorageAdapter):
        self.config = config
        self.job_source = job_source
        self.storage = storage
        self.processor = VideoProcessor(config, job_source, storage)
        self.stats = {
            'jobs_processed': 0,
            'jobs_failed': 0,
            'total_processing_time': 0.0,
            'start_time': datetime.now()
        }
    
    def execute_pipeline(self, job: Job) -> ProcessingResult:
        """
        Execute the complete video processing pipeline.
        
        Args:
            job: Job to process
            
        Returns:
            ProcessingResult with execution details
        """
        start_time = time.time()
        
        try:
            logger.info(f"Executing pipeline for job {job.id}, video {job.video_id}")
            
            # Track progress
            self._track_progress(job, "started", 0.0)
            
            # Execute processing
            result = self.processor.process_video(job)
            
            # Update statistics
            processing_time = time.time() - start_time
            self.stats['jobs_processed'] += 1
            self.stats['total_processing_time'] += processing_time
            
            if result.success:
                # Mark job as completed
                self.job_source.complete_job(job.id, job.video_id)
                self._track_progress(job, "completed", 100.0)
                logger.info(f"Pipeline completed successfully for job {job.id}")
            else:
                # Handle failure
                self._handle_failure(job, result.error)
                self.stats['jobs_failed'] += 1
                logger.error(f"Pipeline failed for job {job.id}: {result.error}")
            
            return result
            
        except Exception as e:
            error_msg = f"Unexpected error in pipeline execution: {str(e)}"
            log_exception(logger, error_msg)
            
            # Handle unexpected failure
            self._handle_failure(job, error_msg)
            self.stats['jobs_failed'] += 1
            
            return ProcessingResult(
                success=False,
                stages_completed=[],
                error=error_msg,
                metrics={'processing_time_sec': time.time() - start_time}
            )
    
    def _handle_failure(self, job: Job, error: str) -> None:
        """
        Handle job failure with appropriate retry logic.
        
        Args:
            job: Failed job
            error: Error message
        """
        try:
            # Check if job should be retried
            if job.attempts < self.config.MAX_ATTEMPTS:
                logger.warning(f"Job {job.id} failed (attempt {job.attempts + 1}/{self.config.MAX_ATTEMPTS}): {error}")
                
                # Mark job as failed for now, but it could be retried
                self.job_source.fail_job(job.id, f"Attempt {job.attempts + 1} failed: {error}")
            else:
                logger.error(f"Job {job.id} failed permanently after {job.attempts} attempts: {error}")
                self.job_source.fail_job(job.id, f"Permanent failure after {job.attempts} attempts: {error}")
                
        except Exception as e:
            log_exception(logger, f"Error handling job failure for {job.id}: {e}")
    
    def _track_progress(self, job: Job, stage: str, progress: float) -> None:
        """
        Track processing progress for monitoring.
        
        Args:
            job: Job being processed
            stage: Current processing stage
            progress: Progress percentage (0-100)
        """
        try:
            logger.debug(f"Job {job.id} progress: {stage} ({progress:.1f}%)")
            
            # In a real implementation, you might:
            # - Update a progress tracking system
            # - Send metrics to monitoring service
            # - Update job status in database
            
        except Exception as e:
            logger.warning(f"Error tracking progress for job {job.id}: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics"""
        uptime = (datetime.now() - self.stats['start_time']).total_seconds()
        avg_processing_time = (
            self.stats['total_processing_time'] / self.stats['jobs_processed']
            if self.stats['jobs_processed'] > 0 else 0
        )
        
        return {
            'jobs_processed': self.stats['jobs_processed'],
            'jobs_failed': self.stats['jobs_failed'],
            'total_processing_time': self.stats['total_processing_time'],
            'average_processing_time': avg_processing_time,
            'uptime_seconds': uptime,
            'success_rate': (
                self.stats['jobs_processed'] / (self.stats['jobs_processed'] + self.stats['jobs_failed'])
                if (self.stats['jobs_processed'] + self.stats['jobs_failed']) > 0 else 0
            )
        }
    
    def reset_stats(self) -> None:
        """Reset orchestrator statistics"""
        self.stats = {
            'jobs_processed': 0,
            'jobs_failed': 0,
            'total_processing_time': 0.0,
            'start_time': datetime.now()
        }
        logger.info("Orchestrator statistics reset")
