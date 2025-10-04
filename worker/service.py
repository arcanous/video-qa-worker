"""
Main worker service.

Replaces the original VideoWorker class with a more flexible, adapter-based
architecture that supports multiple job sources and storage backends.
"""

import os
import time
import signal
import sys
import logging
from typing import Optional, Dict, Any
from threading import Thread

from .config import WorkerConfig
from .adapters.base import JobSourceAdapter, StorageAdapter
from .adapters.postgres_adapter import PostgresJobSourceAdapter, PostgresStorageAdapter
from .adapters.sqs_adapter import SQSJobSourceAdapter
from .adapters.s3_adapter import S3StorageAdapter
from .adapters.webhook_adapter import WebhookJobSourceAdapter
from .orchestrator import PipelineOrchestrator
from .logging_setup import setup_logging, log_exception
from .http_server import start_health_server

logger = logging.getLogger("video_worker")


class WorkerService:
    """Main worker service with adapter-based architecture"""
    
    def __init__(self, config: Optional[WorkerConfig] = None):
        self.config = config or WorkerConfig.from_env()
        self.job_source: Optional[JobSourceAdapter] = None
        self.storage: Optional[StorageAdapter] = None
        self.orchestrator: Optional[PipelineOrchestrator] = None
        self.health_server = None
        self.running = False
        self.backoff_interval = self.config.POLL_INTERVAL_MS
        self.max_backoff = self.config.MAX_BACKOFF_MS
    
    def initialize(self):
        """Initialize worker with adapters based on configuration"""
        try:
            # Setup logging
            setup_logging(self.config.LOG_LEVEL)
            
            # Validate configuration
            self.config.validate()
            
            # Initialize adapters
            self._initialize_adapters()
            
            # Initialize orchestrator
            self.orchestrator = PipelineOrchestrator(self.config, self.job_source, self.storage)
            
            # Start health server if enabled
            self.health_server = start_health_server(self)
            
            logger.info("Worker service initialized successfully")
            
        except Exception as e:
            log_exception(logger, f"Failed to initialize worker service: {e}")
            raise
    
    def _initialize_adapters(self):
        """Initialize job source and storage adapters based on configuration"""
        
        # Initialize job source adapter
        self.job_source = self._create_job_source_adapter()
        self.job_source.connect()
        
        # Initialize storage adapter
        self.storage = self._create_storage_adapter()
        self.storage.connect()
        
        logger.info(f"Initialized adapters: {self.config.JOB_SOURCE_TYPE} job source, {self.config.STORAGE_TYPE} storage")
    
    def _create_job_source_adapter(self) -> JobSourceAdapter:
        """Create job source adapter based on configuration"""
        
        if self.config.JOB_SOURCE_TYPE == "postgres":
            config = self.config.JOB_SOURCE_CONFIG
            return PostgresJobSourceAdapter(
                database_url=config["database_url"],
                pool_size=config.get("connection_pool_size", 5),
                timeout=config.get("connection_timeout", 10)
            )
        
        elif self.config.JOB_SOURCE_TYPE == "sqs":
            config = self.config.JOB_SOURCE_CONFIG
            return SQSJobSourceAdapter(
                queue_url=config["queue_url"],
                region=config.get("region", "us-east-1"),
                max_messages=config.get("max_messages", 1),
                wait_time=config.get("wait_time_seconds", 20)
            )
        
        elif self.config.JOB_SOURCE_TYPE == "webhook":
            config = self.config.JOB_SOURCE_CONFIG
            return WebhookJobSourceAdapter(
                webhook_url=config["webhook_url"],
                secret=config.get("secret"),
                port=config.get("port", 8080)
            )
        
        else:
            raise ValueError(f"Unsupported job source type: {self.config.JOB_SOURCE_TYPE}")
    
    def _create_storage_adapter(self) -> StorageAdapter:
        """Create storage adapter based on configuration"""
        
        if self.config.STORAGE_TYPE == "postgres":
            config = self.config.STORAGE_CONFIG
            return PostgresStorageAdapter(
                database_url=config["database_url"],
                pool_size=config.get("connection_pool_size", 5),
                timeout=config.get("connection_timeout", 10)
            )
        
        elif self.config.STORAGE_TYPE == "s3":
            config = self.config.STORAGE_CONFIG
            return S3StorageAdapter(
                bucket=config["bucket"],
                region=config.get("region", "us-east-1"),
                prefix=config.get("prefix", "video-processing/")
            )
        
        else:
            raise ValueError(f"Unsupported storage type: {self.config.STORAGE_TYPE}")
    
    def start(self):
        """Start the worker service"""
        if self.running:
            logger.warning("Worker service is already running")
            return
        
        self.running = True
        
        if self.config.JOB_SOURCE_TYPE == "webhook":
            # Start webhook server for push-based processing
            self._start_webhook_server()
        else:
            # Start polling loop for pull-based processing
            self._start_polling_loop()
        
        logger.info("Worker service started")
    
    def _start_polling_loop(self):
        """Start the polling loop for pull-based job sources"""
        logger.info("Worker started, polling for jobs...")
        
        while self.running:
            try:
                # Try to process a job
                processed = self.run_once()
                
                if not processed:
                    # No job available, use exponential backoff
                    time.sleep(self.backoff_interval / 1000.0)
                    # Increase backoff interval (exponential backoff)
                    self.backoff_interval = min(
                        self.backoff_interval * self.config.BACKOFF_MULTIPLIER, 
                        self.max_backoff
                    )
                else:
                    # Reset backoff on successful processing
                    self.backoff_interval = self.config.POLL_INTERVAL_MS
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                log_exception(logger, f"Unexpected error in worker loop: {str(e)}")
                # Use backoff for errors too
                time.sleep(self.backoff_interval / 1000.0)
                self.backoff_interval = min(
                    self.backoff_interval * self.config.BACKOFF_MULTIPLIER, 
                    self.max_backoff
                )
        
        logger.info("Worker polling loop stopped")
    
    def _start_webhook_server(self):
        """Start webhook server for push-based job processing"""
        if not isinstance(self.job_source, WebhookJobSourceAdapter):
            raise ValueError("Webhook adapter not configured")
        
        def job_callback(job):
            """Handle incoming webhook job"""
            try:
                logger.info(f"Received webhook job {job.id} for video {job.video_id}")
                result = self.orchestrator.execute_pipeline(job)
                if result.success:
                    logger.info(f"Webhook job {job.id} completed successfully")
                else:
                    logger.error(f"Webhook job {job.id} failed: {result.error}")
            except Exception as e:
                log_exception(logger, f"Error processing webhook job {job.id}: {e}")
        
        self.job_source.start_server(job_callback)
    
    def run_once(self) -> bool:
        """
        Run one iteration of the worker loop.
        
        Returns:
            True if a job was processed, False if no job available
        """
        try:
            # Claim a job
            job = self.job_source.claim_job()
            if not job:
                return False
            
            # Reset backoff on successful job claim
            self.backoff_interval = self.config.POLL_INTERVAL_MS
            
            # Process the job
            result = self.orchestrator.execute_pipeline(job)
            
            return result.success
            
        except Exception as e:
            log_exception(logger, f"Error in worker loop: {str(e)}")
            return False
    
    def stop(self):
        """Stop the worker service"""
        if not self.running:
            return
        
        self.running = False
        
        # Stop health server
        if self.health_server:
            self.health_server.stop()
        
        # Close adapters
        if self.job_source:
            self.job_source.close()
        if self.storage:
            self.storage.close()
        
        logger.info("Worker service stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics"""
        stats = {
            'running': self.running,
            'config': {
                'job_source_type': self.config.JOB_SOURCE_TYPE,
                'storage_type': self.config.STORAGE_TYPE,
                'max_frames_per_video': self.config.MAX_FRAMES_PER_VIDEO,
                'poll_interval_ms': self.config.POLL_INTERVAL_MS
            }
        }
        
        if self.orchestrator:
            stats['orchestrator'] = self.orchestrator.get_stats()
        
        return stats
    
    def reset_stats(self):
        """Reset worker statistics"""
        if self.orchestrator:
            self.orchestrator.reset_stats()
        logger.info("Worker statistics reset")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)


def main():
    """Main entry point"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker = WorkerService()
    
    try:
        worker.initialize()
        worker.start()
    except Exception as e:
        log_exception(logger, f"Worker failed to start: {str(e)}")
        sys.exit(1)
    finally:
        worker.stop()


if __name__ == "__main__":
    main()
