"""
Webhook adapter for push-based job notifications.

Provides HTTP endpoint to receive job notifications and manages
push-based job processing.
"""

import asyncio
import json
import logging
from typing import Optional, Dict, Any, List, Callable
from fastapi import FastAPI, HTTPException
import uvicorn
from threading import Thread

from .base import JobSourceAdapter, Job

logger = logging.getLogger("video_worker")


class WebhookJobSourceAdapter(JobSourceAdapter):
    """Webhook implementation of job source adapter for push-based notifications"""
    
    def __init__(self, webhook_url: str, secret: str = None, port: int = 8080):
        self.webhook_url = webhook_url
        self.secret = secret
        self.port = port
        self.app = None
        self.server_thread = None
        self.running = False
        self.job_callback: Optional[Callable[[Job], None]] = None
    
    def connect(self):
        """Initialize webhook server"""
        try:
            self.app = FastAPI(title="Video Worker Webhook")
            self._setup_routes()
            logger.info(f"Webhook job source initialized on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to initialize webhook: {e}")
            raise
    
    def _setup_routes(self):
        """Setup webhook routes"""
        
        @self.app.post("/webhook/job")
        async def receive_job(job_data: dict):
            """Receive job notification via webhook"""
            try:
                # Validate webhook secret if provided
                if self.secret:
                    # In a real implementation, you'd validate the signature
                    pass
                
                # Parse job data
                job = Job(
                    id=job_data.get('id', ''),
                    video_id=job_data.get('video_id', ''),
                    status='pending',
                    metadata=job_data.get('metadata', {}),
                    created_at=job_data.get('created_at')
                )
                
                logger.info(f"Received webhook job {job.id} for video {job.video_id}")
                
                # Notify callback if set
                if self.job_callback:
                    self.job_callback(job)
                
                return {"status": "received", "job_id": job.id}
                
            except Exception as e:
                logger.error(f"Error processing webhook job: {e}")
                raise HTTPException(status_code=400, detail=str(e))
        
        @self.app.get("/webhook/health")
        async def health_check():
            """Health check endpoint"""
            return {"status": "healthy", "adapter": "webhook"}
    
    def start_server(self, job_callback: Callable[[Job], None]):
        """Start the webhook server"""
        if self.running:
            return
        
        self.job_callback = job_callback
        
        def run_server():
            try:
                uvicorn.run(
                    self.app,
                    host="0.0.0.0",
                    port=self.port,
                    log_level="warning"
                )
            except Exception as e:
                logger.error(f"Webhook server error: {e}")
        
        self.server_thread = Thread(target=run_server, daemon=True)
        self.server_thread.start()
        self.running = True
        
        logger.info(f"Webhook server started on port {self.port}")
    
    def claim_job(self) -> Optional[Job]:
        """Not applicable for push-based webhook adapter"""
        logger.warning("claim_job() not supported for webhook adapter (push-based)")
        return None
    
    def complete_job(self, job_id: str, video_id: str) -> None:
        """Send completion notification"""
        logger.info(f"Job {job_id} completed for video {video_id}")
        # In a real implementation, you might send a callback to the job source
    
    def fail_job(self, job_id: str, error: str) -> None:
        """Send failure notification"""
        logger.error(f"Job {job_id} failed: {error}")
        # In a real implementation, you might send a callback to the job source
    
    def get_job_info(self, job_id: str) -> Optional[Job]:
        """Get job information (not supported for webhook)"""
        logger.warning("get_job_info() not supported for webhook adapter")
        return None
    
    def get_pending_jobs(self) -> List[Job]:
        """Get pending jobs (not supported for webhook)"""
        logger.warning("get_pending_jobs() not supported for webhook adapter")
        return []
    
    def close(self):
        """Stop webhook server"""
        self.running = False
        logger.info("Webhook job source closed")
