import os
import logging
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
from threading import Thread

from .adapters.base import JobSourceAdapter, StorageAdapter
from .service import WorkerService

logger = logging.getLogger("video_worker")


class HealthServer:
    def __init__(self, worker_service: WorkerService, port: int = 8000):
        self.worker_service = worker_service
        self.port = port
        self.app = FastAPI(title="Video Worker Health API")
        self.setup_routes()
        self.server_thread = None
        self.running = False
    
    def setup_routes(self):
        """Setup API routes"""
        
        @self.app.get("/healthz")
        async def health_check():
            """Health check endpoint"""
            try:
                # Test adapters
                if hasattr(self.worker_service.job_source, 'get_pending_jobs'):
                    self.worker_service.job_source.get_pending_jobs()
                if hasattr(self.worker_service.storage, 'get_stats'):
                    self.worker_service.storage.get_stats()
                
                return {"ok": True, "status": "healthy"}
            except Exception as e:
                logger.error(f"Health check failed: {str(e)}")
                raise HTTPException(status_code=503, detail=f"Adapter connection failed: {str(e)}")
        
        @self.app.get("/jobs/peek")
        async def peek_jobs():
            """Peek at pending jobs (dev only)"""
            try:
                jobs = self.worker_service.job_source.get_pending_jobs()
                return {
                    "pending_jobs": len(jobs),
                    "jobs": [
                        {
                            "id": job.id,
                            "video_id": job.video_id,
                            "created_at": job.created_at.isoformat() if job.created_at else None,
                            "status": job.status,
                            "attempts": job.attempts
                        }
                        for job in jobs
                    ]
                }
            except Exception as e:
                logger.error(f"Error peeking jobs: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error fetching jobs: {str(e)}")
        
        @self.app.get("/stats")
        async def get_stats():
            """Get worker statistics"""
            try:
                # Get worker stats
                worker_stats = self.worker_service.get_stats()
                
                # Get storage stats if available
                storage_stats = {}
                if hasattr(self.worker_service.storage, 'get_stats'):
                    storage_stats = self.worker_service.storage.get_stats()
                
                return {
                    "worker": worker_stats,
                    "storage": storage_stats
                }
            except Exception as e:
                logger.error(f"Error getting stats: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")
    
    def start(self):
        """Start the HTTP server in a background thread"""
        if self.running:
            return
        
        def run_server():
            try:
                uvicorn.run(
                    self.app,
                    host="0.0.0.0",
                    port=self.port,
                    log_level="warning",  # Reduce uvicorn logging
                    access_log=False
                )
            except Exception as e:
                logger.error(f"HTTP server error: {str(e)}")
        
        self.server_thread = Thread(target=run_server, daemon=True)
        self.server_thread.start()
        self.running = True
        
        logger.info(f"Health server started on port {self.port}")
    
    def stop(self):
        """Stop the HTTP server"""
        self.running = False
        logger.info("Health server stopped")


def start_health_server(worker_service: WorkerService) -> Optional[HealthServer]:
    """Start the health server if enabled"""
    if os.getenv("WORKER_DEV_HTTP", "false").lower() == "true":
        port = int(os.getenv("WORKER_HTTP_PORT", "8000"))
        server = HealthServer(worker_service, port)
        server.start()
        return server
    return None
