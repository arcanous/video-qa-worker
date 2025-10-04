import os
import logging
from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
from threading import Thread

from .db import Database

logger = logging.getLogger("video_worker")


class HealthServer:
    def __init__(self, db: Database, port: int = 8000):
        self.db = db
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
                # Test database connection
                with self.db.pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                
                return {"ok": True, "status": "healthy"}
            except Exception as e:
                logger.error(f"Health check failed: {str(e)}")
                raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")
        
        @self.app.get("/jobs/peek")
        async def peek_jobs():
            """Peek at pending jobs (dev only)"""
            try:
                jobs = self.db.get_pending_jobs()
                return {
                    "pending_jobs": len(jobs),
                    "jobs": [
                        {
                            "id": job["id"],
                            "video_id": job["video_id"],
                            "created_at": job["created_at"].isoformat() if job["created_at"] else None,
                            "stored_path": job["storedpath"]
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
                with self.db.pool.connection() as conn:
                    with conn.cursor() as cur:
                        # Get job counts by status
                        cur.execute("""
                            SELECT status, COUNT(*) as count
                            FROM jobs
                            GROUP BY status
                        """)
                        job_counts = {row[0]: row[1] for row in cur.fetchall()}
                        
                        # Get video counts by status
                        cur.execute("""
                            SELECT status, COUNT(*) as count
                            FROM videos
                            GROUP BY status
                        """)
                        video_counts = {row[0]: row[1] for row in cur.fetchall()}
                        
                        # Get processing stats
                        cur.execute("""
                            SELECT 
                                COUNT(DISTINCT ts.video_id) as videos_with_transcripts,
                                COUNT(ts.id) as total_segments,
                                COUNT(f.id) as total_frames,
                                COUNT(fc.id) as total_captions
                            FROM transcript_segments ts
                            LEFT JOIN scenes s ON ts.video_id = s.video_id
                            LEFT JOIN frames f ON s.id = f.scene_id
                            LEFT JOIN frame_captions fc ON f.id = fc.frame_id
                        """)
                        stats_row = cur.fetchone()
                        
                        return {
                            "jobs": job_counts,
                            "videos": video_counts,
                            "processing": {
                                "videos_with_transcripts": stats_row[0] or 0,
                                "total_segments": stats_row[1] or 0,
                                "total_frames": stats_row[2] or 0,
                                "total_captions": stats_row[3] or 0
                            }
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


def start_health_server(db: Database) -> HealthServer:
    """Start the health server if enabled"""
    if os.getenv("WORKER_DEV_HTTP", "false").lower() == "true":
        port = int(os.getenv("WORKER_HTTP_PORT", "8000"))
        server = HealthServer(db, port)
        server.start()
        return server
    return None
