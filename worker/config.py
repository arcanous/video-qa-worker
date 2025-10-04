"""
Configuration management for the video worker.

Centralizes all configuration loading from environment variables
and provides type-safe access to configuration values.
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class WorkerConfig:
    """Configuration for the video worker"""
    
    # Job source settings
    JOB_SOURCE_TYPE: str = "postgres"  # postgres, sqs, webhook
    JOB_SOURCE_CONFIG: Dict[str, Any] = None
    
    # Storage settings
    STORAGE_TYPE: str = "postgres"  # postgres, s3, hybrid
    STORAGE_CONFIG: Dict[str, Any] = None
    
    # Processing settings
    MAX_FRAMES_PER_VIDEO: int = 50
    POLL_INTERVAL_MS: int = 1500
    MAX_ATTEMPTS: int = 3
    BACKOFF_MULTIPLIER: float = 1.5
    MAX_BACKOFF_MS: int = 12000
    
    # Pipeline settings
    ENABLE_TRANSCRIPTION: bool = True
    ENABLE_VISION_ANALYSIS: bool = True
    ENABLE_EMBEDDINGS: bool = True
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    # HTTP server
    ENABLE_HTTP_SERVER: bool = False
    HTTP_PORT: int = 8000
    
    # Data directory
    DATA_DIR: str = "/app/data"
    
    @classmethod
    def from_env(cls) -> 'WorkerConfig':
        """Load configuration from environment variables"""
        config = cls()
        
        # Job source configuration
        config.JOB_SOURCE_TYPE = os.getenv("JOB_SOURCE_TYPE", "postgres")
        config.JOB_SOURCE_CONFIG = cls._parse_job_source_config()
        
        # Storage configuration
        config.STORAGE_TYPE = os.getenv("STORAGE_TYPE", "postgres")
        config.STORAGE_CONFIG = cls._parse_storage_config()
        
        # Processing settings
        config.MAX_FRAMES_PER_VIDEO = int(os.getenv("MAX_FRAMES_PER_VIDEO", "50"))
        config.POLL_INTERVAL_MS = int(os.getenv("WORKER_POLL_MS", "1500"))
        config.MAX_ATTEMPTS = int(os.getenv("WORKER_MAX_ATTEMPTS", "3"))
        config.BACKOFF_MULTIPLIER = float(os.getenv("WORKER_BACKOFF_MULTIPLIER", "1.5"))
        config.MAX_BACKOFF_MS = int(os.getenv("WORKER_MAX_BACKOFF_MS", "12000"))
        
        # Pipeline settings
        config.ENABLE_TRANSCRIPTION = os.getenv("ENABLE_TRANSCRIPTION", "true").lower() == "true"
        config.ENABLE_VISION_ANALYSIS = os.getenv("ENABLE_VISION_ANALYSIS", "true").lower() == "true"
        config.ENABLE_EMBEDDINGS = os.getenv("ENABLE_EMBEDDINGS", "true").lower() == "true"
        
        # Logging
        config.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        
        # HTTP server
        config.ENABLE_HTTP_SERVER = os.getenv("WORKER_DEV_HTTP", "false").lower() == "true"
        config.HTTP_PORT = int(os.getenv("WORKER_HTTP_PORT", "8000"))
        
        # Data directory
        config.DATA_DIR = os.getenv("DATA_DIR", "/app/data")
        
        return config
    
    @classmethod
    def _parse_job_source_config(cls) -> Dict[str, Any]:
        """Parse job source specific configuration"""
        job_source_type = os.getenv("JOB_SOURCE_TYPE", "postgres")
        
        if job_source_type == "postgres":
            return {
                "database_url": os.getenv("DATABASE_URL"),
                "connection_pool_size": int(os.getenv("POSTGRES_POOL_SIZE", "5")),
                "connection_timeout": int(os.getenv("POSTGRES_TIMEOUT", "10"))
            }
        elif job_source_type == "sqs":
            return {
                "queue_url": os.getenv("AWS_SQS_QUEUE_URL"),
                "region": os.getenv("AWS_REGION", "us-east-1"),
                "max_messages": int(os.getenv("SQS_MAX_MESSAGES", "1")),
                "wait_time_seconds": int(os.getenv("SQS_WAIT_TIME", "20"))
            }
        elif job_source_type == "webhook":
            return {
                "webhook_url": os.getenv("WEBHOOK_URL"),
                "secret": os.getenv("WEBHOOK_SECRET"),
                "port": int(os.getenv("WEBHOOK_PORT", "8080"))
            }
        else:
            return {}
    
    @classmethod
    def _parse_storage_config(cls) -> Dict[str, Any]:
        """Parse storage specific configuration"""
        storage_type = os.getenv("STORAGE_TYPE", "postgres")
        
        if storage_type == "postgres":
            return {
                "database_url": os.getenv("DATABASE_URL"),
                "connection_pool_size": int(os.getenv("POSTGRES_POOL_SIZE", "5")),
                "connection_timeout": int(os.getenv("POSTGRES_TIMEOUT", "10"))
            }
        elif storage_type == "s3":
            return {
                "bucket": os.getenv("AWS_S3_BUCKET"),
                "region": os.getenv("AWS_REGION", "us-east-1"),
                "prefix": os.getenv("S3_PREFIX", "video-processing/")
            }
        elif storage_type == "hybrid":
            return {
                "metadata_storage": os.getenv("METADATA_STORAGE", "postgres"),
                "file_storage": os.getenv("FILE_STORAGE", "s3"),
                "postgres_url": os.getenv("DATABASE_URL"),
                "s3_bucket": os.getenv("AWS_S3_BUCKET"),
                "s3_region": os.getenv("AWS_REGION", "us-east-1")
            }
        else:
            return {}
    
    def validate(self) -> None:
        """Validate configuration and raise errors for missing required values"""
        required_vars = []
        
        # Check required environment variables based on configuration
        if self.JOB_SOURCE_TYPE == "postgres" and not self.JOB_SOURCE_CONFIG.get("database_url"):
            required_vars.append("DATABASE_URL")
        
        if self.STORAGE_TYPE == "postgres" and not self.STORAGE_CONFIG.get("database_url"):
            required_vars.append("DATABASE_URL")
        
        if self.STORAGE_TYPE == "s3" and not self.STORAGE_CONFIG.get("bucket"):
            required_vars.append("AWS_S3_BUCKET")
        
        if self.JOB_SOURCE_TYPE == "sqs" and not self.JOB_SOURCE_CONFIG.get("queue_url"):
            required_vars.append("AWS_SQS_QUEUE_URL")
        
        # Check for OpenAI API key if vision analysis is enabled
        if self.ENABLE_VISION_ANALYSIS and not os.getenv("OPENAI_API_KEY"):
            required_vars.append("OPENAI_API_KEY")
        
        if required_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(required_vars)}")
    
    def get_adapter_class_names(self) -> tuple[str, str]:
        """Get the class names for job source and storage adapters"""
        job_source_map = {
            "postgres": "PostgresJobSourceAdapter",
            "sqs": "SQSJobSourceAdapter", 
            "webhook": "WebhookJobSourceAdapter"
        }
        
        storage_map = {
            "postgres": "PostgresStorageAdapter",
            "s3": "S3StorageAdapter",
            "hybrid": "HybridStorageAdapter"
        }
        
        job_source_class = job_source_map.get(self.JOB_SOURCE_TYPE, "PostgresJobSourceAdapter")
        storage_class = storage_map.get(self.STORAGE_TYPE, "PostgresStorageAdapter")
        
        return job_source_class, storage_class
