"""
Adapter pattern implementations for job sources and storage backends.

This module provides abstract base classes and concrete implementations
for different job sources (Postgres, SQS, webhooks) and storage backends
(Postgres, S3, etc.).
"""

from .base import JobSourceAdapter, StorageAdapter
from .postgres_adapter import PostgresJobSourceAdapter, PostgresStorageAdapter

__all__ = [
    'JobSourceAdapter',
    'StorageAdapter', 
    'PostgresJobSourceAdapter',
    'PostgresStorageAdapter'
]
