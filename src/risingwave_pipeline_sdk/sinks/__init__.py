"""Sink implementations for various destinations."""

from .base import SinkConfig, SinkPipeline
from .s3 import S3Config, S3Sink
from .postgresql import PostgreSQLSinkConfig, PostgreSQLSink
from .iceberg import IcebergConfig, IcebergSink

__all__ = [
    "SinkConfig",
    "SinkPipeline",
    "S3Config",
    "S3Sink",
    "PostgreSQLSinkConfig",
    "PostgreSQLSink",
    "IcebergConfig",
    "IcebergSink",
]
