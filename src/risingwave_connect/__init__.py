"""RisingWave Connect

A Python SDK for connecting to RisingWave with table discovery and selection.
"""

from .client import RisingWaveClient
from .models import Source, Table, Sink, MaterializedView
from .connect_builder import ConnectBuilder, create_postgresql_cdc_pipeline
from .sources.postgresql import PostgreSQLConfig, PostgreSQLDiscovery, PostgreSQLPipeline
from .discovery.base import TableSelector, TableInfo, ColumnInfo

# Sink components
from .sinks.base import SinkConfig, SinkResult
from .sinks.s3 import S3Config, S3Sink
from .sinks.postgresql import PostgreSQLSinkConfig, PostgreSQLSink
from .sinks.iceberg import IcebergConfig, IcebergSink

__all__ = [
    # Core components
    "RisingWaveClient",
    "ConnectBuilder",

    # PostgreSQL components
    "PostgreSQLConfig",
    "PostgreSQLDiscovery",
    "PostgreSQLPipeline",
    "create_postgresql_cdc_pipeline",

    # Discovery and selection
    "TableSelector",
    "TableInfo",
    "ColumnInfo",

    # Sink components
    "SinkConfig",
    "SinkResult",
    "S3Config",
    "S3Sink",
    "PostgreSQLSinkConfig",
    "PostgreSQLSink",
    "IcebergConfig",
    "IcebergSink",

    # Data models
    "Source",
    "Table",
    "Sink",
    "MaterializedView",
]

__version__ = "0.2.0"
