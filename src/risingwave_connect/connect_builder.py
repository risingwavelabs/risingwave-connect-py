"""Streamlined connection builder using modular builders."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union

from .client import RisingWaveClient
from .discovery.base import TableSelector, TableInfo, TableColumnConfig
from .sources.postgresql import PostgreSQLConfig
from .sources.mongodb import MongoDBConfig
from .sources.sqlserver import SQLServerConfig
from .sources.kafka import KafkaConfig
from .sources.mysql import MySQLConfig
from .sinks.base import SinkConfig
from .sinks.s3 import S3Config
from .sinks.postgresql import PostgreSQLSinkConfig
from .sinks.iceberg import IcebergConfig

# Import modular builders
from .builders.postgresql import PostgreSQLBuilder
from .builders.mongodb import MongoDBBuilder
from .builders.sqlserver import SQLServerBuilder
from .builders.kafka import KafkaBuilder
from .builders.mysql import MySQLBuilder
from .builders.sinks import SinkBuilder

logger = logging.getLogger(__name__)


class ConnectBuilder:
    """High-level connection builder using modular components."""

    def __init__(self, rw_client: RisingWaveClient):
        self.rw_client = rw_client

        # Initialize specialized builders
        self._postgresql_builder = PostgreSQLBuilder(rw_client)
        self._mongodb_builder = MongoDBBuilder(rw_client)
        self._sqlserver_builder = SQLServerBuilder(rw_client)
        self._kafka_builder = KafkaBuilder(rw_client)
        self._mysql_builder = MySQLBuilder(rw_client)
        self._sink_builder = SinkBuilder(rw_client)

    # PostgreSQL Methods
    def create_postgresql_connection(
        self,
        config: PostgreSQLConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        column_configs: Optional[Dict[str, TableColumnConfig]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create a complete PostgreSQL CDC connection with table discovery."""
        return self._postgresql_builder.create_connection(
            config, table_selector, column_configs, dry_run)

    def discover_postgresql_tables(
        self,
        config: PostgreSQLConfig,
        schema_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available tables in PostgreSQL database."""
        return self._postgresql_builder.discover_tables(config, schema_name)

    def get_postgresql_schemas(self, config: PostgreSQLConfig) -> List[str]:
        """Get list of available schemas in PostgreSQL database."""
        return self._postgresql_builder.get_schemas(config)

    # MongoDB Methods
    def create_mongodb_connection(
        self,
        config: MongoDBConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        dry_run: bool = False,
        include_commit_timestamp: bool = False,
        include_database_name: bool = False,
        include_collection_name: bool = False
    ) -> Dict[str, Any]:
        """Create a complete MongoDB CDC connection with collection discovery."""
        return self._mongodb_builder.create_connection(
            config, table_selector, dry_run, include_commit_timestamp,
            include_database_name, include_collection_name)

    def discover_mongodb_collections(
        self,
        config: MongoDBConfig,
        database_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available collections in MongoDB database."""
        return self._mongodb_builder.discover_tables(config, database_name)

    def get_mongodb_databases(self, config: MongoDBConfig) -> List[str]:
        """Get list of available databases in MongoDB."""
        return self._mongodb_builder.get_schemas(config)

    # SQL Server Methods
    def create_sqlserver_connection(
        self,
        config: SQLServerConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        column_configs: Optional[Dict[str, TableColumnConfig]] = None,
        dry_run: bool = False,
        include_timestamp: bool = False,
        include_database_name: bool = False,
        include_schema_name: bool = False,
        include_table_name: bool = False
    ) -> Dict[str, Any]:
        """Create a complete SQL Server CDC connection with table discovery."""
        return self._sqlserver_builder.create_connection(
            config, table_selector, column_configs, dry_run,
            include_timestamp, include_database_name,
            include_schema_name, include_table_name)

    def discover_sqlserver_tables(
        self,
        config: SQLServerConfig,
        schema_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available tables in SQL Server database."""
        return self._sqlserver_builder.discover_tables(config, schema_name)

    def get_sqlserver_schemas(self, config: SQLServerConfig) -> List[str]:
        """Get list of available schemas in SQL Server database."""
        return self._sqlserver_builder.get_schemas(config)

    # Kafka Methods
    def create_kafka_connection(
        self,
        config: KafkaConfig,
        table_name: Optional[str] = None,
        table_schema: Optional[Dict[str, str]] = None,
        dry_run: bool = False,
        include_offset: bool = False,
        include_partition: bool = False,
        include_timestamp: bool = False,
        include_headers: bool = False,
        include_key: bool = False,
        include_payload: bool = False,
        connection_type: str = "table"
    ) -> Dict[str, Any]:
        """Create a complete Kafka source connection."""
        return self._kafka_builder.create_connection(
            config, table_name, table_schema, dry_run,
            include_offset, include_partition, include_timestamp, include_headers,
            include_key, include_payload, connection_type)

    # MySQL Methods
    def create_mysql_connection(
        self,
        config: MySQLConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        column_configs: Optional[Dict[str, TableColumnConfig]] = None,
        dry_run: bool = False,
        include_timestamp: bool = False,
        include_database_name: bool = False,
        include_table_name: bool = False,
        snapshot: bool = True,
        snapshot_interval: int = 1,
        snapshot_batch_size: int = 1000
    ) -> Dict[str, Any]:
        """Create a complete MySQL CDC connection with table discovery."""
        return self._mysql_builder.create_connection(
            config, table_selector, column_configs, dry_run,
            include_timestamp, include_database_name, include_table_name,
            snapshot, snapshot_interval, snapshot_batch_size)

    def discover_mysql_tables(
        self,
        config: MySQLConfig,
        schema_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available tables in MySQL database."""
        return self._mysql_builder.discover_tables(config, schema_name)

    def get_mysql_schemas(self, config: MySQLConfig) -> List[str]:
        """Get list of available schemas in MySQL database."""
        return self._mysql_builder.get_schemas(config)

    # Sink Methods
    def create_sink(
        self,
        sink_config: Union[S3Config, PostgreSQLSinkConfig, IcebergConfig],
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create a sink based on the config type."""
        return self._sink_builder.create_sink(sink_config, source_tables, select_queries, dry_run)

    def create_s3_sink(
        self,
        s3_config: S3Config,
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create S3 sink."""
        return self._sink_builder._create_s3_sink(s3_config, source_tables, select_queries, dry_run)

    def create_postgresql_sink(
        self,
        pg_sink_config: PostgreSQLSinkConfig,
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create PostgreSQL sink."""
        return self._sink_builder._create_postgresql_sink(pg_sink_config, source_tables, select_queries, dry_run)


# Convenience functions for backward compatibility
def create_postgresql_cdc_source_connection(
    rw_client: RisingWaveClient,
    pg_config: PostgreSQLConfig,
    include_all_tables: bool = False,
    include_tables: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Convenience function to create PostgreSQL CDC source connection."""
    builder = ConnectBuilder(rw_client)

    # Create table selector
    if include_tables:
        selector = TableSelector(specific_tables=include_tables)
    elif include_all_tables:
        selector = TableSelector(
            include_all=True, exclude_patterns=exclude_tables or [])
    else:
        selector = TableSelector(
            include_patterns=["*"], exclude_patterns=exclude_tables or [])

    return builder.create_postgresql_connection(pg_config, selector, dry_run)


def create_mongodb_cdc_source_connection(
    rw_client: RisingWaveClient,
    mongodb_config: MongoDBConfig,
    include_all_collections: bool = False,
    include_collections: Optional[List[str]] = None,
    exclude_collections: Optional[List[str]] = None,
    include_commit_timestamp: bool = False,
    include_database_name: bool = False,
    include_collection_name: bool = False,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Convenience function to create MongoDB CDC source connection."""
    builder = ConnectBuilder(rw_client)

    # Create table selector
    if include_collections:
        selector = TableSelector(specific_tables=include_collections)
    elif include_all_collections:
        selector = TableSelector(
            include_all=True, exclude_patterns=exclude_collections or [])
    else:
        selector = TableSelector(
            include_patterns=["*"], exclude_patterns=exclude_collections or [])

    return builder.create_mongodb_connection(
        mongodb_config, selector, dry_run,
        include_commit_timestamp=include_commit_timestamp,
        include_database_name=include_database_name,
        include_collection_name=include_collection_name
    )


def create_sqlserver_cdc_source_connection(
    rw_client: RisingWaveClient,
    sqlserver_config: SQLServerConfig,
    include_all_tables: bool = False,
    include_tables: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    column_configs: Optional[Dict[str, TableColumnConfig]] = None,
    include_timestamp: bool = False,
    include_database_name: bool = False,
    include_schema_name: bool = False,
    include_table_name: bool = False,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Convenience function to create SQL Server CDC source connection."""
    builder = ConnectBuilder(rw_client)

    # Create table selector
    if include_tables:
        selector = TableSelector(specific_tables=include_tables)
    elif include_all_tables:
        selector = TableSelector(
            include_all=True, exclude_patterns=exclude_tables or [])
    else:
        selector = TableSelector(
            include_patterns=["*"], exclude_patterns=exclude_tables or [])

    return builder.create_sqlserver_connection(
        sqlserver_config, selector, column_configs, dry_run,
        include_timestamp=include_timestamp,
        include_database_name=include_database_name,
        include_schema_name=include_schema_name,
        include_table_name=include_table_name
    )


def create_kafka_source_connection(
    rw_client: RisingWaveClient,
    kafka_config: KafkaConfig,
    table_name: Optional[str] = None,
    table_schema: Optional[Dict[str, str]] = None,
    include_offset: bool = False,
    include_partition: bool = False,
    include_timestamp: bool = False,
    include_headers: bool = False,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Convenience function to create Kafka source connection."""
    builder = ConnectBuilder(rw_client)

    return builder.create_kafka_connection(
        kafka_config, table_name, table_schema, dry_run,
        include_offset=include_offset,
        include_partition=include_partition,
        include_timestamp=include_timestamp,
        include_headers=include_headers
    )


def create_mysql_cdc_source_connection(
    rw_client: RisingWaveClient,
    mysql_config: MySQLConfig,
    include_all_tables: bool = False,
    include_tables: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    column_configs: Optional[Dict[str, TableColumnConfig]] = None,
    include_timestamp: bool = False,
    include_database_name: bool = False,
    include_table_name: bool = False,
    snapshot: bool = True,
    snapshot_interval: int = 1,
    snapshot_batch_size: int = 1000,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Convenience function to create MySQL CDC source connection."""
    builder = ConnectBuilder(rw_client)

    # Create table selector
    if include_tables:
        selector = TableSelector(specific_tables=include_tables)
    elif include_all_tables:
        selector = TableSelector(
            include_all=True, exclude_patterns=exclude_tables or [])
    else:
        selector = TableSelector(
            include_patterns=["*"], exclude_patterns=exclude_tables or [])

    return builder.create_mysql_connection(
        mysql_config, selector, column_configs, dry_run,
        include_timestamp=include_timestamp,
        include_database_name=include_database_name,
        include_table_name=include_table_name,
        snapshot=snapshot,
        snapshot_interval=snapshot_interval,
        snapshot_batch_size=snapshot_batch_size
    )
