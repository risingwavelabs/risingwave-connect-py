"""
Template for adding new database source types.

This template shows how to extend the SDK to support additional database types
like MySQL, SQL Server, MongoDB, etc.
"""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

# Example: MySQL implementation template
# from mysql.connector import connect as mysql_connect
from pydantic import BaseModel, Field

from ..discovery.base import (
    DatabaseDiscovery,
    SourcePipeline,
    SourceConfig,
    TableInfo,
    ColumnInfo
)

logger = logging.getLogger(__name__)


class MySQLConfig(SourceConfig):
    """MySQL-specific configuration."""

    # MySQL specific options
    schema_name: str = "mysql"  # MySQL uses database as schema
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None

    # CDC Configuration for MySQL (using Debezium MySQL connector)
    server_id: Optional[int] = None
    binlog_filename: Optional[str] = None
    binlog_position: Optional[int] = None

    # Advanced MySQL properties
    mysql_properties: Dict[str, str] = Field(default_factory=dict)


class MySQLDiscovery(DatabaseDiscovery):
    """MySQL database discovery implementation."""

    def __init__(self, config: MySQLConfig):
        self.config = config

    def _connection(self):
        """Get MySQL connection - implement based on your MySQL driver."""
        # Example using mysql-connector-python:
        # return mysql_connect(
        #     host=self.config.hostname,
        #     port=self.config.port,
        #     user=self.config.username,
        #     password=self.config.password,
        #     database=self.config.database
        # )
        raise NotImplementedError("Implement MySQL connection")

    def test_connection(self) -> bool:
        """Test MySQL connection."""
        try:
            with self._connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"MySQL connection test failed: {e}")
            return False

    def list_schemas(self) -> List[str]:
        """List MySQL databases (schemas)."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            return [row[0] for row in cursor.fetchall()
                    if row[0] not in ('information_schema', 'performance_schema', 'mysql', 'sys')]

    def list_tables(self, schema_name: Optional[str] = None) -> List[TableInfo]:
        """List tables in MySQL database."""
        schema = schema_name or self.config.database

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE,
                    TABLE_ROWS,
                    DATA_LENGTH,
                    TABLE_COMMENT
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                ORDER BY TABLE_NAME
            """, (schema,))

            tables = []
            for row in cursor.fetchall():
                tables.append(TableInfo(
                    schema_name=row[0],
                    table_name=row[1],
                    table_type=row[2],
                    row_count=row[3] or 0,
                    size_bytes=row[4] or 0,
                    comment=row[5]
                ))
            return tables

    def get_table_columns(self, schema_name: str, table_name: str) -> List[ColumnInfo]:
        """Get MySQL table column information."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE = 'YES',
                    COLUMN_DEFAULT,
                    ORDINAL_POSITION,
                    COLUMN_KEY = 'PRI'
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (schema_name, table_name))

            columns = []
            for row in cursor.fetchall():
                columns.append(ColumnInfo(
                    column_name=row[0],
                    data_type=row[1],
                    is_nullable=row[2],
                    default_value=row[3],
                    ordinal_position=row[4],
                    is_primary_key=row[5]
                ))
            return columns


class MySQLPipeline(SourcePipeline):
    """MySQL CDC pipeline implementation."""

    def __init__(self, rw_client, config: MySQLConfig):
        super().__init__(rw_client, config)
        self.config: MySQLConfig = config

    def create_source_sql(self) -> str:
        """Generate CREATE SOURCE SQL for MySQL CDC."""
        # Note: RisingWave MySQL CDC connector syntax would go here
        # This is a template - check RisingWave docs for actual MySQL connector syntax

        with_items = [
            "connector='mysql-cdc'",  # Hypothetical MySQL connector
            f"hostname='{self._escape_sql_string(self.config.hostname)}'",
            f"port='{self.config.port}'",
            f"username='{self._escape_sql_string(self.config.username)}'",
            f"password='{self._escape_sql_string(self.config.password)}'",
            f"database.name='{self._escape_sql_string(self.config.database)}'",
        ]

        # Add MySQL-specific options
        if self.config.server_id:
            with_items.append(f"database.server.id='{self.config.server_id}'")

        # Add SSL options
        if self.config.ssl_ca:
            with_items.append(
                f"ssl.ca='{self._escape_sql_string(self.config.ssl_ca)}'")

        # Add auto schema change
        with_items.append(
            f"auto.schema.change='{str(self.config.auto_schema_change).lower()}'")

        # Add MySQL-specific properties
        for key, value in self.config.mysql_properties.items():
            with_items.append(
                f"mysql.{key}='{self._escape_sql_string(str(value))}'")

        with_clause = ",\\n    ".join(with_items)

        return f"""-- Create MySQL CDC source {self.config.source_name}
CREATE SOURCE IF NOT EXISTS {self.config.source_name} WITH (
    {with_clause}
);"""

    def create_table_sql(self, table_info: TableInfo, **kwargs) -> str:
        """Generate CREATE TABLE SQL for MySQL table."""
        table_name = kwargs.get('table_name', table_info.table_name)
        rw_schema = kwargs.get('rw_schema', 'public')

        # Build WITH clause for backfill configuration
        with_items = []
        if self.config.backfill_num_rows_per_split:
            with_items.append(
                f"backfill.num_rows_per_split='{self.config.backfill_num_rows_per_split}'")
        if self.config.backfill_parallelism:
            with_items.append(
                f"backfill.parallelism='{self.config.backfill_parallelism}'")
        if self.config.backfill_as_even_splits:
            with_items.append(
                f"backfill.as_even_splits='{str(self.config.backfill_as_even_splits).lower()}'")

        with_clause = ""
        if with_items:
            with_clause = f"\\nWITH (\\n    {',\\n    '.join(with_items)}\\n)"

        qualified_table_name = f"{rw_schema}.{table_name}" if rw_schema != "public" else table_name

        return f"""-- Create table from MySQL source
CREATE TABLE IF NOT EXISTS {qualified_table_name} (*) {with_clause}
FROM {self.config.source_name}
TABLE '{table_info.qualified_name}';"""

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes in SQL strings."""
        return value.replace("'", "''") if value else ""


# Similar templates can be created for:

class SQLServerConfig(SourceConfig):
    """SQL Server-specific configuration."""
    # Add SQL Server specific options
    instance_name: Optional[str] = None
    encrypt: bool = True
    trust_server_certificate: bool = False

    # CDC options for SQL Server
    database_history_kafka_topic: Optional[str] = None


class MongoDBConfig(SourceConfig):
    """MongoDB-specific configuration."""
    # MongoDB doesn't use traditional schema/table concepts
    replica_set: Optional[str] = None
    auth_source: str = "admin"

    # Collection selection
    collection_include_list: Optional[str] = None
    collection_exclude_list: Optional[str] = None


class KafkaConfig(SourceConfig):
    """Kafka-specific configuration."""
    # Kafka is different - it's a streaming source, not a database
    bootstrap_servers: str
    topic: str
    consumer_group: Optional[str] = None

    # Format and encoding
    message_format: str = "JSON"  # JSON, AVRO, etc.
    schema_registry_url: Optional[str] = None


# Usage example for extending the pipeline builder:
"""
def create_mysql_pipeline(
    rw_client: RisingWaveClient,
    mysql_config: MySQLConfig,
    table_selector: Optional[TableSelector] = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    discovery = MySQLDiscovery(mysql_config)
    pipeline = MySQLPipeline(rw_client, mysql_config)
    
    # Same logic as PostgreSQL implementation
    available_tables = discovery.list_tables()
    selected_tables = table_selector.select_tables(available_tables)
    sql_statements = pipeline.create_pipeline_sql(selected_tables)
    
    # Execute or return SQL
    ...
"""
