"""PostgreSQL sink implementation for RisingWave."""

from __future__ import annotations
import logging
from typing import Optional, Dict, Any
from pydantic import Field, validator

from .base import SinkConfig, SinkPipeline, SinkResult

logger = logging.getLogger(__name__)


class PostgreSQLSinkConfig(SinkConfig):
    """Configuration for PostgreSQL sink."""

    sink_type: str = Field(default="postgres", description="Sink type")

    # Required PostgreSQL parameters
    hostname: str = Field(..., min_length=1, description="PostgreSQL hostname")
    port: int = Field(default=5432, description="PostgreSQL port")
    username: str = Field(..., min_length=1, description="PostgreSQL username")
    password: str = Field(..., description="PostgreSQL password")
    database: str = Field(..., min_length=1,
                          description="PostgreSQL database name")

    # Optional PostgreSQL parameters
    postgres_schema: str = Field(
        default="public", description="Target PostgreSQL schema")
    table_name: Optional[str] = Field(
        None, description="Target table name (defaults to sink name)")
    ssl_mode: Optional[str] = Field(None, description="SSL mode")

    # Sink configuration
    data_type: str = Field(default="append-only", description="Type of sink")

    # Extra properties
    extra_properties: Dict[str, Any] = Field(
        default_factory=dict, description="Additional WITH properties")

    @validator('data_type')
    def validate_data_type(cls, v):
        """Validate data type."""
        allowed = ['append-only', 'upsert']
        if v not in allowed:
            raise ValueError(f"data_type must be one of {allowed}, got {v}")
        return v

    @validator('port')
    def validate_port(cls, v):
        """Validate port number."""
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535")
        return v


class PostgreSQLSink(SinkPipeline):
    """PostgreSQL sink implementation."""

    def __init__(self, config: PostgreSQLSinkConfig):
        super().__init__(config)
        self.config: PostgreSQLSinkConfig = config

    def validate_config(self) -> bool:
        """Validate PostgreSQL configuration."""
        if not self.config.hostname:
            raise ValueError("hostname is required for PostgreSQL sink")
        if not self.config.username:
            raise ValueError("username is required for PostgreSQL sink")
        if not self.config.database:
            raise ValueError("database is required for PostgreSQL sink")
        return True

    def create_sink_sql(self, source_table: str, select_query: Optional[str] = None) -> str:
        """Generate CREATE SINK SQL for PostgreSQL.

        Args:
            source_table: Name of source table to sink from
            select_query: Optional custom SELECT query

        Returns:
            SQL CREATE SINK statement
        """
        self.validate_config()

        # Build the FROM clause or AS clause
        if select_query:
            source_clause = f"AS {select_query}"
        else:
            source_clause = f"FROM {source_table}"

        # Determine target table name
        target_table = self.config.table_name or self.config.sink_name

        # Build WITH properties
        with_props = [
            "connector='postgres'",
            f"postgres.host='{self._quote(self.config.hostname)}'",
            f"postgres.port='{self.config.port}'",
            f"postgres.user='{self._quote(self.config.username)}'",
            f"postgres.password='{self._quote(self.config.password)}'",
            f"postgres.database='{self._quote(self.config.database)}'",
            f"postgres.table='{self._quote(self.config.postgres_schema)}.{self._quote(target_table)}'",
            f"type='{self.config.data_type}'"
        ]

        # Add optional SSL mode
        if self.config.ssl_mode:
            with_props.append(
                f"postgres.ssl.mode='{self._quote(self.config.ssl_mode)}'")

        # Add extra properties
        for key, value in self.config.extra_properties.items():
            with_props.append(f"{key}='{self._quote(str(value))}'")

        with_clause = ",\n    ".join(with_props)

        # Generate full SQL
        qualified_sink_name = f"{self.config.schema_name}.{self.config.sink_name}" if self.config.schema_name != "public" else self.config.sink_name

        sql = f"""CREATE SINK IF NOT EXISTS {qualified_sink_name}
{source_clause}
WITH (
    {with_clause}
);"""

        return sql

    def _quote(self, value: str) -> str:
        """Quote SQL string values."""
        return value.replace("'", "''")

    def create_sink(self, source_table: str, select_query: Optional[str] = None) -> SinkResult:
        """Create PostgreSQL sink and return result.

        Args:
            source_table: Name of source table
            select_query: Optional custom SELECT query

        Returns:
            SinkResult with creation details
        """
        try:
            sql = self.create_sink_sql(source_table, select_query)
            return SinkResult(
                sink_name=self.config.sink_name,
                sink_type=self.config.sink_type,
                sql_statement=sql,
                source_table=source_table,
                success=True
            )
        except Exception as e:
            return SinkResult(
                sink_name=self.config.sink_name,
                sink_type=self.config.sink_type,
                sql_statement="",
                source_table=source_table,
                success=False,
                error_message=str(e)
            )
