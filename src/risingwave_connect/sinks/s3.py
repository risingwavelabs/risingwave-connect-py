"""S3 sink implementation for RisingWave."""

from __future__ import annotations
import logging
from typing import Optional, Dict, Any
from pydantic import Field, validator

from .base import SinkConfig, SinkPipeline, SinkResult

logger = logging.getLogger(__name__)


class S3Config(SinkConfig):
    """Configuration for S3 sink."""

    sink_type: str = Field(default="s3", description="Sink type")

    # Required S3 parameters
    region_name: str = Field(..., description="AWS region name")
    bucket_name: str = Field(..., min_length=1, description="S3 bucket name")
    path: str = Field(..., min_length=1, description="S3 path/directory")

    # Optional S3 parameters
    access_key_id: Optional[str] = Field(None, description="AWS access key ID")
    secret_access_key: Optional[str] = Field(
        None, description="AWS secret access key")
    endpoint_url: Optional[str] = Field(
        None, description="Custom S3 endpoint URL")
    assume_role: Optional[str] = Field(
        None, description="IAM role ARN to assume")

    # Sink configuration
    data_type: str = Field(default="append-only", description="Type of sink")
    format_type: str = Field(default="PLAIN", description="Data format")
    encode_type: str = Field(default="PARQUET", description="Data encoding")
    force_append_only: bool = Field(
        default=True, description="Force append-only mode")

    # Extra properties
    extra_properties: Dict[str, Any] = Field(
        default_factory=dict, description="Additional WITH properties")

    @validator('data_type')
    def validate_data_type(cls, v):
        """Validate data type."""
        allowed = ['append-only']
        if v not in allowed:
            raise ValueError(f"data_type must be one of {allowed}, got {v}")
        return v

    @validator('format_type')
    def validate_format_type(cls, v):
        """Validate format type."""
        allowed = ['PLAIN', 'UPSERT', 'DEBEZIUM']
        if v not in allowed:
            raise ValueError(f"format_type must be one of {allowed}, got {v}")
        return v

    @validator('encode_type')
    def validate_encode_type(cls, v):
        """Validate encode type."""
        allowed = ['PARQUET', 'JSON', 'CSV']
        if v not in allowed:
            raise ValueError(f"encode_type must be one of {allowed}, got {v}")
        return v


class S3Sink(SinkPipeline):
    """S3 sink implementation."""

    def __init__(self, config: S3Config):
        super().__init__(config)
        self.config: S3Config = config

    def validate_config(self) -> bool:
        """Validate S3 configuration."""
        if not self.config.region_name:
            raise ValueError("region_name is required for S3 sink")
        if not self.config.bucket_name:
            raise ValueError("bucket_name is required for S3 sink")
        if not self.config.path:
            raise ValueError("path is required for S3 sink")
        return True

    def create_sink_sql(self, source_table: str, select_query: Optional[str] = None) -> str:
        """Generate CREATE SINK SQL for S3.

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

        # Build WITH properties
        with_props = [
            "connector='s3'",
            f"s3.region_name='{self._quote(self.config.region_name)}'",
            f"s3.bucket_name='{self._quote(self.config.bucket_name)}'",
            f"s3.path='{self._quote(self.config.path)}'",
            f"type='{self.config.data_type}'"
        ]

        # Add optional credentials
        if self.config.access_key_id:
            with_props.append(
                f"s3.credentials.access='{self._quote(self.config.access_key_id)}'")
        if self.config.secret_access_key:
            with_props.append(
                f"s3.credentials.secret='{self._quote(self.config.secret_access_key)}'")
        if self.config.endpoint_url:
            with_props.append(
                f"s3.endpoint_url='{self._quote(self.config.endpoint_url)}'")
        if self.config.assume_role:
            with_props.append(
                f"s3.assume_role='{self._quote(self.config.assume_role)}'")

        # Add extra properties
        for key, value in self.config.extra_properties.items():
            with_props.append(f"{key}='{self._quote(str(value))}'")

        with_clause = ",\n    ".join(with_props)

        # Build encode clause
        encode_props = []
        if self.config.encode_type == "PARQUET" and self.config.force_append_only:
            encode_props.append("force_append_only=true")

        encode_clause = ""
        if encode_props:
            encode_params = ", ".join(encode_props)
            encode_clause = f"({encode_params})"

        # Generate full SQL
        qualified_sink_name = f"{self.config.schema_name}.{self.config.sink_name}" if self.config.schema_name != "public" else self.config.sink_name

        sql = f"""CREATE SINK IF NOT EXISTS {qualified_sink_name}
{source_clause}
WITH (
    {with_clause}
)
FORMAT {self.config.format_type} ENCODE {self.config.encode_type}{encode_clause};"""

        return sql

    def _quote(self, value: str) -> str:
        """Quote SQL string values."""
        return value.replace("'", "''")

    def create_sink(self, source_table: str, select_query: Optional[str] = None) -> SinkResult:
        """Create S3 sink and return result.

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
