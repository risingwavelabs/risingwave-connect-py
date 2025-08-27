"""Base classes for sink implementations."""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class SinkConfig(BaseModel):
    """Base configuration for sinks."""

    sink_name: Optional[str] = Field(
        None, description="Name of the sink (auto-generated if not provided)")
    sink_type: str = Field(...,
                           description="Type of sink (s3, postgres, etc.)")
    schema_name: str = Field(
        default="public", description="Schema to create sink in")

    class Config:
        extra = "forbid"


class SinkPipeline(ABC):
    """Abstract base class for sink pipeline implementations."""

    def __init__(self, config: SinkConfig):
        self.config = config

        # Auto-generate sink_name if not provided
        if not self.config.sink_name:
            self.config.sink_name = self._generate_sink_name()

    def _generate_sink_name(self) -> str:
        """Generate a default sink name based on sink type and configuration."""
        # Use sink type and target info to create a practical sink name
        base_name = f"{self.config.sink_type}_sink"

        # Add additional context based on sink type if available
        if hasattr(self.config, 'database_name') and self.config.database_name:
            # For Iceberg and similar sinks with database_name
            db_clean = self.config.database_name.replace(
                '-', '_').replace('.', '_').replace(' ', '_')
            base_name = f"{self.config.sink_type}_{db_clean}_sink"
        elif hasattr(self.config, 'database') and self.config.database:
            # For PostgreSQL sinks with database field
            db_clean = self.config.database.replace(
                '-', '_').replace('.', '_').replace(' ', '_')
            base_name = f"{self.config.sink_type}_{db_clean}_sink"
        elif hasattr(self.config, 'bucket_name') and self.config.bucket_name:
            # For S3 sinks with bucket name
            bucket_clean = self.config.bucket_name.replace(
                '-', '_').replace('.', '_')
            base_name = f"{self.config.sink_type}_{bucket_clean}_sink"

        return base_name

    @abstractmethod
    def create_sink_sql(self, source_table: str, select_query: Optional[str] = None) -> str:
        """Generate CREATE SINK SQL statement.

        Args:
            source_table: Name of source table to sink from
            select_query: Optional custom SELECT query instead of using FROM table

        Returns:
            SQL CREATE SINK statement
        """
        pass

    @abstractmethod
    def validate_config(self) -> bool:
        """Validate sink configuration.

        Returns:
            True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        pass

    def get_sink_info(self) -> Dict[str, Any]:
        """Get information about this sink.

        Returns:
            Dictionary with sink metadata
        """
        return {
            "name": self.config.sink_name,
            "type": self.config.sink_type,
            "schema": self.config.schema_name,
        }


class SinkResult(BaseModel):
    """Result of sink creation."""

    sink_name: str
    sink_type: str
    sql_statement: str
    source_table: str
    success: bool = True
    error_message: Optional[str] = None
