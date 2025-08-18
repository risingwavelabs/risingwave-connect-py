"""Base classes for sink implementations."""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class SinkConfig(BaseModel):
    """Base configuration for sinks."""

    sink_name: str = Field(..., min_length=1, description="Name of the sink")
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
