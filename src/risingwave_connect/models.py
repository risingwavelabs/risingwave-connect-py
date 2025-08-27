"""Data models for RisingWave connection components."""

from __future__ import annotations
from typing import Optional

from pydantic import BaseModel


class Source(BaseModel):
    """Represents a RisingWave source."""

    name: str
    schema_name: str = "public"
    source_type: str  # e.g., "postgres-cdc", "kafka"

    @property
    def qualified_name(self) -> str:
        """Get fully qualified source name."""
        return f"{self.schema_name}.{self.name}"


class Table(BaseModel):
    """Represents a RisingWave table."""

    name: str
    schema_name: str = "public"
    source: Optional[Source] = None

    @property
    def qualified_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema_name}.{self.name}"


class Sink(BaseModel):
    """Represents a RisingWave sink."""

    name: str
    schema_name: str = "public"
    sink_type: str  # e.g., "s3", "postgres", "kafka"
    target_table: Optional[str] = None  # Source table this sink reads from

    @property
    def qualified_name(self) -> str:
        """Get fully qualified sink name."""
        return f"{self.schema_name}.{self.name}"


class MaterializedView(BaseModel):
    """Represents a RisingWave materialized view."""

    name: str
    schema_name: str = "public"
    definition: str  # SQL definition

    @property
    def qualified_name(self) -> str:
        """Get fully qualified materialized view name."""
        return f"{self.schema_name}.{self.name}"
