"""Data models for RisingWave pipeline components."""

from __future__ import annotations
from typing import Optional

from pydantic import BaseModel


class Source(BaseModel):
    """Represents a RisingWave source."""
    
    name: str
    schema: str = "public"
    source_type: str  # e.g., "postgres-cdc", "kafka"
    
    @property
    def qualified_name(self) -> str:
        """Get fully qualified source name."""
        return f"{self.schema}.{self.name}"


class Table(BaseModel):
    """Represents a RisingWave table."""
    
    name: str
    schema: str = "public"
    source: Optional[Source] = None
    
    @property
    def qualified_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{self.name}"


class MaterializedView(BaseModel):
    """Represents a RisingWave materialized view."""
    
    name: str
    schema: str = "public"
    definition: str  # SQL definition
    
    @property
    def qualified_name(self) -> str:
        """Get fully qualified materialized view name."""
        return f"{self.schema}.{self.name}"
