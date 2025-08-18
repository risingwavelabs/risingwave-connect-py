"""Base classes for database discovery and source management."""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class TableInfo:
    """Information about a discoverable table."""
    schema_name: str
    table_name: str
    table_type: str = "BASE TABLE"  # BASE TABLE, VIEW, etc.
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    comment: Optional[str] = None

    @property
    def qualified_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema_name}.{self.table_name}"


@dataclass
class ColumnInfo:
    """Information about a table column."""
    column_name: str
    data_type: str
    is_nullable: bool
    default_value: Optional[str] = None
    is_primary_key: bool = False
    ordinal_position: int = 0


class TableSelector:
    """Utility for selecting tables based on patterns."""

    def __init__(self, include_all: bool = False, include_patterns: Optional[List[str]] = None,
                 exclude_patterns: Optional[List[str]] = None, specific_tables: Optional[List[str]] = None):
        self.include_all = include_all
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []
        self.specific_tables = specific_tables or []

    def select_tables(self, available_tables: List[TableInfo]) -> List[TableInfo]:
        """Select tables based on configured patterns."""
        if self.specific_tables:
            # Use specific table list
            selected = []
            for table in available_tables:
                if table.qualified_name in self.specific_tables or table.table_name in self.specific_tables:
                    selected.append(table)
            return selected

        if self.include_all:
            selected = available_tables.copy()
        else:
            # Start with empty and add matches
            selected = []
            for table in available_tables:
                for pattern in self.include_patterns:
                    if self._matches_pattern(table.qualified_name, pattern) or self._matches_pattern(table.table_name, pattern):
                        selected.append(table)
                        break

        # Remove excluded tables
        if self.exclude_patterns:
            filtered = []
            for table in selected:
                should_exclude = False
                for pattern in self.exclude_patterns:
                    if self._matches_pattern(table.qualified_name, pattern) or self._matches_pattern(table.table_name, pattern):
                        should_exclude = True
                        break
                if not should_exclude:
                    filtered.append(table)
            selected = filtered

        return selected

    def _matches_pattern(self, name: str, pattern: str) -> bool:
        """Check if name matches pattern (supports * wildcards)."""
        import fnmatch
        return fnmatch.fnmatch(name.lower(), pattern.lower())


class DatabaseDiscovery(ABC):
    """Abstract base class for database discovery."""

    @abstractmethod
    def list_schemas(self) -> List[str]:
        """List all available schemas."""
        pass

    @abstractmethod
    def list_tables(self, schema_name: Optional[str] = None) -> List[TableInfo]:
        """List tables in a schema or all schemas."""
        pass

    @abstractmethod
    def get_table_columns(self, schema_name: str, table_name: str) -> List[ColumnInfo]:
        """Get column information for a specific table."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if connection to database is working."""
        pass


class SourceConfig(BaseModel):
    """Base configuration for all source types."""

    source_name: str
    hostname: str
    port: int
    username: str
    password: str
    database: str

    # Common source options
    auto_schema_change: bool = False

    # Backfill configuration
    backfill_num_rows_per_split: Optional[str] = None
    backfill_parallelism: Optional[str] = None
    backfill_as_even_splits: bool = True


class SourcePipeline(ABC):
    """Abstract base class for source pipelines."""

    def __init__(self, rw_client, config: SourceConfig):
        self.rw_client = rw_client
        self.config = config

    @abstractmethod
    def create_source_sql(self) -> str:
        """Generate CREATE SOURCE SQL."""
        pass

    @abstractmethod
    def create_table_sql(self, table_info: TableInfo, **kwargs) -> str:
        """Generate CREATE TABLE SQL for a specific table."""
        pass

    def create_pipeline_sql(self, selected_tables: List[TableInfo], **kwargs) -> List[str]:
        """Generate complete pipeline SQL (source + tables)."""
        sqls = []

        # Add source creation
        sqls.append(self.create_source_sql())

        # Add table creations
        for table in selected_tables:
            sqls.append(self.create_table_sql(table, **kwargs))

        return sqls
