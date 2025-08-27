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


@dataclass
class ColumnSelection:
    """Column selection specification for table creation."""
    column_name: str
    risingwave_type: Optional[str] = None  # Override RisingWave type if needed
    is_primary_key: bool = False
    is_nullable: Optional[bool] = None  # Override nullability if needed


@dataclass
class TableColumnConfig:
    """Table configuration with column-level filtering."""
    table_info: TableInfo
    selected_columns: Optional[List[ColumnSelection]
                               ] = None  # None means all columns
    # Override table name in RisingWave
    custom_table_name: Optional[str] = None

    @property
    def risingwave_table_name(self) -> str:
        """Get the table name to use in RisingWave."""
        return self.custom_table_name or self.table_info.table_name

    def get_primary_key_columns(self) -> List[str]:
        """Get list of primary key column names."""
        if not self.selected_columns:
            return []
        return [col.column_name for col in self.selected_columns if col.is_primary_key]


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
            # Use specific table list - include even tables that don't exist yet
            selected = []
            available_table_names = {
                table.table_name for table in available_tables}
            available_qualified_names = {
                table.qualified_name for table in available_tables}

            for table_name in self.specific_tables:
                # First try to find the table in available tables
                found = False
                for table in available_tables:
                    if table.qualified_name == table_name or table.table_name == table_name:
                        selected.append(table)
                        found = True
                        break

                # If not found, create a placeholder TableInfo for tables that might not exist yet
                if not found:
                    # Assume it's in the 'public' schema if no schema specified
                    if '.' in table_name:
                        schema_name, table_name_only = table_name.split('.', 1)
                    else:
                        schema_name = 'public'
                        table_name_only = table_name

                    placeholder_table = TableInfo(
                        schema_name=schema_name,
                        table_name=table_name_only,
                        table_type='BASE TABLE',  # Assume base table
                        row_count=None,
                        size_bytes=None,
                        comment=f"Table not found in discovery - will attempt to create CDC"
                    )
                    selected.append(placeholder_table)

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


def map_postgres_type_to_risingwave(postgres_type: str) -> str:
    """Map PostgreSQL data type to RisingWave data type."""
    # Convert to lowercase for consistent mapping
    pg_type = postgres_type.lower()

    # Handle common PostgreSQL types
    type_mapping = {
        # Integer types
        'smallint': 'SMALLINT',
        'integer': 'INT',
        'int': 'INT',
        'int4': 'INT',
        'bigint': 'BIGINT',
        'int8': 'BIGINT',

        # Numeric types
        'decimal': 'DECIMAL',
        'numeric': 'DECIMAL',
        'real': 'REAL',
        'float4': 'REAL',
        'double precision': 'DOUBLE',
        'float8': 'DOUBLE',

        # Character types
        'character varying': 'VARCHAR',
        'varchar': 'VARCHAR',
        'character': 'VARCHAR',
        'char': 'VARCHAR',
        'text': 'VARCHAR',

        # Boolean
        'boolean': 'BOOLEAN',
        'bool': 'BOOLEAN',

        # Date/Time types
        'timestamp': 'TIMESTAMP',
        'timestamp without time zone': 'TIMESTAMP',
        'timestamp with time zone': 'TIMESTAMPTZ',
        'timestamptz': 'TIMESTAMPTZ',
        'date': 'DATE',
        'time': 'TIME',
        'time without time zone': 'TIME',

        # JSON types
        'json': 'JSONB',
        'jsonb': 'JSONB',

        # Array types (simplified)
        'array': 'VARCHAR',  # Arrays often need special handling

        # UUID
        'uuid': 'VARCHAR',

        # Binary types
        'bytea': 'BYTEA',
    }

    # Check for array types (ends with [])
    if pg_type.endswith('[]'):
        base_type = pg_type[:-2]
        if base_type in type_mapping:
            return f"{type_mapping[base_type]}[]"
        return 'VARCHAR[]'  # Default for unknown array types

    # Check for types with parameters (e.g., VARCHAR(255))
    if '(' in pg_type:
        base_type = pg_type.split('(')[0]
        if base_type in type_mapping:
            # For types like VARCHAR(255), keep the parameter
            if base_type in ['character varying', 'varchar', 'character', 'char']:
                return postgres_type.upper()  # Keep original formatting for VARCHAR(n)
            elif base_type in ['decimal', 'numeric']:
                return postgres_type.upper()  # Keep precision/scale for DECIMAL(p,s)
            else:
                return type_mapping[base_type]

    # Direct mapping
    if pg_type in type_mapping:
        return type_mapping[pg_type]

    # Default fallback
    return 'VARCHAR'  # Safe default for unknown types


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
    def check_specific_tables(self, table_names: List[str], schema_name: Optional[str] = None) -> List[TableInfo]:
        """Check if specific tables exist and return their info.

        This is more efficient than list_tables() when you only need to verify
        specific tables exist.

        Args:
            table_names: List of table names to check (can include schema.table format)
            schema_name: Default schema if table names don't include schema

        Returns:
            List of TableInfo for tables that exist
        """
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

    source_name: Optional[str] = None  # Will be auto-generated if not provided
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


class SourceConnection(ABC):
    """Abstract base class for source connections."""

    def __init__(self, rw_client, config: SourceConfig):
        self.rw_client = rw_client
        self.config = config

        # Auto-generate source_name if not provided
        if not self.config.source_name:
            self.config.source_name = self._generate_source_name()

    def _generate_source_name(self) -> str:
        """Generate a default source name based on the source type and database."""
        # Use database name and schema to create a practical source name
        # Avoid including hostname to prevent exposure of private information

        # Sanitize database name (replace special chars with underscores)
        clean_db = self.config.database.replace(
            '-', '_').replace('.', '_').replace(' ', '_')
        base_name = f"postgres_cdc_{clean_db}"

        # Add schema if it's not the default 'public'
        if hasattr(self.config, 'schema_name') and self.config.schema_name != 'public':
            clean_schema = self.config.schema_name.replace(
                '-', '_').replace('.', '_').replace(' ', '_')
            base_name += f"_{clean_schema}"

        return base_name

    @abstractmethod
    def create_source_sql(self) -> str:
        """Generate CREATE SOURCE SQL."""
        pass

    @abstractmethod
    def create_table_sql(self, table_info: TableInfo, **kwargs) -> str:
        """Generate CREATE TABLE SQL for a specific table."""
        pass

    def create_connection_sql(self, selected_tables: List[TableInfo], **kwargs) -> List[str]:
        """Generate complete connection SQL (source + tables)."""
        sqls = []

        # Add source creation
        sqls.append(self.create_source_sql())

        # Add table creations
        for table in selected_tables:
            sqls.append(self.create_table_sql(table, **kwargs))

        return sqls
