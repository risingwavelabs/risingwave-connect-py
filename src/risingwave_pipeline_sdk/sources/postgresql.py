"""PostgreSQL-specific discovery and pipeline implementation."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

import psycopg
from pydantic import BaseModel, Field, validator

from ..discovery.base import (
    DatabaseDiscovery,
    SourcePipeline,
    SourceConfig,
    TableInfo,
    ColumnInfo
)

logger = logging.getLogger(__name__)


class PostgreSQLConfig(SourceConfig):
    """PostgreSQL-specific configuration.

    Args:
        ssl_mode: Required SSL/TLS encryption mode. 
                 Accepted values: disabled, preferred, required, verify-ca, verify-full
        ssl_root_cert: Optional path to CA certificate for SSL verification
        schema_name: PostgreSQL schema name (default: public)
        slot_name: Optional replication slot name
        publication_name: PostgreSQL publication name (default: rw_publication) 
        publication_create_enable: Auto-create publication if missing (default: True)
    """

    # PostgreSQL specific
    schema_name: str = "public"
    ssl_mode: str  # Required: SSL/TLS encryption mode
    ssl_root_cert: Optional[str] = None

    @validator('ssl_mode')
    def validate_ssl_mode(cls, v):
        """Validate SSL mode values."""
        valid_modes = ['disabled', 'preferred',
                       'required', 'verify-ca', 'verify-full']
        if v not in valid_modes:
            raise ValueError(
                f"ssl_mode must be one of: {', '.join(valid_modes)}")
        return v

    # CDC Configuration
    slot_name: Optional[str] = None
    # Optional: defaults to 'rw_publication' if not specified
    publication_name: Optional[str] = None
    # Optional: defaults to True if not specified
    publication_create_enable: Optional[bool] = None

    # Parallel backfill configuration (optional)
    backfill_num_rows_per_split: Optional[str] = None  # e.g., '100000'
    backfill_parallelism: Optional[str] = None         # e.g., '8'
    backfill_as_even_splits: Optional[bool] = None     # e.g., True

    # Advanced Debezium properties
    debezium_properties: Dict[str, str] = Field(default_factory=dict)
    extra_properties: Dict[str, str] = Field(default_factory=dict)


class PostgreSQLDiscovery(DatabaseDiscovery):
    """PostgreSQL database discovery implementation."""

    def __init__(self, config: PostgreSQLConfig):
        self.config = config
        self._dsn = self._build_dsn()

    def _build_dsn(self) -> str:
        """Build PostgreSQL connection string."""
        return f"postgresql://{self.config.username}:{self.config.password}@{self.config.hostname}:{self.config.port}/{self.config.database}"

    @contextmanager
    def _connection(self):
        """Get database connection."""
        with psycopg.connect(self._dsn) as conn:
            yield conn

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self._connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def list_schemas(self) -> List[str]:
        """List all schemas in the database."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY schema_name
                """)
                return [row[0] for row in cur.fetchall()]

    def list_tables(self, schema_name: Optional[str] = None) -> List[TableInfo]:
        """List tables in specified schema or all schemas."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                if schema_name:
                    cur.execute("""
                        SELECT 
                            t.table_schema,
                            t.table_name,
                            t.table_type,
                            pg_stat.n_tup_ins + pg_stat.n_tup_upd + pg_stat.n_tup_del as row_count,
                            pg_total_relation_size(pg_class.oid) as size_bytes,
                            obj_description(pg_class.oid) as comment
                        FROM information_schema.tables t
                        LEFT JOIN pg_stat_user_tables pg_stat ON t.table_name = pg_stat.relname AND t.table_schema = pg_stat.schemaname
                        LEFT JOIN pg_class ON t.table_name = pg_class.relname
                        LEFT JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid AND pg_namespace.nspname = t.table_schema
                        WHERE t.table_schema = %s 
                        AND t.table_type IN ('BASE TABLE', 'VIEW')
                        ORDER BY t.table_schema, t.table_name
                    """, (schema_name,))
                else:
                    cur.execute("""
                        SELECT 
                            t.table_schema,
                            t.table_name,
                            t.table_type,
                            pg_stat.n_tup_ins + pg_stat.n_tup_upd + pg_stat.n_tup_del as row_count,
                            pg_total_relation_size(pg_class.oid) as size_bytes,
                            obj_description(pg_class.oid) as comment
                        FROM information_schema.tables t
                        LEFT JOIN pg_stat_user_tables pg_stat ON t.table_name = pg_stat.relname AND t.table_schema = pg_stat.schemaname
                        LEFT JOIN pg_class ON t.table_name = pg_class.relname
                        LEFT JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid AND pg_namespace.nspname = t.table_schema
                        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                        AND t.table_type IN ('BASE TABLE', 'VIEW')
                        ORDER BY t.table_schema, t.table_name
                    """)

                tables = []
                for row in cur.fetchall():
                    tables.append(TableInfo(
                        schema_name=row[0],
                        table_name=row[1],
                        table_type=row[2],
                        row_count=row[3] if row[3] is not None else 0,
                        size_bytes=row[4] if row[4] is not None else 0,
                        comment=row[5]
                    ))
                return tables

    def get_table_columns(self, schema_name: str, table_name: str) -> List[ColumnInfo]:
        """Get column information for a table."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                # Get column information
                cur.execute("""
                    SELECT 
                        c.column_name,
                        c.data_type,
                        c.is_nullable = 'YES' as is_nullable,
                        c.column_default,
                        c.ordinal_position,
                        CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT ku.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
                        WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                    ) pk ON c.column_name = pk.column_name
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position
                """, (schema_name, table_name, schema_name, table_name))

                columns = []
                for row in cur.fetchall():
                    columns.append(ColumnInfo(
                        column_name=row[0],
                        data_type=row[1],
                        is_nullable=row[2],
                        default_value=row[3],
                        ordinal_position=row[4],
                        is_primary_key=row[5]
                    ))
                return columns


class PostgreSQLPipeline(SourcePipeline):
    """PostgreSQL CDC pipeline implementation."""

    def __init__(self, rw_client, config: PostgreSQLConfig):
        super().__init__(rw_client, config)
        self.config: PostgreSQLConfig = config

    def create_source_sql(self) -> str:
        """Generate CREATE SOURCE SQL for PostgreSQL CDC."""
        with_items = [
            "connector='postgres-cdc'",
            f"hostname='{self._escape_sql_string(self.config.hostname)}'",
            f"port='{self.config.port}'",
            f"username='{self._escape_sql_string(self.config.username)}'",
            f"password='{self._escape_sql_string(self.config.password)}'",
            f"database.name='{self._escape_sql_string(self.config.database)}'",
            f"schema.name='{self._escape_sql_string(self.config.schema_name)}'",
            # Always include ssl_mode since it's required
            f"ssl.mode='{self.config.ssl_mode}'",
        ]

        # Add optional configurations
        if self.config.ssl_root_cert:
            with_items.append(
                f"ssl.root.cert='{self._escape_sql_string(self.config.ssl_root_cert)}'")
        if self.config.slot_name:
            with_items.append(
                f"slot.name='{self._escape_sql_string(self.config.slot_name)}'")

        # Add publication settings only if explicitly provided by user
        if self.config.publication_name is not None:
            with_items.append(
                f"publication.name='{self._escape_sql_string(self.config.publication_name)}'")
        if self.config.publication_create_enable is not None:
            with_items.append(
                f"publication.create.enable='{str(self.config.publication_create_enable).lower()}'")

        with_items.append(
            f"auto.schema.change='{str(self.config.auto_schema_change).lower()}'")

        # Add Debezium properties
        for key, value in self.config.debezium_properties.items():
            with_items.append(
                f"debezium.{key}='{self._escape_sql_string(str(value))}'")

        # Add extra properties
        for key, value in self.config.extra_properties.items():
            with_items.append(f"{key}='{self._escape_sql_string(str(value))}'")

        with_clause = ",\n    ".join(with_items)

        return f"""-- Step 1: Create the shared CDC source {self.config.source_name}
CREATE SOURCE IF NOT EXISTS {self.config.source_name} WITH (
    {with_clause}
);"""

    def create_table_sql(self, table_info: TableInfo, **kwargs) -> str:
        """Generate CREATE TABLE SQL for a specific table."""
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
            with_clause = f"\nWITH (\n    {',\n    '.join(with_items)}\n)"

        qualified_table_name = f"{rw_schema}.{table_name}" if rw_schema != "public" else table_name

        return f"""-- Step 2: Create the CDC table on top of the shared source
CREATE TABLE IF NOT EXISTS {qualified_table_name} (*) {with_clause}
FROM {self.config.source_name}
TABLE '{table_info.qualified_name}';"""

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes in SQL strings."""
        return value.replace("'", "''") if value else ""
