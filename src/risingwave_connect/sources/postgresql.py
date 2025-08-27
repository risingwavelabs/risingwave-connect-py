"""PostgreSQL-specific discovery and pipeline implementation."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

import psycopg
from pydantic import BaseModel, Field, validator

from ..discovery.base import (
    DatabaseDiscovery,
    SourceConnection,
    SourceConfig,
    TableInfo,
    ColumnInfo,
    ColumnSelection
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
        if not table_names:
            return []

        default_schema = schema_name or self.config.schema_name

        with self._connection() as conn:
            with conn.cursor() as cur:
                # Build conditions for specific tables
                table_conditions = []
                params = []

                for table_name in table_names:
                    if '.' in table_name:
                        schema, table = table_name.split('.', 1)
                    else:
                        schema = default_schema
                        table = table_name

                    table_conditions.append(
                        "(t.table_schema = %s AND t.table_name = %s)")
                    params.extend([schema, table])

                if not table_conditions:
                    return []

                where_clause = " OR ".join(table_conditions)

                cur.execute(f"""
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
                    WHERE ({where_clause})
                    AND t.table_type IN ('BASE TABLE', 'VIEW')
                    ORDER BY t.table_schema, t.table_name
                """, params)

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

    def validate_column_selection(self, table_info: TableInfo, column_selections: List[ColumnSelection]) -> Dict[str, Any]:
        """Validate column selection against actual table schema.

        Args:
            table_info: Table information
            column_selections: List of column selections

        Returns:
            Dictionary with validation results
        """
        from ..discovery.base import map_postgres_type_to_risingwave

        # Get actual table columns
        actual_columns = self.get_table_columns(
            table_info.schema_name, table_info.table_name)
        actual_column_map = {col.column_name: col for col in actual_columns}

        # Validation results
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'column_mapping': {},
            'primary_key_columns': [],
            'missing_columns': [],
            'type_overrides': {}
        }

        # Check each selected column
        selected_column_names = [col.column_name for col in column_selections]
        pk_columns = [
            col.column_name for col in column_selections if col.is_primary_key]

        for col_selection in column_selections:
            col_name = col_selection.column_name

            # Check if column exists in actual table
            if col_name not in actual_column_map:
                validation_result['valid'] = False
                validation_result['errors'].append(
                    f"Column '{col_name}' does not exist in table '{table_info.qualified_name}'"
                )
                validation_result['missing_columns'].append(col_name)
                continue

            actual_col = actual_column_map[col_name]

            # Determine RisingWave type
            if col_selection.risingwave_type:
                rw_type = col_selection.risingwave_type
                validation_result['type_overrides'][col_name] = rw_type
            else:
                rw_type = map_postgres_type_to_risingwave(actual_col.data_type)

            # Check primary key consistency
            if col_selection.is_primary_key and not actual_col.is_primary_key:
                validation_result['errors'].append(
                    f"Column '{col_name}' is marked as primary key in selection but is not a primary key in the upstream table"
                )
                validation_result['valid'] = False
            elif actual_col.is_primary_key and not col_selection.is_primary_key:
                validation_result['warnings'].append(
                    f"Column '{col_name}' is a primary key in upstream table but not marked as such in selection"
                )

            # Check nullability
            if col_selection.is_nullable is not None:
                if col_selection.is_nullable != actual_col.is_nullable:
                    validation_result['warnings'].append(
                        f"Column '{col_name}' nullability override: upstream={actual_col.is_nullable}, selection={col_selection.is_nullable}"
                    )

            validation_result['column_mapping'][col_name] = {
                'postgres_type': actual_col.data_type,
                'risingwave_type': rw_type,
                'is_nullable': col_selection.is_nullable if col_selection.is_nullable is not None else actual_col.is_nullable,
                'is_primary_key': col_selection.is_primary_key,
                'ordinal_position': actual_col.ordinal_position
            }

        # Validate primary key requirements
        actual_pk_columns = [
            col.column_name for col in actual_columns if col.is_primary_key]

        if not pk_columns:
            validation_result['errors'].append(
                "No primary key columns specified. At least one primary key column is required for CDC."
            )
            validation_result['valid'] = False

        # Check if we're missing any actual primary key columns
        missing_pk_columns = set(actual_pk_columns) - \
            set(selected_column_names)
        if missing_pk_columns:
            validation_result['errors'].append(
                f"Missing primary key columns from upstream table: {list(missing_pk_columns)}. "
                f"All primary key columns must be included for consistent CDC."
            )
            validation_result['valid'] = False

        validation_result['primary_key_columns'] = pk_columns

        return validation_result


class PostgreSQLPipeline(SourceConnection):
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
        from ..discovery.base import TableColumnConfig, ColumnSelection, map_postgres_type_to_risingwave

        table_name = kwargs.get('table_name', table_info.table_name)
        rw_schema = kwargs.get('rw_schema', 'public')
        # Optional TableColumnConfig
        column_config = kwargs.get('column_config')

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

        # Handle column-level filtering
        if column_config and isinstance(column_config, TableColumnConfig) and column_config.selected_columns:
            # In dry_run mode, skip actual validation and use mock data
            if hasattr(self, '_dry_run_mode') and self._dry_run_mode:
                # Mock validation for dry run
                validation_result = {
                    'valid': True,
                    'column_mapping': {},
                    'primary_key_columns': [col.column_name for col in column_config.selected_columns if col.is_primary_key]
                }
                for col_selection in column_config.selected_columns:
                    rw_type = col_selection.risingwave_type or "VARCHAR"
                    validation_result['column_mapping'][col_selection.column_name] = {
                        'risingwave_type': rw_type,
                        'is_nullable': col_selection.is_nullable if col_selection.is_nullable is not None else True,
                        'is_primary_key': col_selection.is_primary_key
                    }
            else:
                # Create discovery instance to validate columns
                discovery = PostgreSQLDiscovery(self.config)

                # Validate column selection
                validation_result = discovery.validate_column_selection(
                    table_info, column_config.selected_columns)

            if not validation_result['valid']:
                raise ValueError(
                    f"Column validation failed for table '{table_info.qualified_name}': " +
                    "; ".join(validation_result['errors'])
                )

            # Build explicit column definitions
            column_definitions = []
            primary_key_columns = validation_result['primary_key_columns']

            for col_selection in column_config.selected_columns:
                col_name = col_selection.column_name
                col_mapping = validation_result['column_mapping'][col_name]

                # Build column definition
                col_def = f"{col_name} {col_mapping['risingwave_type']}"

                # Add constraints
                if col_mapping['is_primary_key']:
                    col_def += " PRIMARY KEY"
                elif not col_mapping['is_nullable']:
                    col_def += " NOT NULL"

                column_definitions.append(col_def)

            # Create table with explicit schema
            columns_sql = ",\n    ".join(column_definitions)

            return f"""-- Step 2: Create the CDC table with column filtering on top of the shared source
CREATE TABLE IF NOT EXISTS {qualified_table_name} (
    {columns_sql}
) {with_clause}
FROM {self.config.source_name}
TABLE '{table_info.qualified_name}';"""

        else:
            # Default behavior: include all columns
            return f"""-- Step 2: Create the CDC table on top of the shared source
CREATE TABLE IF NOT EXISTS {qualified_table_name} (*) {with_clause}
FROM {self.config.source_name}
TABLE '{table_info.qualified_name}';"""

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes in SQL strings."""
        return value.replace("'", "''") if value else ""
