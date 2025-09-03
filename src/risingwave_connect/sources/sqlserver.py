"""SQL Server-specific discovery and pipeline implementation."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union
from contextlib import contextmanager

try:
    import pyodbc
except ImportError:
    pyodbc = None

from pydantic import BaseModel, Field, field_validator

from ..discovery.base import (
    DatabaseDiscovery,
    SourceConnection,
    SourceConfig,
    TableInfo,
    ColumnInfo,
    ColumnSelection
)

logger = logging.getLogger(__name__)


class SQLServerConfig(SourceConfig):
    """SQL Server-specific configuration.

    Args:
        hostname: SQL Server hostname or IP address
        port: SQL Server port (default: 1433)
        username: SQL Server username
        password: SQL Server password
        database: SQL Server database name
        schema_name: SQL Server schema name (default: dbo)
        table_name: Table name(s) to ingest data from. Use schema.table format.
                   Supports patterns like 'dbo.*' for all tables in a schema,
                   or 'dbo.*, sales.*' for tables across multiple schemas.
        database_encrypt: Optional. Enable SSL encryption (default: False)
        snapshot: Whether to enable snapshot for initial data load (default: True)
        snapshot_interval: Barrier interval for buffering upstream events (default: 1)
        snapshot_batch_size: Batch size for snapshot read queries (default: 1000)
    """

    port: int = 1433
    schema_name: str = "dbo"
    table_name: str
    database_encrypt: bool = False

    # CDC specific options
    snapshot: bool = True
    snapshot_interval: int = 1
    snapshot_batch_size: int = 1000

    @field_validator('table_name')
    @classmethod
    def validate_table_name(cls, v):
        """Validate table name pattern."""
        if not v:
            raise ValueError("table_name is required")
        return v

    @field_validator('hostname')
    @classmethod
    def validate_hostname(cls, v):
        """Validate hostname."""
        if not v or not v.strip():
            raise ValueError("hostname is required")
        return v

    @field_validator('database')
    @classmethod
    def validate_database(cls, v):
        """Validate database name."""
        if not v or not v.strip():
            raise ValueError("database is required")
        return v

    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        """Validate port number."""
        if not (1 <= v <= 65535):
            raise ValueError("port must be between 1 and 65535")
        return v

    def get_schema_names(self) -> List[str]:
        """Extract schema names from table patterns."""
        schemas = set()
        for pattern in self.table_name.split(','):
            pattern = pattern.strip()
            if '.' in pattern:
                schema_name = pattern.split('.')[0]
                schemas.add(schema_name)
            else:
                schemas.add(self.schema_name)
        return list(schemas)

    def get_table_patterns(self) -> List[str]:
        """Get list of table patterns."""
        return [pattern.strip() for pattern in self.table_name.split(',')]

    def get_connection_string(self) -> str:
        """Generate SQL Server connection string."""
        driver = "{ODBC Driver 17 for SQL Server}"
        encrypt = "yes" if self.database_encrypt else "no"

        return (
            f"DRIVER={driver};"
            f"SERVER={self.hostname},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Encrypt={encrypt};"
            "TrustServerCertificate=yes;"
        )


class SQLServerDiscovery(DatabaseDiscovery):
    """SQL Server database discovery implementation."""

    def __init__(self, config: SQLServerConfig):
        self.config = config
        self.connection_string = config.get_connection_string()

    @contextmanager
    def get_connection(self):
        """Get SQL Server database connection."""
        if pyodbc is None:
            raise ImportError(
                "pyodbc is required for SQL Server connectivity. "
                "Install it with: pip install 'risingwave-connect-py[sqlserver]' or pip install pyodbc"
            )

        conn = None
        try:
            conn = pyodbc.connect(self.connection_string, timeout=10)
            yield conn
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            raise ConnectionError(f"SQL Server connection failed: {e}")
        finally:
            if conn:
                conn.close()

    def test_connection(self) -> Dict[str, Any]:
        """Test SQL Server connection."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION as version")
                result = cursor.fetchone()

                # Get database info
                cursor.execute(
                    "SELECT DB_NAME() as database_name, SCHEMA_NAME() as default_schema"
                )
                db_info = cursor.fetchone()

                return {
                    "success": True,
                    "message": "Connection successful",
                    "server_version": result.version if result else "Unknown",
                    "database_name": db_info.database_name if db_info else self.config.database,
                    "default_schema": db_info.default_schema if db_info else "dbo"
                }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
                "error": str(e)
            }

    def list_schemas(self) -> List[str]:
        """List available schemas in SQL Server database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT SCHEMA_NAME 
                    FROM INFORMATION_SCHEMA.SCHEMATA 
                    WHERE SCHEMA_NAME NOT IN ('sys', 'information_schema')
                    ORDER BY SCHEMA_NAME
                """)

                schemas = [row.SCHEMA_NAME for row in cursor.fetchall()]
                return schemas
        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
            return []

    def list_tables(self, schema_name: Optional[str] = None) -> List[TableInfo]:
        """List available tables in SQL Server database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Use provided schema or default to config schema
                target_schema = schema_name or self.config.schema_name

                # Query for tables with row counts
                query = """
                    SELECT 
                        t.TABLE_SCHEMA,
                        t.TABLE_NAME,
                        ISNULL(p.rows, 0) as row_count
                    FROM INFORMATION_SCHEMA.TABLES t
                    LEFT JOIN (
                        SELECT 
                            SCHEMA_NAME(o.schema_id) as schema_name,
                            o.name as table_name,
                            SUM(p.rows) as rows
                        FROM sys.objects o
                        JOIN sys.partitions p ON o.object_id = p.object_id
                        WHERE o.type = 'U' AND p.index_id IN (0, 1)
                        GROUP BY o.schema_id, o.name
                    ) p ON t.TABLE_SCHEMA = p.schema_name AND t.TABLE_NAME = p.table_name
                    WHERE t.TABLE_TYPE = 'BASE TABLE'
                """

                if target_schema:
                    query += " AND t.TABLE_SCHEMA = ?"
                    cursor.execute(query, (target_schema,))
                else:
                    cursor.execute(query)

                tables = []
                for row in cursor.fetchall():
                    tables.append(TableInfo(
                        schema_name=row.TABLE_SCHEMA,
                        table_name=row.TABLE_NAME,
                        table_type=row.TABLE_TYPE,
                        row_count=None,  # Could be fetched with additional query
                        size_bytes=None,  # Could be fetched with additional query
                        comment=None
                    ))

                return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []

    def check_specific_tables(self, table_patterns: List[str]) -> List[TableInfo]:
        """Check if specific tables exist and return their info."""
        found_tables = []

        try:
            all_tables = self.list_tables()

            for pattern in table_patterns:
                pattern = pattern.strip()

                if pattern.endswith('.*'):
                    # Schema-level pattern (e.g., 'dbo.*')
                    schema_prefix = pattern[:-2]
                    matching_tables = [
                        table for table in all_tables
                        if table.schema_name == schema_prefix
                    ]
                    found_tables.extend(matching_tables)
                elif '.' in pattern:
                    # Specific table (e.g., 'dbo.users')
                    schema_name, table_name = pattern.split('.', 1)
                    matching_table = next(
                        (table for table in all_tables
                         if table.schema_name == schema_name and table.table_name == table_name),
                        None
                    )
                    if matching_table:
                        found_tables.append(matching_table)
                else:
                    # Table name without schema, use default schema
                    matching_table = next(
                        (table for table in all_tables
                         if table.table_name == pattern and table.schema_name == self.config.schema_name),
                        None
                    )
                    if matching_table:
                        found_tables.append(matching_table)

        except Exception as e:
            logger.error(f"Failed to check specific tables: {e}")

        return found_tables

    def get_table_columns(self, schema_name: str, table_name: str) -> List[ColumnInfo]:
        """Get column information for a specific table."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Get column information including primary keys
                query = """
                    SELECT 
                        c.COLUMN_NAME,
                        c.DATA_TYPE,
                        c.IS_NULLABLE,
                        c.ORDINAL_POSITION,
                        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    LEFT JOIN (
                        SELECT ku.COLUMN_NAME
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku 
                            ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                        WHERE tc.TABLE_SCHEMA = ? 
                            AND tc.TABLE_NAME = ?
                            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
                    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
                    ORDER BY c.ORDINAL_POSITION
                """

                cursor.execute(
                    query, (schema_name, table_name, schema_name, table_name))

                columns = []
                for row in cursor.fetchall():
                    columns.append(ColumnInfo(
                        column_name=row.COLUMN_NAME,
                        data_type=self._map_sqlserver_type_to_risingwave(
                            row.DATA_TYPE),
                        is_nullable=row.IS_NULLABLE == "YES",
                        is_primary_key=bool(row.IS_PRIMARY_KEY),
                        ordinal_position=row.ORDINAL_POSITION
                    ))

                return columns
        except Exception as e:
            logger.error(f"Failed to get table columns: {e}")
            return []

    def _map_sqlserver_type_to_risingwave(self, sqlserver_type: str) -> str:
        """Map SQL Server data types to RisingWave types."""
        type_mapping = {
            # Numeric types
            'tinyint': 'SMALLINT',
            'smallint': 'SMALLINT',
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'decimal': 'DECIMAL',
            'numeric': 'DECIMAL',
            'float': 'DOUBLE PRECISION',
            'real': 'REAL',
            'money': 'DECIMAL(19,4)',
            'smallmoney': 'DECIMAL(10,4)',

            # String types
            'char': 'CHAR',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'nchar': 'CHAR',
            'nvarchar': 'VARCHAR',
            'ntext': 'TEXT',

            # Date/time types
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'TIMESTAMP',
            'datetime2': 'TIMESTAMP',
            'smalldatetime': 'TIMESTAMP',
            'datetimeoffset': 'TIMESTAMPTZ',

            # Binary types
            'binary': 'BYTEA',
            'varbinary': 'BYTEA',
            'image': 'BYTEA',

            # Other types
            'bit': 'BOOLEAN',
            'uniqueidentifier': 'UUID',
            'xml': 'TEXT',
            'sql_variant': 'TEXT'
        }

        return type_mapping.get(sqlserver_type.lower(), 'TEXT')

    def validate_column_selection(self, table_info: TableInfo, column_selections: List[ColumnSelection]) -> Dict[str, Any]:
        """Validate column selection against SQL Server table schema."""
        actual_columns = self.get_table_columns(
            table_info.schema_name, table_info.table_name)
        actual_column_map = {col.column_name: col for col in actual_columns}

        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'column_mapping': {},
            'primary_key_columns': [],
            'missing_columns': [],
            'type_overrides': {}
        }

        selected_column_names = [col.column_name for col in column_selections]
        pk_columns = [
            col.column_name for col in column_selections if col.is_primary_key]

        for col_selection in column_selections:
            col_name = col_selection.column_name

            if col_name not in actual_column_map:
                validation_result['valid'] = False
                validation_result['errors'].append(
                    f"Column '{col_name}' does not exist in table {table_info.qualified_name}. "
                    f"Available columns: {list(actual_column_map.keys())}"
                )
                validation_result['missing_columns'].append(col_name)
                continue

            actual_col = actual_column_map[col_name]

            if col_selection.risingwave_type:
                rw_type = col_selection.risingwave_type
                validation_result['type_overrides'][col_name] = rw_type
            else:
                rw_type = actual_col.data_type

            if col_selection.is_primary_key and not actual_col.is_primary_key:
                validation_result['errors'].append(
                    f"Column '{col_name}' is marked as primary key in selection but is not a primary key in SQL Server table"
                )
                validation_result['valid'] = False
            elif actual_col.is_primary_key and not col_selection.is_primary_key:
                validation_result['warnings'].append(
                    f"Column '{col_name}' is a primary key in SQL Server table but not marked as such in selection"
                )

            validation_result['column_mapping'][col_name] = {
                'sqlserver_type': actual_col.data_type,
                'risingwave_type': rw_type,
                'is_nullable': col_selection.is_nullable if col_selection.is_nullable is not None else actual_col.is_nullable,
                'is_primary_key': col_selection.is_primary_key,
                'ordinal_position': actual_col.ordinal_position
            }

        if not pk_columns:
            validation_result['errors'].append(
                "No primary key columns specified. SQL Server CDC requires at least one primary key column."
            )
            validation_result['valid'] = False

        validation_result['primary_key_columns'] = pk_columns

        return validation_result


class SQLServerSourceConnection(SourceConnection):
    """SQL Server CDC source connection implementation."""

    def __init__(self, rw_client, config: SQLServerConfig):
        super().__init__(rw_client, config)
        self.config: SQLServerConfig = config

    def _generate_source_name(self) -> str:
        """Generate a default source name for SQL Server."""
        clean_db = self.config.database.replace(
            '-', '_').replace('.', '_').replace(' ', '_')
        base_name = f"sqlserver_cdc_{clean_db}"
        return base_name

    def create_source_sql(self) -> str:
        """Generate CREATE SOURCE SQL for SQL Server CDC."""
        with_items = [
            "connector='sqlserver-cdc'",
            f"hostname='{self._escape_sql_string(self.config.hostname)}'",
            f"port='{self.config.port}'",
            f"username='{self._escape_sql_string(self.config.username)}'",
            f"password='{self._escape_sql_string(self.config.password)}'",
            f"database.name='{self._escape_sql_string(self.config.database)}'",
        ]

        if self.config.database_encrypt:
            with_items.append("database.encrypt='true'")

        with_clause = ",\n    ".join(with_items)

        return f"""-- Step 1: Create the SQL Server CDC source {self.config.source_name}
CREATE SOURCE IF NOT EXISTS {self.config.source_name} WITH (
    {with_clause}
);"""

    def create_table_sql(self, table_info: TableInfo, **kwargs) -> str:
        """Generate CREATE TABLE SQL for SQL Server CDC.

        Args:
            table_info: Table information
            **kwargs: Additional parameters including:
                - table_name: Override table name in RisingWave
                - rw_schema: RisingWave schema name
                - include_timestamp: Whether to include timestamp metadata (default: False)
                - include_database_name: Whether to include database name metadata (default: False)
                - include_schema_name: Whether to include schema name metadata (default: False)
                - include_table_name: Whether to include table name metadata (default: False)
                - column_config: TableColumnConfig for column filtering
        """
        table_name = kwargs.get('table_name', table_info.table_name)
        rw_schema = kwargs.get('rw_schema', 'public')
        include_timestamp = kwargs.get('include_timestamp', False)
        include_database_name = kwargs.get('include_database_name', False)
        include_schema_name = kwargs.get('include_schema_name', False)
        include_table_name = kwargs.get('include_table_name', False)
        column_config = kwargs.get('column_config')

        qualified_table_name = f"{rw_schema}.{table_name}" if rw_schema != "public" else table_name

        # Get column definitions
        if column_config and column_config.column_selections:
            # Use filtered columns
            columns_sql = self._generate_filtered_columns_sql(
                table_info, column_config.column_selections)
        else:
            # Use all columns
            table_columns = SQLServerDiscovery(self.config).get_table_columns(
                table_info.schema_name, table_info.table_name
            )
            columns_sql = self._generate_columns_sql(table_columns)

        # Include clauses for metadata
        include_clauses = []
        if include_timestamp:
            include_clauses.append("INCLUDE timestamp AS commit_ts")
        if include_database_name:
            include_clauses.append("INCLUDE database_name AS database_name")
        if include_schema_name:
            include_clauses.append("INCLUDE schema_name AS schema_name")
        if include_table_name:
            include_clauses.append("INCLUDE table_name AS table_name")

        include_sql = ""
        if include_clauses:
            include_sql = "\n" + "\n".join(include_clauses)

        # WITH clause options
        with_items = []
        if not self.config.snapshot:
            with_items.append("snapshot='false'")
        if self.config.snapshot_interval != 1:
            with_items.append(
                f"snapshot.interval='{self.config.snapshot_interval}'")
        if self.config.snapshot_batch_size != 1000:
            with_items.append(
                f"snapshot.batch_size='{self.config.snapshot_batch_size}'")

        with_sql = ""
        if with_items:
            with_sql = f"\nWITH (\n    {', '.join(with_items)}\n)"

        # Format row count for comment
        row_count_str = f"{table_info.row_count:,}" if table_info.row_count is not None else "unknown"

        return f"""-- SQL Server CDC Table: {qualified_table_name}
-- Source: {table_info.qualified_name} ({row_count_str} rows)
CREATE TABLE IF NOT EXISTS {qualified_table_name} (
    {columns_sql}
){include_sql}{with_sql}
FROM {self.config.source_name} TABLE '{table_info.qualified_name}';"""

    def _generate_columns_sql(self, columns: List[ColumnInfo]) -> str:
        """Generate column definitions SQL."""
        column_defs = []
        pk_columns = []

        for col in columns:
            nullable = "" if col.is_nullable else " NOT NULL"
            column_def = f"{col.column_name} {col.data_type}{nullable}"

            if col.is_primary_key:
                pk_columns.append(col.column_name)

            column_defs.append(column_def)

        # Add primary key constraint
        if pk_columns:
            if len(pk_columns) == 1:
                # Single column primary key - add inline
                for i, col_def in enumerate(column_defs):
                    if col_def.startswith(pk_columns[0] + " "):
                        column_defs[i] = col_def.replace(
                            pk_columns[0] + " ", f"{pk_columns[0]} ", 1)
                        if " NOT NULL" in column_defs[i]:
                            column_defs[i] = column_defs[i].replace(
                                " NOT NULL", " PRIMARY KEY")
                        else:
                            column_defs[i] += " PRIMARY KEY"
                        break
            else:
                # Multi-column primary key
                pk_constraint = f"PRIMARY KEY ({', '.join(pk_columns)})"
                column_defs.append(pk_constraint)

        return ",\n    ".join(column_defs)

    def _generate_filtered_columns_sql(self, table_info: TableInfo, column_selections: List[ColumnSelection]) -> str:
        """Generate filtered column definitions SQL."""
        column_defs = []
        pk_columns = []

        for col_selection in column_selections:
            data_type = col_selection.risingwave_type or "TEXT"
            nullable = "" if col_selection.is_nullable else " NOT NULL"
            column_def = f"{col_selection.column_name} {data_type}{nullable}"

            if col_selection.is_primary_key:
                pk_columns.append(col_selection.column_name)

            column_defs.append(column_def)

        # Add primary key constraint
        if pk_columns:
            if len(pk_columns) == 1:
                # Single column primary key - add inline
                for i, col_def in enumerate(column_defs):
                    if col_def.startswith(pk_columns[0] + " "):
                        if " NOT NULL" in column_defs[i]:
                            column_defs[i] = column_defs[i].replace(
                                " NOT NULL", " PRIMARY KEY")
                        else:
                            column_defs[i] += " PRIMARY KEY"
                        break
            else:
                # Multi-column primary key
                pk_constraint = f"PRIMARY KEY ({', '.join(pk_columns)})"
                column_defs.append(pk_constraint)

        return ",\n    ".join(column_defs)

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes in SQL strings."""
        return value.replace("'", "''")
