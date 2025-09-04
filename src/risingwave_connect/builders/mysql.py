"""MySQL CDC source builder for RisingWave."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union

from ..sources.mysql import MySQLConfig
from ..discovery.base import TableSelector, TableInfo, TableColumnConfig
from .base import BaseSourceBuilder

logger = logging.getLogger(__name__)


class MySQLBuilder(BaseSourceBuilder):
    """Builder for MySQL CDC sources in RisingWave."""

    def create_connection(
        self,
        config: MySQLConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        column_configs: Optional[Dict[str, TableColumnConfig]] = None,
        dry_run: bool = False,
        include_timestamp: bool = False,
        include_database_name: bool = False,
        include_table_name: bool = False,
        snapshot: bool = True,
        snapshot_interval: int = 1,
        snapshot_batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Create a complete MySQL CDC connection with table discovery.

        Args:
            config: MySQL configuration
            table_selector: Table selection criteria
            column_configs: Column configurations per table
            dry_run: If True, return SQL without executing
            include_timestamp: Include CDC timestamp metadata
            include_database_name: Include database name metadata
            include_table_name: Include table name metadata
            snapshot: Enable CDC backfill (default: True)
            snapshot_interval: Barrier interval for buffering upstream events
            snapshot_batch_size: Batch size of snapshot read query
        """
        try:
            # Create source
            source_name = config.get_source_name()
            source_sql = self._create_mysql_source(config, source_name)

            # Discover and select tables
            available_tables = self.discover_tables(config)

            # Handle table selection
            if isinstance(table_selector, list):
                table_selector = TableSelector(specific_tables=table_selector)
            elif table_selector is None:
                table_selector = TableSelector(include_all=True)

            selected_tables = table_selector.select_tables(available_tables)

            if not selected_tables:
                logger.warning("No tables selected for MySQL CDC")
                return {
                    "success_summary": {
                        "overall_success": False,
                        "error": "No tables selected"
                    },
                    "sql_statements": [source_sql],
                    "selected_tables": []
                }

            # Create table SQL statements
            sql_statements = [source_sql]
            created_tables = []

            for table_info in selected_tables:
                # Get column configuration for this table
                table_column_config = column_configs.get(
                    table_info.table_name) if column_configs else None

                # Get table columns with retry logic
                columns = self._get_table_columns_with_retry(
                    config, table_info.table_name)

                # Create table SQL
                table_sql = self._create_mysql_cdc_table(
                    config, source_name, table_info, columns, table_column_config,
                    include_timestamp, include_database_name, include_table_name,
                    snapshot, snapshot_interval, snapshot_batch_size
                )

                sql_statements.append(table_sql)
                created_tables.append(table_info.table_name)

            if not dry_run:
                # Execute SQL statements
                for sql in sql_statements:
                    self.rw_client.execute_sql(sql)

            # Return success result
            return {
                "success_summary": {
                    "overall_success": True,
                    "total_tables": len(created_tables),
                    "total_sources": 1,
                    "created_tables": created_tables
                },
                "source_info": {
                    "source_name": source_name,
                    "source_type": "mysql-cdc",
                    "database": config.database,
                    "hostname": config.hostname,
                    "connection_url": config.get_connection_url()
                },
                "table_info": [
                    {
                        "table_name": table.table_name,
                        "schema_name": table.schema_name,
                        "estimated_rows": table.row_count,
                        "table_type": table.table_type
                    }
                    for table in selected_tables
                ],
                "sql_statements": sql_statements,
                "selected_tables": created_tables
            }

        except Exception as e:
            logger.error(f"Failed to create MySQL CDC connection: {e}")
            return {
                "success_summary": {
                    "overall_success": False,
                    "error": str(e)
                },
                "sql_statements": [],
                "selected_tables": []
            }

    def _create_mysql_source(self, config: MySQLConfig, source_name: str) -> str:
        """Create MySQL CDC source SQL statement."""
        properties = config.to_source_properties()

        # Build WITH clause
        with_clauses = []
        for key, value in properties.items():
            with_clauses.append(f"    {key}='{value}'")

        with_clause = ",\n".join(with_clauses)

        sql = f"""-- Step 1: Create the MySQL CDC source {source_name}
CREATE SOURCE IF NOT EXISTS {source_name} WITH (
{with_clause}
);"""

        return sql

    def _create_mysql_cdc_table(
        self,
        config: MySQLConfig,
        source_name: str,
        table_info: TableInfo,
        columns: List[Dict[str, Any]],
        column_config: Optional[TableColumnConfig] = None,
        include_timestamp: bool = False,
        include_database_name: bool = False,
        include_table_name: bool = False,
        snapshot: bool = True,
        snapshot_interval: int = 1,
        snapshot_batch_size: int = 1000
    ) -> str:
        """Create MySQL CDC table SQL statement."""

        # Build column definitions and detect primary keys
        column_definitions = []
        primary_keys = []
        included_columns = set()
        excluded_columns = set()

        if column_config:
            included_columns = set(column_config.included_columns or [])
            excluded_columns = set(column_config.excluded_columns or [])

        for col in columns:
            col_name = col["column_name"]

            # Apply column filtering
            if included_columns and col_name not in included_columns:
                continue
            if col_name in excluded_columns:
                continue

            # Map MySQL types to RisingWave types
            rw_type = self._map_mysql_type_to_risingwave(
                col["data_type"], col.get("character_maximum_length"))

            # Check if this is a primary key column
            is_primary = col.get("is_primary_key", False) or col.get(
                "column_key") == "PRI"
            if is_primary:
                primary_keys.append(col_name)
                column_definitions.append(
                    f"    {col_name} {rw_type} PRIMARY KEY")
            else:
                column_definitions.append(f"    {col_name} {rw_type}")

        # Add primary key constraint if multiple columns
        if len(primary_keys) > 1:
            pk_constraint = f"    PRIMARY KEY ({', '.join(primary_keys)})"
            column_definitions.append(pk_constraint)

        # Build INCLUDE clauses for metadata
        include_clauses = []
        if include_timestamp:
            include_clauses.append("INCLUDE commit_ts AS commit_ts")
        if include_database_name:
            include_clauses.append("INCLUDE database_name AS database_name")
        if include_table_name:
            include_clauses.append("INCLUDE table_name AS table_name")

        include_clause = "\n" + \
            "\n".join(include_clauses) if include_clauses else ""

        # Build WITH clause for CDC table parameters
        with_clauses = []
        if not snapshot:
            with_clauses.append("snapshot='false'")
        if snapshot_interval != 1:
            with_clauses.append(f"snapshot.interval='{snapshot_interval}'")
        if snapshot_batch_size != 1000:
            with_clauses.append(f"snapshot.batch_size='{snapshot_batch_size}'")

        with_clause = ""
        if with_clauses:
            with_clause = "\nWITH (\n    " + \
                ",\n    ".join(with_clauses) + "\n)"

        # Column definitions or empty if none
        columns_clause = "\n".join(
            column_definitions) if column_definitions else "    "

        # Full table name with database and schema/table
        # Format should be 'database.table' as per RisingWave docs
        full_table_name = f"{config.database}.{table_info.table_name}"

        sql = f"""-- MySQL CDC Table: {table_info.table_name}
-- Source: {full_table_name} ({table_info.row_count or 'unknown'} rows)
CREATE TABLE IF NOT EXISTS {table_info.table_name} (
{columns_clause}
){include_clause}{with_clause}
FROM {source_name} TABLE '{full_table_name}';"""

        return sql

    def _map_mysql_type_to_risingwave(self, mysql_type: str, max_length: Optional[int] = None) -> str:
        """Map MySQL data types to RisingWave data types."""
        mysql_type = mysql_type.upper()

        # Integer types
        if mysql_type in ["TINYINT"]:
            return "SMALLINT"
        elif mysql_type in ["SMALLINT"]:
            return "SMALLINT"
        elif mysql_type in ["MEDIUMINT", "INT", "INTEGER"]:
            return "INTEGER"
        elif mysql_type == "BIGINT":
            return "BIGINT"

        # Unsigned integer types (map to next larger signed type)
        elif mysql_type in ["TINYINT UNSIGNED", "SMALLINT UNSIGNED"]:
            return "INTEGER"
        elif mysql_type in ["MEDIUMINT UNSIGNED", "INT UNSIGNED", "INTEGER UNSIGNED"]:
            return "BIGINT"
        elif mysql_type == "BIGINT UNSIGNED":
            return "DECIMAL(20,0)"  # Use DECIMAL for unsigned BIGINT

        # Decimal types
        elif mysql_type in ["DECIMAL", "NUMERIC", "DEC", "FIXED"]:
            return "DECIMAL"
        elif mysql_type in ["FLOAT", "REAL"]:
            return "REAL"
        elif mysql_type in ["DOUBLE", "DOUBLE PRECISION"]:
            return "DOUBLE PRECISION"

        # String types
        elif mysql_type in ["CHAR", "VARCHAR"]:
            if max_length:
                return f"VARCHAR({max_length})"
            return "VARCHAR"
        elif mysql_type in ["TINYTEXT"]:
            return "VARCHAR(255)"
        elif mysql_type in ["TEXT"]:
            return "TEXT"
        elif mysql_type in ["MEDIUMTEXT", "LONGTEXT"]:
            return "TEXT"

        # Binary types
        elif mysql_type in ["BINARY", "VARBINARY"]:
            return "BYTEA"
        elif mysql_type in ["TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"]:
            return "BYTEA"

        # Date/time types
        elif mysql_type == "DATE":
            return "DATE"
        elif mysql_type == "TIME":
            return "TIME"
        elif mysql_type in ["DATETIME"]:
            return "TIMESTAMP"
        elif mysql_type == "TIMESTAMP":
            return "TIMESTAMPTZ"
        elif mysql_type == "YEAR":
            return "SMALLINT"

        # JSON type
        elif mysql_type == "JSON":
            return "JSONB"

        # Boolean type (TINYINT(1) in MySQL)
        elif mysql_type in ["BOOLEAN", "BOOL"]:
            return "BOOLEAN"

        # Enum and Set (convert to text)
        elif mysql_type in ["ENUM", "SET"]:
            return "VARCHAR"

        # Geometry types (convert to text for now)
        elif mysql_type in ["GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"]:
            return "TEXT"

        # Bit type
        elif mysql_type == "BIT":
            return "BIT"

        # Default fallback
        else:
            logger.warning(f"Unknown MySQL type '{mysql_type}', using VARCHAR")
            return "VARCHAR"

    def _get_table_columns_with_retry(self, config: MySQLConfig, table_name: str) -> List[Dict[str, Any]]:
        """Get table columns with retry logic (placeholder for MySQL connection)."""
        try:
            # This would connect to MySQL and get column information
            # For now, return empty list to allow dry run testing
            logger.warning(
                f"MySQL connection not implemented, returning empty columns for {table_name}")
            return []

        except Exception as e:
            logger.error(f"Failed to get table columns: {e}")
            return []

    def discover_tables(
        self,
        config: MySQLConfig,
        schema_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available tables in MySQL database."""
        try:
            # This would connect to MySQL and discover tables
            # For now, return placeholder data for testing
            logger.warning(
                "MySQL table discovery not implemented, returning placeholder data")

            # Return some example tables for testing
            return [
                TableInfo(
                    schema_name=schema_name or config.database,
                    table_name="users",
                    table_type="BASE TABLE",
                    row_count=0,  # Unknown without connection
                    size_bytes=None
                ),
                TableInfo(
                    schema_name=schema_name or config.database,
                    table_name="orders",
                    table_type="BASE TABLE",
                    row_count=0,
                    size_bytes=None
                )
            ]

        except Exception as e:
            logger.error(f"Failed to discover MySQL tables: {e}")
            return []

    def get_schemas(self, config: MySQLConfig) -> List[str]:
        """Get list of available schemas in MySQL database."""
        try:
            # This would connect to MySQL and get schema list
            # For now, return the configured database
            logger.warning(
                "MySQL schema discovery not implemented, returning configured database")
            return [config.database]

        except Exception as e:
            logger.error(f"Failed to get MySQL schemas: {e}")
            return []
