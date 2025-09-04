"""SQL Server source builder."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union

from .base import BaseSourceBuilder
from ..discovery.base import TableSelector, TableInfo, TableColumnConfig
from ..sources.sqlserver import SQLServerConfig, SQLServerDiscovery, SQLServerSourceConnection

logger = logging.getLogger(__name__)


class SQLServerBuilder(BaseSourceBuilder):
    """SQL Server CDC source builder."""

    def create_connection(
        self,
        config: SQLServerConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        column_configs: Optional[Dict[str, TableColumnConfig]] = None,
        dry_run: bool = False,
        include_timestamp: bool = False,
        include_database_name: bool = False,
        include_schema_name: bool = False,
        include_table_name: bool = False
    ) -> Dict[str, Any]:
        """Create a complete SQL Server CDC connection with table discovery."""
        # Initialize discovery and connection
        discovery = SQLServerDiscovery(config)
        sqlserver_source = SQLServerSourceConnection(self.rw_client, config)

        # Test connection (skip in dry run mode)
        if not dry_run:
            connection_test = discovery.test_connection()
            if not connection_test.get("success"):
                raise ConnectionError(
                    f"Cannot connect to SQL Server at {config.hostname}:{config.port}. "
                    f"Error: {connection_test.get('message', 'Unknown error')}"
                )

        # Convert table_selector if it's a list
        if isinstance(table_selector, list):
            table_selector = TableSelector(specific_tables=table_selector)

        # Discover tables (skip validation in dry run mode)
        if dry_run:
            # In dry run mode, create placeholder tables without validation
            if table_selector and table_selector.specific_tables:
                selected_tables = self._create_placeholder_tables(
                    table_selector.specific_tables, 'dbo')
            else:
                # Default: use config patterns for dry run
                patterns = config.get_table_patterns()
                selected_tables = self._create_placeholder_tables(
                    patterns, 'dbo')
        else:
            # Normal mode: validate tables with actual discovery
            if table_selector and table_selector.specific_tables:
                # Check specific tables
                selected_tables = discovery.check_specific_tables(
                    table_selector.specific_tables)
            elif table_selector and (table_selector.include_patterns or table_selector.include_all):
                # Use patterns or include all
                all_tables = discovery.list_tables()
                selected_tables = table_selector.select_tables(all_tables)
            else:
                # Default: discover tables based on config patterns
                patterns = config.get_table_patterns()
                selected_tables = discovery.check_specific_tables(patterns)

        if not selected_tables:
            logger.warning("No tables found matching the selection criteria")
            return {
                "success": False,
                "message": "No tables found matching the selection criteria",
                "selected_tables": [],
                "sql_statements": [],
                "failed_statements": []
            }

        # Create source first
        source_sql = sqlserver_source.create_source_sql()

        # Prepare table creation
        sql_statements = [source_sql]
        failed_statements = []
        successful_tables = []

        # Process each selected table
        for table_info in selected_tables:
            try:
                # Get column config for this table if provided
                column_config = None
                if column_configs:
                    table_key = table_info.table_name
                    if table_key not in column_configs:
                        # Try qualified name
                        table_key = table_info.qualified_name
                    column_config = column_configs.get(table_key)

                # Validate column config if provided
                if column_config and not dry_run:
                    validation_result = discovery.validate_column_selection(
                        table_info, column_config.column_selections
                    )
                    if not validation_result['valid']:
                        error_msg = f"Column validation failed for {table_info.qualified_name}: {validation_result['errors']}"
                        failed_statements.append({
                            "table": table_info.qualified_name,
                            "error": error_msg,
                            "sql": "-- Column validation failed"
                        })
                        continue

                # Generate table SQL
                table_sql = sqlserver_source.create_table_sql(
                    table_info,
                    column_config=column_config,
                    include_timestamp=include_timestamp,
                    include_database_name=include_database_name,
                    include_schema_name=include_schema_name,
                    include_table_name=include_table_name
                )
                sql_statements.append(table_sql)
                successful_tables.append(table_info)

            except Exception as e:
                error_msg = f"Failed to create table {table_info.qualified_name}: {str(e)}"
                logger.error(error_msg)
                failed_statements.append({
                    "table": table_info.qualified_name,
                    "error": error_msg,
                    "sql": f"-- Error: {str(e)}"
                })

        # Execute SQL statements if not dry run
        execution_results = []
        if not dry_run and sql_statements:
            try:
                # Execute source creation
                source_result = self.rw_client.execute_sql(source_sql)
                execution_results.append({
                    "sql": source_sql,
                    "success": source_result.get("success", True),
                    "message": source_result.get("message", "Source created")
                })

                # Execute table creations
                for i, table_sql in enumerate(sql_statements[1:], 1):
                    try:
                        table_result = self.rw_client.execute_sql(table_sql)
                        execution_results.append({
                            "sql": table_sql,
                            "success": table_result.get("success", True),
                            "message": table_result.get("message", f"Table {i} created")
                        })
                    except Exception as e:
                        execution_results.append({
                            "sql": table_sql,
                            "success": False,
                            "message": f"Failed to execute table SQL: {str(e)}"
                        })

            except Exception as e:
                logger.error(f"Failed to execute source SQL: {e}")
                execution_results.append({
                    "sql": source_sql,
                    "success": False,
                    "message": f"Failed to create source: {str(e)}"
                })

        # Generate summary
        total_tables = len(selected_tables)
        successful_count = len(successful_tables)
        failed_count = len(failed_statements)

        return {
            "success_summary": {
                "overall_success": failed_count == 0,
                "total_tables": total_tables,
                "successful_tables": successful_count,
                "failed_tables": failed_count
            },
            "selected_tables": successful_tables,
            "sql_statements": sql_statements,
            "failed_statements": failed_statements,
            "execution_results": execution_results if not dry_run else [],
            "dry_run": dry_run,
            "executed": not dry_run,
            "partial_success": len(failed_statements) > 0
        }

    def discover_tables(
        self,
        config: SQLServerConfig,
        schema_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available tables in SQL Server database."""
        discovery = SQLServerDiscovery(config)

        connection_test = discovery.test_connection()
        if not connection_test.get("success"):
            raise ConnectionError(
                f"Cannot connect to SQL Server at {config.hostname}:{config.port}. "
                f"Error: {connection_test.get('message', 'Unknown error')}"
            )

        return discovery.list_tables(schema_name)

    def get_schemas(self, config: SQLServerConfig) -> List[str]:
        """Get list of available schemas in SQL Server database."""
        discovery = SQLServerDiscovery(config)

        connection_test = discovery.test_connection()
        if not connection_test.get("success"):
            raise ConnectionError(
                f"Cannot connect to SQL Server at {config.hostname}:{config.port}. "
                f"Error: {connection_test.get('message', 'Unknown error')}"
            )

        return discovery.list_schemas()
