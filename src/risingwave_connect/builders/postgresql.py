"""PostgreSQL source builder."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union

from .base import BaseSourceBuilder
from ..discovery.base import TableSelector, TableInfo, TableColumnConfig
from ..sources.postgresql import PostgreSQLConfig, PostgreSQLDiscovery, PostgreSQLSourceConnection

logger = logging.getLogger(__name__)


class PostgreSQLBuilder(BaseSourceBuilder):
    """PostgreSQL CDC source builder."""

    def create_connection(
        self,
        config: PostgreSQLConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        column_configs: Optional[Dict[str, TableColumnConfig]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create a complete PostgreSQL CDC connection with table discovery."""
        # Initialize discovery and connection
        discovery = PostgreSQLDiscovery(config)
        pg_source = PostgreSQLSourceConnection(self.rw_client, config)

        # Set dry_run mode on pipeline for column validation
        pg_source._dry_run_mode = dry_run

        # Test connection (skip in dry run mode)
        if not dry_run:
            connection_test = discovery.test_connection()
            if not connection_test.get("success"):
                raise ConnectionError(
                    f"Cannot connect to PostgreSQL at {config.hostname}:{config.port}. "
                    f"Error: {connection_test.get('message', 'Unknown error')}"
                )

        # Validate and convert table_selector
        table_selector = self._validate_table_selector(table_selector, dry_run)

        # Get available tables
        available_tables = self._get_available_tables(
            discovery, config, table_selector, dry_run)

        logger.info(f"Found {len(available_tables)} tables")

        # Select tables: if table_selector is not specified, include all source tables
        if table_selector is None:
            table_selector = TableSelector(include_all=True)

        selected_tables = table_selector.select_tables(available_tables)
        logger.info(f"Selected {len(selected_tables)} tables for CDC")

        # Generate SQL
        sql_statements = []

        # Add source creation
        sql_statements.append(pg_source.create_source_sql())

        # Add table creations with column configurations
        for table in selected_tables:
            table_key = table.qualified_name
            column_config = None

            # Check if we have column configuration for this table
            if column_configs:
                # Try exact qualified name first, then just table name
                column_config = column_configs.get(
                    table_key) or column_configs.get(table.table_name)

            # Validate column config if provided
            if column_config and not dry_run:
                validation_result = discovery.validate_column_selection(
                    table, column_config.column_selections
                )
                if not validation_result['valid']:
                    logger.error(
                        f"Column validation failed for {table.qualified_name}: {validation_result['errors']}")
                    continue

            # Generate table SQL
            table_sql = pg_source.create_table_sql(
                table, column_config=column_config)
            sql_statements.append(table_sql)

        # Execute SQL statements if not dry run
        execution_results = []
        if not dry_run and sql_statements:
            try:
                # Execute source creation
                source_result = self.rw_client.execute_sql(sql_statements[0])
                execution_results.append({
                    "sql": sql_statements[0],
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
                    "sql": sql_statements[0],
                    "success": False,
                    "message": f"Failed to create source: {str(e)}"
                })

        return {
            "success_summary": {
                "overall_success": True,
                "total_tables": len(selected_tables),
                "successful_tables": len(selected_tables),
                "failed_tables": 0
            },
            "selected_tables": selected_tables,
            "sql_statements": sql_statements,
            "execution_results": execution_results if not dry_run else [],
            "dry_run": dry_run,
            "executed": not dry_run
        }

    def discover_tables(
        self,
        config: PostgreSQLConfig,
        schema_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available tables in PostgreSQL database."""
        discovery = PostgreSQLDiscovery(config)

        connection_test = discovery.test_connection()
        if not connection_test.get("success"):
            raise ConnectionError(
                f"Cannot connect to PostgreSQL at {config.hostname}:{config.port}. "
                f"Error: {connection_test.get('message', 'Unknown error')}"
            )

        return discovery.list_tables(schema_name or config.schema_name)

    def get_schemas(self, config: PostgreSQLConfig) -> List[str]:
        """Get list of available schemas in PostgreSQL database."""
        discovery = PostgreSQLDiscovery(config)

        connection_test = discovery.test_connection()
        if not connection_test.get("success"):
            raise ConnectionError(
                f"Cannot connect to PostgreSQL at {config.hostname}:{config.port}. "
                f"Error: {connection_test.get('message', 'Unknown error')}"
            )

        return discovery.list_schemas()

    def _get_available_tables(
        self,
        discovery: PostgreSQLDiscovery,
        config: PostgreSQLConfig,
        table_selector: Optional[TableSelector],
        dry_run: bool
    ) -> List[TableInfo]:
        """Get available tables based on selection criteria."""
        # Check if user provided specific tables
        if table_selector and isinstance(table_selector, TableSelector) and table_selector.specific_tables:
            # User provided specific tables - only check if those tables exist (skip in dry_run mode)
            if not dry_run:
                logger.info(
                    f"Checking existence of {len(table_selector.specific_tables)} specific tables...")
                available_tables = discovery.check_specific_tables(
                    table_selector.specific_tables, config.schema_name)

                # Validate that all requested tables exist
                existing_table_names = {
                    table.qualified_name for table in available_tables}
                existing_table_names.update(
                    {table.table_name for table in available_tables})

                missing_tables = []
                for table_name in table_selector.specific_tables:
                    if table_name not in existing_table_names:
                        missing_tables.append(table_name)

                if missing_tables:
                    raise ValueError(
                        f"The following tables do not exist in schema '{config.schema_name}': {missing_tables}. "
                        f"Please verify the table names and ensure they exist before setting up CDC."
                    )
            else:
                # In dry_run mode, create mock tables for demonstration
                available_tables = self._create_placeholder_tables(
                    table_selector.specific_tables, config.schema_name)

        elif isinstance(table_selector, list):
            # table_selector is a list of table names - only check those specific tables (skip in dry_run mode)
            if not dry_run:
                logger.info(
                    f"Checking existence of {len(table_selector)} specific tables...")
                available_tables = discovery.check_specific_tables(
                    table_selector, config.schema_name)

                # Validate that all requested tables exist
                existing_table_names = {
                    table.qualified_name for table in available_tables}
                existing_table_names.update(
                    {table.table_name for table in available_tables})

                missing_tables = []
                for table_name in table_selector:
                    if table_name not in existing_table_names:
                        missing_tables.append(table_name)

                if missing_tables:
                    raise ValueError(
                        f"The following tables do not exist in schema '{config.schema_name}': {missing_tables}. "
                        f"Please verify the table names and ensure they exist before setting up CDC."
                    )
            else:
                # In dry_run mode, create mock tables for demonstration
                available_tables = self._create_placeholder_tables(
                    table_selector, config.schema_name)
        else:
            # No specific tables provided - discover all tables in schema (skip in dry_run mode)
            if not dry_run:
                logger.info(
                    f"Discovering all tables in schema '{config.schema_name}'...")
                available_tables = discovery.list_tables(config.schema_name)
            else:
                # In dry_run mode, create mock tables for demonstration
                available_tables = [
                    TableInfo(schema_name=config.schema_name,
                              table_name="mock_table_1", table_type="BASE TABLE"),
                    TableInfo(schema_name=config.schema_name,
                              table_name="mock_table_2", table_type="BASE TABLE")
                ]

        return available_tables
