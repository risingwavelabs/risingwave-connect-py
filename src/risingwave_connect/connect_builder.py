"""Connection builder with table discovery and selection."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union

from .client import RisingWaveClient
from .discovery.base import TableSelector, TableInfo, TableColumnConfig
from .sources.postgresql import PostgreSQLConfig, PostgreSQLDiscovery, PostgreSQLPipeline
from .sinks.base import SinkConfig, SinkResult
from .sinks.s3 import S3Config, S3Sink
from .sinks.postgresql import PostgreSQLSinkConfig, PostgreSQLSink
from .sinks.iceberg import IcebergConfig, IcebergSink

logger = logging.getLogger(__name__)


class ConnectBuilder:
    """High-level connection builder with table discovery and selection."""

    def __init__(self, rw_client: RisingWaveClient):
        self.rw_client = rw_client

    def create_postgresql_connection(
        self,
        config: PostgreSQLConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        column_configs: Optional[Dict[str, TableColumnConfig]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create a complete PostgreSQL CDC connection with table discovery.

        Args:
            config: PostgreSQL configuration
            table_selector: Table selection criteria. Can be a TableSelector object or a list of table names.
            column_configs: Optional dictionary mapping table names to TableColumnConfig for column-level filtering
            dry_run: If True, return SQL without executing

        Returns:
            Dictionary with connection creation results
        """
        # Initialize discovery and connection
        discovery = PostgreSQLDiscovery(config)
        pipeline = PostgreSQLPipeline(self.rw_client, config)

        # Set dry_run mode on pipeline for column validation
        pipeline._dry_run_mode = dry_run

        # Test connection (skip in dry run mode)
        if not dry_run and not discovery.test_connection():
            raise ConnectionError(
                f"Cannot connect to PostgreSQL at {config.hostname}:{config.port}")

        # Discover tables (mock in dry run mode)
        if dry_run:
            logger.info("Dry run mode: skipping actual table discovery")
            # Provide mock tables for demonstration
            available_tables = [
                TableInfo(
                    schema_name=config.schema_name,
                    table_name="user_events",
                    table_type="BASE TABLE"
                ),
                TableInfo(
                    schema_name=config.schema_name,
                    table_name="users",
                    table_type="BASE TABLE"
                )
            ]
        else:
            # Check if user provided specific tables
            if table_selector and isinstance(table_selector, TableSelector) and table_selector.specific_tables:
                # User provided specific tables - only check if those tables exist (more efficient)
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

            elif isinstance(table_selector, list):
                # table_selector is a list of table names - only check those specific tables
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
                # No specific tables provided - discover all tables in schema (slower)
                logger.info(
                    f"Discovering all tables in schema '{config.schema_name}'...")
                available_tables = discovery.list_tables(config.schema_name)

        logger.info(f"Found {len(available_tables)} tables")

        # Select tables: if table_selector is not specified, include all source tables
        if table_selector is None:
            table_selector = TableSelector(include_all=True)
        elif isinstance(table_selector, list):
            # Convert list of table names to TableSelector
            table_selector = TableSelector(specific_tables=table_selector)

        selected_tables = table_selector.select_tables(available_tables)
        logger.info(f"Selected {len(selected_tables)} tables for CDC")

        # Generate SQL
        sql_statements = []

        # Add source creation
        sql_statements.append(pipeline.create_source_sql())

        # Add table creations with column configurations
        for table in selected_tables:
            table_key = table.qualified_name
            column_config = None

            # Check if we have column configuration for this table
            if column_configs:
                # Try exact qualified name first, then just table name
                column_config = column_configs.get(
                    table_key) or column_configs.get(table.table_name)

            table_sql = pipeline.create_table_sql(
                table, column_config=column_config)
            sql_statements.append(table_sql)

        if dry_run:
            return {
                "available_tables": available_tables,
                "selected_tables": selected_tables,
                "sql_statements": sql_statements,
                "executed": False
            }

        # Execute SQL statements
        executed_statements = []
        failed_statements = []
        success_messages = []

        for sql in sql_statements:
            try:
                logger.info(f"Executing SQL: {sql[:100]}...")
                self.rw_client.execute(sql)
                executed_statements.append(sql)

                # Generate success message based on SQL type
                if "CREATE SOURCE" in sql.upper():
                    success_messages.append(
                        "✅ CDC source created successfully")
                elif "CREATE TABLE" in sql.upper():
                    # Extract table name from SQL
                    table_name = sql.split("CREATE TABLE IF NOT EXISTS ")[
                        1].split(" ")[0]
                    success_messages.append(
                        f"✅ CDC table '{table_name}' created successfully")
                else:
                    success_messages.append(
                        "✅ SQL statement executed successfully")

            except Exception as e:
                logger.warning(f"Failed to execute SQL statement: {e}")
                failed_statements.append({"sql": sql, "error": str(e)})
                # Continue with next statement instead of stopping

        # Calculate success summary
        total_statements = len(sql_statements)
        successful_statements = len(executed_statements)
        failed_count = len(failed_statements)

        success_summary = {
            "total_statements": total_statements,
            "successful_statements": successful_statements,
            "failed_statements": failed_count,
            "success_rate": f"{successful_statements}/{total_statements}",
            "overall_success": failed_count == 0
        }

        return {
            "available_tables": available_tables,
            "selected_tables": selected_tables,
            "sql_statements": sql_statements,
            "executed_statements": executed_statements,
            "failed_statements": failed_statements,
            "success_messages": success_messages,
            "success_summary": success_summary,
            "executed": True,
            "partial_success": len(failed_statements) > 0
        }

    def discover_postgresql_tables(
        self,
        config: PostgreSQLConfig,
        schema_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available tables in PostgreSQL database.

        Args:
            config: PostgreSQL configuration
            schema_name: Optional specific schema to query

        Returns:
            List of discovered tables
        """
        discovery = PostgreSQLDiscovery(config)

        if not discovery.test_connection():
            raise ConnectionError(
                f"Cannot connect to PostgreSQL at {config.hostname}:{config.port}")

        schema = schema_name or config.schema_name
        return discovery.list_tables(schema)

    def get_postgresql_schemas(self, config: PostgreSQLConfig) -> List[str]:
        """Get list of available schemas in PostgreSQL database.

        Args:
            config: PostgreSQL configuration

        Returns:
            List of schema names
        """
        discovery = PostgreSQLDiscovery(config)

        if not discovery.test_connection():
            raise ConnectionError(
                f"Cannot connect to PostgreSQL at {config.hostname}:{config.port}")

        return discovery.list_schemas()

    def create_sink(
        self,
        sink_config: Union[S3Config, PostgreSQLSinkConfig, IcebergConfig],
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create sinks for the specified source tables.

        Args:
            sink_config: Sink configuration (S3, PostgreSQL, or Iceberg)
            source_tables: List of source table names to create sinks for
            select_queries: Optional custom SELECT queries per table
            dry_run: If True, return SQL without executing

        Returns:
            Dictionary with sink creation results
        """
        # Create appropriate sink instance
        if isinstance(sink_config, S3Config):
            sink = S3Sink(sink_config)
        elif isinstance(sink_config, PostgreSQLSinkConfig):
            sink = PostgreSQLSink(sink_config)
        elif isinstance(sink_config, IcebergConfig):
            sink = IcebergSink(sink_config)
        else:
            raise ValueError(
                f"Unsupported sink config type: {type(sink_config)}")

        results = []
        sql_statements = []

        # Create sink for each source table
        for i, source_table in enumerate(source_tables):
            # Generate unique sink name if multiple tables
            if len(source_tables) > 1:
                sink_name = f"{sink_config.sink_name}_{source_table.replace('.', '_')}"
                # Update config with unique name
                if isinstance(sink_config, S3Config):
                    table_config = S3Config(**sink_config.dict())
                elif isinstance(sink_config, PostgreSQLSinkConfig):
                    table_config = PostgreSQLSinkConfig(**sink_config.dict())
                else:  # IcebergConfig
                    table_config = IcebergConfig(**sink_config.dict())
                    # For Iceberg, also update the table name to be unique
                    table_config.table_name = f"{sink_config.table_name}_{source_table.replace('.', '_')}"

                table_config.sink_name = sink_name

                # Create new sink instance with updated config
                if isinstance(table_config, S3Config):
                    table_sink = S3Sink(table_config)
                elif isinstance(table_config, PostgreSQLSinkConfig):
                    table_sink = PostgreSQLSink(table_config)
                else:  # IcebergConfig
                    table_sink = IcebergSink(table_config)
            else:
                table_sink = sink
                table_config = sink_config

            # Get custom query for this table if provided
            custom_query = select_queries.get(
                source_table) if select_queries else None

            # Create sink SQL
            try:
                sql = table_sink.create_sink_sql(source_table, custom_query)
                sql_statements.append(sql)

                result = SinkResult(
                    sink_name=table_config.sink_name,
                    sink_type=table_config.sink_type,
                    sql_statement=sql,
                    source_table=source_table,
                    success=True
                )
                results.append(result)

            except Exception as e:
                logger.error(
                    f"Failed to create sink SQL for {source_table}: {e}")
                result = SinkResult(
                    sink_name=table_config.sink_name,
                    sink_type=table_config.sink_type,
                    sql_statement="",
                    source_table=source_table,
                    success=False,
                    error_message=str(e)
                )
                results.append(result)

        if dry_run:
            return {
                "sink_results": results,
                "sql_statements": sql_statements,
                "executed": False
            }

        # Execute SQL statements
        executed_statements = []
        executed_results = []
        success_messages = []
        failed_results = []

        for result, sql in zip(results, sql_statements):
            if result.success:
                try:
                    logger.info(f"Creating sink {result.sink_name}...")
                    self.rw_client.execute(sql)
                    executed_statements.append(sql)
                    executed_results.append(result)
                    success_messages.append(
                        f"✅ Sink '{result.sink_name}' created successfully for table '{result.source_table}'")
                except Exception as e:
                    logger.error(
                        f"Failed to execute sink SQL for {result.sink_name}: {e}")
                    failed_result = SinkResult(
                        sink_name=result.sink_name,
                        sink_type=result.sink_type,
                        sql_statement=result.sql_statement,
                        source_table=result.source_table,
                        success=False,
                        error_message=str(e)
                    )
                    failed_results.append(failed_result)
            else:
                failed_results.append(result)

        # Calculate success summary
        total_sinks = len(results)
        successful_sinks = len(executed_results)
        failed_sinks = len(failed_results)

        success_summary = {
            "total_sinks": total_sinks,
            "successful_sinks": successful_sinks,
            "failed_sinks": failed_sinks,
            "success_rate": f"{successful_sinks}/{total_sinks}",
            "overall_success": failed_sinks == 0
        }

        return {
            "sink_results": executed_results,
            "failed_results": failed_results,
            "sql_statements": sql_statements,
            "executed_statements": executed_statements,
            "success_messages": success_messages,
            "success_summary": success_summary,
            "executed": True
        }

    def create_s3_sink(
        self,
        s3_config: S3Config,
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create S3 sinks for the specified source tables.

        Args:
            s3_config: S3 sink configuration
            source_tables: List of source table names
            select_queries: Optional custom SELECT queries per table
            dry_run: If True, return SQL without executing

        Returns:
            Dictionary with S3 sink creation results
        """
        return self.create_sink(s3_config, source_tables, select_queries, dry_run)

    def create_postgresql_sink(
        self,
        pg_sink_config: PostgreSQLSinkConfig,
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create PostgreSQL sinks for the specified source tables.

        Args:
            pg_sink_config: PostgreSQL sink configuration
            source_tables: List of source table names
            select_queries: Optional custom SELECT queries per table
            dry_run: If True, return SQL without executing

        Returns:
            Dictionary with PostgreSQL sink creation results
        """
        return self.create_sink(pg_sink_config, source_tables, select_queries, dry_run)


# Convenience functions for backward compatibility
def create_postgresql_cdc_pipeline(
    rw_client: RisingWaveClient,
    pg_config: PostgreSQLConfig,
    include_all_tables: bool = False,
    include_tables: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Convenience function to create PostgreSQL CDC pipeline.

    Args:
        rw_client: RisingWave client
        pg_config: PostgreSQL configuration
        include_all_tables: Whether to include all discovered tables
        include_tables: Specific tables to include
        exclude_tables: Tables to exclude
        dry_run: If True, return SQL without executing

    Returns:
        Pipeline creation results
    """
    builder = ConnectBuilder(rw_client)

    # Create table selector
    if include_tables:
        selector = TableSelector(specific_tables=include_tables)
    elif include_all_tables:
        selector = TableSelector(
            include_all=True, exclude_patterns=exclude_tables or [])
    else:
        selector = TableSelector(
            include_patterns=["*"], exclude_patterns=exclude_tables or [])

    return builder.create_postgresql_connection(pg_config, selector, dry_run)
