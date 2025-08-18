"""Pipeline builder with table discovery and selection."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union

from .client import RisingWaveClient
from .discovery.base import TableSelector, TableInfo
from .sources.postgresql import PostgreSQLConfig, PostgreSQLDiscovery, PostgreSQLPipeline
from .sinks.base import SinkConfig, SinkResult
from .sinks.s3 import S3Config, S3Sink
from .sinks.postgresql import PostgreSQLSinkConfig, PostgreSQLSink

logger = logging.getLogger(__name__)


class PipelineBuilder:
    """High-level pipeline builder with table discovery and selection."""

    def __init__(self, rw_client: RisingWaveClient):
        self.rw_client = rw_client

    def create_postgresql_pipeline(
        self,
        config: PostgreSQLConfig,
        table_selector: Optional[TableSelector] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create a complete PostgreSQL CDC pipeline with table discovery.

        Args:
            config: PostgreSQL configuration
            table_selector: Table selection criteria
            dry_run: If True, return SQL without executing

        Returns:
            Dictionary with pipeline creation results
        """
        # Initialize discovery and pipeline
        discovery = PostgreSQLDiscovery(config)
        pipeline = PostgreSQLPipeline(self.rw_client, config)

        # Test connection
        if not discovery.test_connection():
            raise ConnectionError(
                f"Cannot connect to PostgreSQL at {config.hostname}:{config.port}")

        # Discover tables
        logger.info(f"Discovering tables in schema '{config.schema_name}'...")
        available_tables = discovery.list_tables(config.schema_name)
        logger.info(f"Found {len(available_tables)} tables")

        # Select tables
        if table_selector is None:
            table_selector = TableSelector(include_all=True)

        selected_tables = table_selector.select_tables(available_tables)
        logger.info(f"Selected {len(selected_tables)} tables for CDC")

        # Generate SQL
        sql_statements = pipeline.create_pipeline_sql(selected_tables)

        if dry_run:
            return {
                "available_tables": available_tables,
                "selected_tables": selected_tables,
                "sql_statements": sql_statements,
                "executed": False
            }

        # Execute SQL statements
        executed_statements = []
        try:
            for sql in sql_statements:
                logger.info(f"Executing SQL: {sql[:100]}...")
                self.rw_client.execute(sql)
                executed_statements.append(sql)

            return {
                "available_tables": available_tables,
                "selected_tables": selected_tables,
                "sql_statements": sql_statements,
                "executed_statements": executed_statements,
                "executed": True
            }

        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            raise

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
        sink_config: Union[S3Config, PostgreSQLSinkConfig],
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create sinks for the specified source tables.

        Args:
            sink_config: Sink configuration (S3 or PostgreSQL)
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
                else:
                    table_config = PostgreSQLSinkConfig(**sink_config.dict())
                table_config.sink_name = sink_name

                # Create new sink instance with updated config
                if isinstance(table_config, S3Config):
                    table_sink = S3Sink(table_config)
                else:
                    table_sink = PostgreSQLSink(table_config)
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

        try:
            for result, sql in zip(results, sql_statements):
                if result.success:
                    logger.info(f"Creating sink {result.sink_name}...")
                    self.rw_client.execute(sql)
                    executed_statements.append(sql)
                    executed_results.append(result)

            return {
                "sink_results": executed_results,
                "sql_statements": sql_statements,
                "executed_statements": executed_statements,
                "executed": True
            }

        except Exception as e:
            logger.error(f"Failed to execute sink SQL: {e}")
            raise

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
    builder = PipelineBuilder(rw_client)

    # Create table selector
    if include_tables:
        selector = TableSelector(specific_tables=include_tables)
    elif include_all_tables:
        selector = TableSelector(
            include_all=True, exclude_patterns=exclude_tables or [])
    else:
        selector = TableSelector(
            include_patterns=["*"], exclude_patterns=exclude_tables or [])

    return builder.create_postgresql_pipeline(pg_config, selector, dry_run)
