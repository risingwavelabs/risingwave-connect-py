"""Advanced pipeline orchestrator for end-to-end data pipelines."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union, Set
from dataclasses import dataclass
from enum import Enum

from .client import RisingWaveClient
from .pipeline_builder import PipelineBuilder
from .sources.postgresql import PostgreSQLConfig
from .sinks.base import SinkConfig
from .sinks.s3 import S3Config
from .sinks.postgresql import PostgreSQLSinkConfig
from .sinks.iceberg import IcebergConfig
from .discovery.base import TableSelector, TableInfo

logger = logging.getLogger(__name__)


class PipelineType(Enum):
    """Types of supported pipelines."""
    CDC_TO_ICEBERG = "cdc_to_iceberg"
    CDC_TO_S3 = "cdc_to_s3"
    CDC_TO_POSTGRES = "cdc_to_postgres"
    MULTI_SINK = "multi_sink"


@dataclass
class TableMapping:
    """Configuration for table-to-sink mapping."""
    source_table: str
    sink_table: Optional[str] = None  # If None, uses source_table name
    custom_query: Optional[str] = None
    transformation: Optional[str] = None


@dataclass
class PipelineExecutionResult:
    """Result of pipeline execution."""
    pipeline_id: str
    pipeline_type: PipelineType
    source_info: Dict[str, Any]
    sink_results: Dict[str, Any]
    sql_statements: List[str]
    executed: bool
    errors: List[str] = None


class PipelineOrchestrator:
    """Advanced pipeline orchestrator for building complete data pipelines."""

    def __init__(self, rw_client: RisingWaveClient):
        self.rw_client = rw_client
        self.builder = PipelineBuilder(rw_client)
        self._pipelines: Dict[str, PipelineExecutionResult] = {}

    def create_cdc_to_iceberg_pipeline(
        self,
        pipeline_id: str,
        postgres_config: PostgreSQLConfig,
        iceberg_config: IcebergConfig,
        table_mappings: Optional[List[TableMapping]] = None,
        table_names: Optional[List[str]] = None,
        table_selector: Optional[TableSelector] = None,
        dry_run: bool = False
    ) -> PipelineExecutionResult:
        """Create a complete CDC to Iceberg pipeline.

        Args:
            pipeline_id: Unique identifier for this pipeline
            postgres_config: PostgreSQL CDC source configuration
            iceberg_config: Iceberg sink configuration (will be customized per table)
            table_mappings: Detailed table mapping configurations
            table_names: Simple list of table names (alternative to table_mappings)
            table_selector: Table selection criteria (used if neither mappings nor names provided)
            dry_run: If True, return SQL without executing

        Returns:
            PipelineExecutionResult with execution details
        """
        logger.info(f"Creating CDC to Iceberg pipeline: {pipeline_id}")

        errors = []
        all_sql_statements = []

        try:
            # Step 1: Create CDC source and tables
            source_name = f"{pipeline_id}_cdc_source"

            # Determine which tables to process
            if table_mappings:
                table_names = [
                    mapping.source_table for mapping in table_mappings]

            cdc_result = self.builder.create_cdc_pipeline(
                source_name=source_name,
                config=postgres_config,
                table_names=table_names,
                table_selector=table_selector,
                dry_run=dry_run
            )

            all_sql_statements.extend(cdc_result['sql_statements'])
            selected_tables = cdc_result['selected_tables']

            # Step 2: Create Iceberg sinks for each table
            sink_results = {}

            # Create mappings if not provided
            if not table_mappings:
                if table_names:
                    table_mappings = [TableMapping(
                        source_table=name) for name in table_names]
                else:
                    table_mappings = [TableMapping(source_table=table.table_name)
                                      for table in selected_tables]

            # Create Iceberg sink for each table mapping
            for mapping in table_mappings:
                sink_table_name = mapping.sink_table or mapping.source_table
                sink_name = f"{pipeline_id}_{sink_table_name}_iceberg_sink"

                # Create customized Iceberg config for this table
                table_iceberg_config = IcebergConfig(**iceberg_config.dict())
                table_iceberg_config.sink_name = sink_name
                table_iceberg_config.table_name = sink_table_name

                try:
                    # Determine source table name (including any transformations)
                    source_table = mapping.source_table
                    custom_query = mapping.custom_query

                    sink_result = self.builder.create_sink(
                        sink_config=table_iceberg_config,
                        source_tables=[source_table],
                        select_queries={
                            source_table: custom_query} if custom_query else None,
                        dry_run=dry_run
                    )

                    sink_results[mapping.source_table] = sink_result
                    all_sql_statements.extend(sink_result['sql_statements'])

                except Exception as e:
                    error_msg = f"Failed to create Iceberg sink for {mapping.source_table}: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            # Create pipeline result
            result = PipelineExecutionResult(
                pipeline_id=pipeline_id,
                pipeline_type=PipelineType.CDC_TO_ICEBERG,
                source_info=cdc_result,
                sink_results=sink_results,
                sql_statements=all_sql_statements,
                executed=not dry_run,
                errors=errors if errors else None
            )

            self._pipelines[pipeline_id] = result
            return result

        except Exception as e:
            error_msg = f"Failed to create CDC to Iceberg pipeline {pipeline_id}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)

            result = PipelineExecutionResult(
                pipeline_id=pipeline_id,
                pipeline_type=PipelineType.CDC_TO_ICEBERG,
                source_info={},
                sink_results={},
                sql_statements=all_sql_statements,
                executed=False,
                errors=errors
            )

            self._pipelines[pipeline_id] = result
            return result

    def create_multi_table_iceberg_pipeline(
        self,
        pipeline_id: str,
        postgres_config: PostgreSQLConfig,
        iceberg_base_config: IcebergConfig,
        table_configs: Dict[str, Dict[str, Any]],
        dry_run: bool = False
    ) -> PipelineExecutionResult:
        """Create a pipeline that sinks multiple tables to Iceberg with individual configurations.

        Args:
            pipeline_id: Unique identifier for this pipeline
            postgres_config: PostgreSQL CDC source configuration
            iceberg_base_config: Base Iceberg configuration
            table_configs: Per-table configuration overrides
                {
                    "table_name": {
                        "iceberg_table_name": "custom_name",
                        "primary_key": "id",
                        "data_type": "upsert",
                        "custom_query": "SELECT * FROM table WHERE active=true",
                        "warehouse_path": "s3://custom-bucket/path"
                    }
                }
            dry_run: If True, return SQL without executing

        Returns:
            PipelineExecutionResult with execution details
        """
        logger.info(f"Creating multi-table Iceberg pipeline: {pipeline_id}")

        errors = []
        all_sql_statements = []

        try:
            # Step 1: Create CDC source
            source_name = f"{pipeline_id}_cdc_source"
            table_names = list(table_configs.keys())

            cdc_result = self.builder.create_cdc_pipeline(
                source_name=source_name,
                config=postgres_config,
                table_names=table_names,
                dry_run=dry_run
            )

            all_sql_statements.extend(cdc_result['sql_statements'])

            # Step 2: Create customized Iceberg sinks
            sink_results = {}

            for table_name, config_overrides in table_configs.items():
                sink_name = f"{pipeline_id}_{table_name}_iceberg_sink"

                # Create customized Iceberg config
                table_iceberg_config = IcebergConfig(
                    **iceberg_base_config.dict())
                table_iceberg_config.sink_name = sink_name

                # Apply table-specific overrides
                for key, value in config_overrides.items():
                    if key == "iceberg_table_name":
                        table_iceberg_config.table_name = value
                    elif key == "custom_query":
                        continue  # Handle separately
                    elif hasattr(table_iceberg_config, key):
                        setattr(table_iceberg_config, key, value)

                # Set default table name if not specified
                if not hasattr(table_iceberg_config, 'table_name') or not table_iceberg_config.table_name:
                    table_iceberg_config.table_name = table_name

                try:
                    custom_query = config_overrides.get("custom_query")

                    sink_result = self.builder.create_sink(
                        sink_config=table_iceberg_config,
                        source_tables=[table_name],
                        select_queries={
                            table_name: custom_query} if custom_query else None,
                        dry_run=dry_run
                    )

                    sink_results[table_name] = sink_result
                    all_sql_statements.extend(sink_result['sql_statements'])

                except Exception as e:
                    error_msg = f"Failed to create Iceberg sink for {table_name}: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            # Create pipeline result
            result = PipelineExecutionResult(
                pipeline_id=pipeline_id,
                pipeline_type=PipelineType.CDC_TO_ICEBERG,
                source_info=cdc_result,
                sink_results=sink_results,
                sql_statements=all_sql_statements,
                executed=not dry_run,
                errors=errors if errors else None
            )

            self._pipelines[pipeline_id] = result
            return result

        except Exception as e:
            error_msg = f"Failed to create multi-table Iceberg pipeline {pipeline_id}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)

            result = PipelineExecutionResult(
                pipeline_id=pipeline_id,
                pipeline_type=PipelineType.CDC_TO_ICEBERG,
                source_info={},
                sink_results={},
                sql_statements=all_sql_statements,
                executed=False,
                errors=errors
            )

            self._pipelines[pipeline_id] = result
            return result

    def create_multi_sink_pipeline(
        self,
        pipeline_id: str,
        postgres_config: PostgreSQLConfig,
        sink_configs: List[Union[IcebergConfig, S3Config, PostgreSQLSinkConfig]],
        table_names: Optional[List[str]] = None,
        table_selector: Optional[TableSelector] = None,
        dry_run: bool = False
    ) -> PipelineExecutionResult:
        """Create a pipeline that sends data to multiple different sink types.

        Args:
            pipeline_id: Unique identifier for this pipeline
            postgres_config: PostgreSQL CDC source configuration
            sink_configs: List of different sink configurations
            table_names: Specific table names to include
            table_selector: Table selection criteria
            dry_run: If True, return SQL without executing

        Returns:
            PipelineExecutionResult with execution details
        """
        logger.info(f"Creating multi-sink pipeline: {pipeline_id}")

        errors = []
        all_sql_statements = []

        try:
            # Step 1: Create CDC source
            source_name = f"{pipeline_id}_cdc_source"

            cdc_result = self.builder.create_cdc_pipeline(
                source_name=source_name,
                config=postgres_config,
                table_names=table_names,
                table_selector=table_selector,
                dry_run=dry_run
            )

            all_sql_statements.extend(cdc_result['sql_statements'])
            selected_tables = cdc_result['selected_tables']

            # Get table names from selected tables
            if not table_names:
                table_names = [table.table_name for table in selected_tables]

            # Step 2: Create sinks for each sink config
            sink_results = {}

            for i, sink_config in enumerate(sink_configs):
                sink_type = sink_config.sink_type
                sink_key = f"{sink_type}_{i}"

                try:
                    # Customize sink name to include pipeline ID
                    customized_config = type(sink_config)(**sink_config.dict())
                    customized_config.sink_name = f"{pipeline_id}_{sink_config.sink_name}"

                    sink_result = self.builder.create_sink(
                        sink_config=customized_config,
                        source_tables=table_names,
                        dry_run=dry_run
                    )

                    sink_results[sink_key] = sink_result
                    all_sql_statements.extend(sink_result['sql_statements'])

                except Exception as e:
                    error_msg = f"Failed to create {sink_type} sink: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            # Create pipeline result
            result = PipelineExecutionResult(
                pipeline_id=pipeline_id,
                pipeline_type=PipelineType.MULTI_SINK,
                source_info=cdc_result,
                sink_results=sink_results,
                sql_statements=all_sql_statements,
                executed=not dry_run,
                errors=errors if errors else None
            )

            self._pipelines[pipeline_id] = result
            return result

        except Exception as e:
            error_msg = f"Failed to create multi-sink pipeline {pipeline_id}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)

            result = PipelineExecutionResult(
                pipeline_id=pipeline_id,
                pipeline_type=PipelineType.MULTI_SINK,
                source_info={},
                sink_results={},
                sql_statements=all_sql_statements,
                executed=False,
                errors=errors
            )

            self._pipelines[pipeline_id] = result
            return result

    def get_pipeline_status(self, pipeline_id: str) -> Optional[PipelineExecutionResult]:
        """Get status of a created pipeline."""
        return self._pipelines.get(pipeline_id)

    def list_pipelines(self) -> Dict[str, PipelineExecutionResult]:
        """List all created pipelines."""
        return self._pipelines.copy()

    def generate_pipeline_summary(self, pipeline_id: str) -> Optional[str]:
        """Generate a human-readable summary of the pipeline."""
        result = self.get_pipeline_status(pipeline_id)
        if not result:
            return None

        summary = [
            f"Pipeline: {pipeline_id}",
            f"Type: {result.pipeline_type.value}",
            f"Executed: {result.executed}",
            f"Tables processed: {len(result.source_info.get('selected_tables', []))}",
            f"Sinks created: {len(result.sink_results)}",
            f"SQL statements: {len(result.sql_statements)}"
        ]

        if result.errors:
            summary.append(f"Errors: {len(result.errors)}")
            for error in result.errors:
                summary.append(f"  - {error}")

        return "\n".join(summary)


# Template functions for common patterns
def create_simple_cdc_to_iceberg(
    rw_client: RisingWaveClient,
    pipeline_id: str,
    postgres_config: PostgreSQLConfig,
    iceberg_config: IcebergConfig,
    table_names: List[str],
    dry_run: bool = False
) -> PipelineExecutionResult:
    """Template for simple CDC to Iceberg pipeline.

    This is a convenience function that creates a basic CDC to Iceberg pipeline
    where each source table maps to an Iceberg table with the same name.
    """
    orchestrator = PipelineOrchestrator(rw_client)
    return orchestrator.create_cdc_to_iceberg_pipeline(
        pipeline_id=pipeline_id,
        postgres_config=postgres_config,
        iceberg_config=iceberg_config,
        table_names=table_names,
        dry_run=dry_run
    )


def create_analytics_pipeline(
    rw_client: RisingWaveClient,
    pipeline_id: str,
    postgres_config: PostgreSQLConfig,
    iceberg_config: IcebergConfig,
    analytics_queries: Dict[str, str],
    dry_run: bool = False
) -> PipelineExecutionResult:
    """Template for analytics pipeline with custom transformations.

    Args:
        analytics_queries: Dict mapping source table names to custom SELECT queries
    """
    orchestrator = PipelineOrchestrator(rw_client)

    # Create table mappings with custom queries
    table_mappings = [
        TableMapping(source_table=table, custom_query=query)
        for table, query in analytics_queries.items()
    ]

    return orchestrator.create_cdc_to_iceberg_pipeline(
        pipeline_id=pipeline_id,
        postgres_config=postgres_config,
        iceberg_config=iceberg_config,
        table_mappings=table_mappings,
        dry_run=dry_run
    )
