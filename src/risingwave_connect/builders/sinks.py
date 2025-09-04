"""Sink builder for various sink types."""

from __future__ import annotations
import logging
import time
from typing import Dict, List, Optional, Any, Union

from .base import BaseSinkBuilder
from ..sinks.base import SinkConfig, SinkResult
from ..sinks.s3 import S3Config, S3Sink
from ..sinks.postgresql import PostgreSQLSinkConfig, PostgreSQLSink
from ..sinks.iceberg import IcebergConfig, IcebergSink

logger = logging.getLogger(__name__)


class SinkBuilder(BaseSinkBuilder):
    """Universal sink builder for all sink types."""

    def create_sink(
        self,
        sink_config: Union[S3Config, PostgreSQLSinkConfig, IcebergConfig],
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create a sink based on the config type."""
        if isinstance(sink_config, S3Config):
            return self._create_s3_sink(sink_config, source_tables, select_queries, dry_run)
        elif isinstance(sink_config, PostgreSQLSinkConfig):
            return self._create_postgresql_sink(sink_config, source_tables, select_queries, dry_run)
        elif isinstance(sink_config, IcebergConfig):
            return self._create_iceberg_sink(sink_config, source_tables, select_queries, dry_run)
        else:
            raise ValueError(
                f"Unsupported sink config type: {type(sink_config)}")

    def _create_s3_sink(
        self,
        s3_config: S3Config,
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create S3 sink."""
        sink = S3Sink(s3_config)

        sink_results = []
        sql_statements = []

        for table_name in source_tables:
            try:
                select_query = select_queries.get(
                    table_name) if select_queries else None

                if dry_run:
                    # Generate SQL without executing
                    sql = sink.create_sink_sql(table_name, select_query)
                    sql_statements.append(sql)
                    sink_results.append(SinkResult(
                        sink_name=f"{s3_config.sink_name}_{table_name}",
                        sink_type=s3_config.sink_type,
                        sql_statement=sql,
                        source_table=table_name,
                        success=True,
                        message="Dry run - SQL generated",
                        execution_time=0.0
                    ))
                else:
                    # Execute sink creation
                    result = sink.create_sink(table_name, select_query)
                    if result.success:
                        # Execute the SQL using rw_client
                        start_time = time.time()
                        try:
                            self.rw_client.execute_sql(result.sql_statement)
                            execution_time = time.time() - start_time
                            result.execution_time = execution_time
                            result.message = "Sink created successfully"
                        except Exception as exec_error:
                            execution_time = time.time() - start_time
                            result.success = False
                            result.error_message = f"SQL execution failed: {str(exec_error)}"
                            result.execution_time = execution_time

                    sink_results.append(result)
                    sql_statements.append(result.sql_statement)

            except Exception as e:
                logger.error(
                    f"Failed to create S3 sink for table {table_name}: {e}")
                sink_results.append(SinkResult(
                    sink_name=f"{s3_config.sink_name}_{table_name}",
                    success=False,
                    sql_statement="",
                    message=f"Failed to create sink: {str(e)}",
                    execution_time=0.0
                ))

        successful_results = [r for r in sink_results if r.success]
        failed_results = [r for r in sink_results if not r.success]

        return {
            "success_summary": {
                "overall_success": len(failed_results) == 0,
                "total_sinks": len(sink_results),
                "successful_sinks": len(successful_results),
                "failed_sinks": len(failed_results)
            },
            "sink_results": successful_results,
            "failed_results": failed_results,
            "sql_statements": sql_statements,
            "dry_run": dry_run,
            "executed": not dry_run
        }

    def _create_postgresql_sink(
        self,
        pg_sink_config: PostgreSQLSinkConfig,
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create PostgreSQL sink."""
        sink = PostgreSQLSink(pg_sink_config)

        sink_results = []
        sql_statements = []

        for table_name in source_tables:
            try:
                select_query = select_queries.get(
                    table_name) if select_queries else None

                if dry_run:
                    # Generate SQL without executing
                    sql = sink.create_sink_sql(table_name, select_query)
                    sql_statements.append(sql)
                    sink_results.append(SinkResult(
                        sink_name=f"{pg_sink_config.sink_name}_{table_name}",
                        sink_type=pg_sink_config.sink_type,
                        sql_statement=sql,
                        source_table=table_name,
                        success=True,
                        message="Dry run - SQL generated",
                        execution_time=0.0
                    ))
                else:
                    # Execute sink creation
                    result = sink.create_sink(table_name, select_query)
                    if result.success:
                        # Execute the SQL using rw_client
                        start_time = time.time()
                        try:
                            self.rw_client.execute_sql(result.sql_statement)
                            execution_time = time.time() - start_time
                            result.execution_time = execution_time
                            result.message = "Sink created successfully"
                        except Exception as exec_error:
                            execution_time = time.time() - start_time
                            result.success = False
                            result.error_message = f"SQL execution failed: {str(exec_error)}"
                            result.execution_time = execution_time

                    sink_results.append(result)
                    sql_statements.append(result.sql_statement)

            except Exception as e:
                logger.error(
                    f"Failed to create PostgreSQL sink for table {table_name}: {e}")
                sink_results.append(SinkResult(
                    sink_name=f"{pg_sink_config.sink_name}_{table_name}",
                    success=False,
                    sql_statement="",
                    message=f"Failed to create sink: {str(e)}",
                    execution_time=0.0
                ))

        successful_results = [r for r in sink_results if r.success]
        failed_results = [r for r in sink_results if not r.success]

        return {
            "success_summary": {
                "overall_success": len(failed_results) == 0,
                "total_sinks": len(sink_results),
                "successful_sinks": len(successful_results),
                "failed_sinks": len(failed_results)
            },
            "sink_results": successful_results,
            "failed_results": failed_results,
            "sql_statements": sql_statements,
            "dry_run": dry_run,
            "executed": not dry_run
        }

    def _create_iceberg_sink(
        self,
        iceberg_config: IcebergConfig,
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create Iceberg sink."""
        sink = IcebergSink(iceberg_config)

        sink_results = []
        sql_statements = []

        for table_name in source_tables:
            try:
                select_query = select_queries.get(
                    table_name) if select_queries else None

                if dry_run:
                    # Generate SQL without executing
                    sql = sink.create_sink_sql(table_name, select_query)
                    sql_statements.append(sql)
                    sink_results.append(SinkResult(
                        sink_name=f"{iceberg_config.sink_name}_{table_name}",
                        sink_type=iceberg_config.sink_type,
                        sql_statement=sql,
                        source_table=table_name,
                        success=True,
                        message="Dry run - SQL generated",
                        execution_time=0.0
                    ))
                else:
                    # Execute sink creation
                    result = sink.create_sink(table_name, select_query)
                    if result.success:
                        # Execute the SQL using rw_client
                        start_time = time.time()
                        try:
                            self.rw_client.execute_sql(result.sql_statement)
                            execution_time = time.time() - start_time
                            result.execution_time = execution_time
                            result.message = "Sink created successfully"
                        except Exception as exec_error:
                            execution_time = time.time() - start_time
                            result.success = False
                            result.error_message = f"SQL execution failed: {str(exec_error)}"
                            result.execution_time = execution_time

                    sink_results.append(result)
                    sql_statements.append(result.sql_statement)

            except Exception as e:
                logger.error(
                    f"Failed to create Iceberg sink for table {table_name}: {e}")
                sink_results.append(SinkResult(
                    sink_name=f"{iceberg_config.sink_name}_{table_name}",
                    success=False,
                    sql_statement="",
                    message=f"Failed to create sink: {str(e)}",
                    execution_time=0.0
                ))

        successful_results = [r for r in sink_results if r.success]
        failed_results = [r for r in sink_results if not r.success]

        return {
            "success_summary": {
                "overall_success": len(failed_results) == 0,
                "total_sinks": len(sink_results),
                "successful_sinks": len(successful_results),
                "failed_sinks": len(failed_results)
            },
            "sink_results": successful_results,
            "failed_results": failed_results,
            "sql_statements": sql_statements,
            "dry_run": dry_run,
            "executed": not dry_run
        }
