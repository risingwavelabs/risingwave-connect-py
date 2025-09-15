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
from ..sinks.elasticsearch import ElasticsearchConfig, ElasticsearchSink

logger = logging.getLogger(__name__)


class SinkBuilder(BaseSinkBuilder):
    """Universal sink builder for all sink types."""

    def create_sink(
        self,
        sink_config: Union[S3Config, PostgreSQLSinkConfig, IcebergConfig, ElasticsearchConfig],
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
        elif isinstance(sink_config, ElasticsearchConfig):
            return self._create_elasticsearch_sink(sink_config, source_tables, select_queries, dry_run)
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

    def _create_elasticsearch_sink(
        self,
        elasticsearch_config: ElasticsearchConfig,
        source_tables: List[str],
        select_queries: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Create Elasticsearch sink."""
        sink = ElasticsearchSink(elasticsearch_config)

        sink_results = []
        sql_statements = []

        for table_name in source_tables:
            start_time = time.time()
            
            try:
                # Update sink name for this specific table
                if len(source_tables) > 1:
                    # For multiple tables, append table name to sink name
                    table_clean = table_name.replace('-', '_').replace('.', '_').replace(' ', '_')
                    original_name = elasticsearch_config.sink_name
                    elasticsearch_config.sink_name = f"{original_name}_{table_clean}"

                # Check if we have a custom select query for this table
                select_query = select_queries.get(table_name) if select_queries else None
                
                if select_query:
                    sql = sink.create_sink_sql(select_query=select_query)
                else:
                    sql = sink.create_sink_sql(source_name=table_name)

                sql_statements.append(sql)

                if not dry_run:
                    # For Elasticsearch, we need to execute SET statement and CREATE SINK separately
                    if "SET sink_decouple = false;" in sql:
                        # Split the SQL and execute SET statement first
                        sql_parts = sql.split('\n\n')
                        if len(sql_parts) >= 2:
                            set_statement = sql_parts[0]
                            create_sink_statement = '\n\n'.join(sql_parts[1:])
                            
                            # Execute SET statement first
                            self.execute_sql(set_statement)
                            # Then execute CREATE SINK statement
                            self.execute_sql(create_sink_statement)
                        else:
                            self.execute_sql(sql)
                    else:
                        self.execute_sql(sql)

                execution_time = time.time() - start_time

                sink_results.append(SinkResult(
                    sink_name=elasticsearch_config.sink_name,
                    success=True,
                    sql_statement=sql,
                    message=f"Successfully created Elasticsearch sink for {table_name}",
                    execution_time=execution_time
                ))

                logger.info(
                    f"Successfully created Elasticsearch sink '{elasticsearch_config.sink_name}' for table '{table_name}'"
                )

                # Reset sink name for next iteration
                if len(source_tables) > 1:
                    elasticsearch_config.sink_name = original_name

            except Exception as e:
                logger.error(
                    f"Failed to create Elasticsearch sink for table {table_name}: {e}")
                sink_results.append(SinkResult(
                    sink_name=f"{elasticsearch_config.sink_name}_{table_name}",
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
