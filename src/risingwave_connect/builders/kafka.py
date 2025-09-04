"""Kafka source builder for RisingWave."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union

from ..sources.kafka import KafkaConfig
from ..discovery.base import TableSelector, TableInfo
from .base import BaseSourceBuilder

logger = logging.getLogger(__name__)


class KafkaBuilder(BaseSourceBuilder):
    """Builder for Kafka sources in RisingWave."""

    def create_connection(
        self,
        config: KafkaConfig,
        table_name: Optional[str] = None,
        table_schema: Optional[Dict[str, str]] = None,
        dry_run: bool = False,
        include_offset: bool = False,
        include_partition: bool = False,
        include_timestamp: bool = False,
        include_headers: bool = False,
        include_key: bool = False,
        include_payload: bool = False,
        connection_type: str = "table"  # "source" or "table"
    ) -> Dict[str, Any]:
        """
        Create a complete Kafka source connection.

        Args:
            config: Kafka configuration
            table_name: Name for the RisingWave table/source (defaults to topic name)
            table_schema: Column definitions for the table (optional for JSON format)
            dry_run: If True, return SQL without executing
            include_offset: Include Kafka offset in table
            include_partition: Include Kafka partition in table
            include_timestamp: Include Kafka timestamp in table
            include_headers: Include Kafka headers in table
            include_key: Include Kafka key in table
            include_payload: Include Kafka payload in table
            connection_type: "source" (data not stored) or "table" (data stored)
        """
        try:
            # Generate table name if not provided
            if not table_name:
                table_name = config.topic.replace(".", "_").replace("-", "_")

            sql_statements = []
            
            if connection_type == "source":
                # Create standalone source (data not stored in RisingWave)
                source_sql = self._create_kafka_source_standalone(
                    config, table_name, table_schema,
                    include_offset, include_partition, include_timestamp, 
                    include_headers, include_key, include_payload
                )
                sql_statements.append(source_sql)
            else:
                # Create table (data stored in RisingWave)
                table_sql = self._create_kafka_table_standalone(
                    config, table_name, table_schema,
                    include_offset, include_partition, include_timestamp, 
                    include_headers, include_key, include_payload
                )
                sql_statements.append(table_sql)

            if not dry_run:
                # Execute SQL statements
                for sql in sql_statements:
                    self.rw_client.execute_sql(sql)

            # Return success result
            return {
                "success_summary": {
                    "overall_success": True,
                    "total_tables": 1 if connection_type == "table" else 0,
                    "total_sources": 1 if connection_type == "source" else 0,
                    "created_tables": [table_name] if connection_type == "table" else [],
                    "created_sources": [table_name] if connection_type == "source" else []
                },
                "source_info": {
                    "source_name": table_name,
                    "source_type": "kafka",
                    "topic": config.topic,
                    "bootstrap_servers": config.bootstrap_servers,
                    "connection_type": connection_type
                },
                "table_info": {
                    "table_name": table_name,
                    "format": f"{config.format_type} ENCODE {config.encode_type}",
                    "includes_metadata": {
                        "offset": include_offset,
                        "partition": include_partition,
                        "timestamp": include_timestamp,
                        "headers": include_headers,
                        "key": include_key,
                        "payload": include_payload
                    }
                },
                "sql_statements": sql_statements,
                "selected_tables": [table_name]
            }

        except Exception as e:
            logger.error(f"Failed to create Kafka connection: {e}")
            return {
                "success_summary": {
                    "overall_success": False,
                    "error": str(e)
                },
                "sql_statements": [],
                "selected_tables": []
            }

    def _create_kafka_source_standalone(
        self,
        config: KafkaConfig,
        source_name: str,
        table_schema: Optional[Dict[str, str]] = None,
        include_offset: bool = False,
        include_partition: bool = False,
        include_timestamp: bool = False,
        include_headers: bool = False,
        include_key: bool = False,
        include_payload: bool = False
    ) -> str:
        """Create standalone Kafka source SQL statement (data not stored)."""
        
        # Build column definitions
        columns = []
        if table_schema:
            for col_name, col_type in table_schema.items():
                columns.append(f"    {col_name} {col_type}")
        else:
            # Auto-generate basic schema based on encode type
            if config.encode_type.upper() == "JSON":
                columns.extend([
                    "    user_id INT",
                    "    product_id VARCHAR", 
                    "    timestamp TIMESTAMP"
                ])
            elif config.encode_type.upper() == "BYTES":
                columns.append("    id BYTEA")
            else:
                columns.append("    data JSONB")

        column_definitions = ",\n".join(columns) if columns else ""

        # Build INCLUDE clauses for metadata
        include_clauses = []
        if include_key:
            include_clauses.append("INCLUDE key AS kafka_key")
        if include_offset:
            include_clauses.append("INCLUDE offset AS kafka_offset")
        if include_partition:
            include_clauses.append("INCLUDE partition AS kafka_partition")
        if include_timestamp:
            include_clauses.append("INCLUDE timestamp AS kafka_timestamp")
        if include_headers:
            include_clauses.append("INCLUDE header AS kafka_headers")
        if include_payload:
            include_clauses.append("INCLUDE payload AS kafka_payload")

        include_clause = "\n" + "\n".join(include_clauses) if include_clauses else ""

        # Build format clause with ENCODE parameters
        format_clause = self._build_format_clause(config)

        # Build WITH clause with connection properties
        properties = config.to_source_properties()
        with_clauses = []
        for key, value in properties.items():
            with_clauses.append(f"    {key}='{value}'")
        with_clause = ",\n".join(with_clauses)

        sql = f"""CREATE SOURCE IF NOT EXISTS {source_name} (
{column_definitions}
){include_clause}
WITH (
{with_clause}
) {format_clause};"""

        return sql

    def _create_kafka_table_standalone(
        self,
        config: KafkaConfig,
        table_name: str,
        table_schema: Optional[Dict[str, str]] = None,
        include_offset: bool = False,
        include_partition: bool = False,
        include_timestamp: bool = False,
        include_headers: bool = False,
        include_key: bool = False,
        include_payload: bool = False
    ) -> str:
        """Create standalone Kafka table SQL statement (data stored)."""
        
        # Build column definitions with primary key if needed
        columns = []
        primary_key_columns = []
        
        if table_schema:
            for col_name, col_type in table_schema.items():
                if "PRIMARY KEY" in col_type.upper():
                    primary_key_columns.append(col_name)
                    columns.append(f"    {col_name} {col_type}")
                else:
                    columns.append(f"    {col_name} {col_type}")
        else:
            # Auto-generate basic schema based on encode type
            if config.encode_type.upper() == "JSON":
                columns.extend([
                    "    user_id INT",
                    "    product_id VARCHAR", 
                    "    timestamp TIMESTAMP"
                ])
                if config.format_type.upper() == "UPSERT":
                    primary_key_columns.append("user_id")
            elif config.encode_type.upper() == "BYTES":
                columns.append("    id BYTEA")
            else:
                columns.append("    data JSONB")

        # Add primary key constraint if UPSERT format
        if config.format_type.upper() == "UPSERT" and include_key:
            if not primary_key_columns:
                columns.append("    PRIMARY KEY (rw_key)")
            elif len(primary_key_columns) == 1:
                # Replace single column definition with PRIMARY KEY
                for i, col in enumerate(columns):
                    if primary_key_columns[0] in col:
                        if "PRIMARY KEY" not in col:
                            columns[i] = col.replace(primary_key_columns[0], f"{primary_key_columns[0]} PRIMARY KEY", 1)

        column_definitions = ",\n".join(columns) if columns else ""

        # Build INCLUDE clauses for metadata
        include_clauses = []
        if include_key:
            if config.format_type.upper() == "UPSERT":
                include_clauses.append("INCLUDE key AS rw_key")
            else:
                include_clauses.append("INCLUDE key AS kafka_key")
        if include_offset:
            include_clauses.append("INCLUDE offset AS kafka_offset")
        if include_partition:
            include_clauses.append("INCLUDE partition AS kafka_partition")
        if include_timestamp:
            include_clauses.append("INCLUDE timestamp AS kafka_timestamp")
        if include_headers:
            include_clauses.append("INCLUDE header AS kafka_headers")
        if include_payload:
            include_clauses.append("INCLUDE payload AS kafka_payload")

        include_clause = "\n" + "\n".join(include_clauses) if include_clauses else ""

        # Build format clause with ENCODE parameters
        format_clause = self._build_format_clause(config)

        # Build WITH clause with connection properties
        properties = config.to_source_properties()
        with_clauses = []
        for key, value in properties.items():
            with_clauses.append(f"    {key}='{value}'")
        with_clause = ",\n".join(with_clauses)

        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{column_definitions}
){include_clause}
WITH (
{with_clause}
) {format_clause};"""

        return sql

    def _build_format_clause(self, config: KafkaConfig) -> str:
        """Build the FORMAT clause with encoding parameters."""
        format_clause = f"FORMAT {config.format_type} ENCODE {config.encode_type}"
        
        # Add encoding-specific parameters
        encode_params = []
        if config.encode_type.upper() == "AVRO":
            if config.avro_message:
                encode_params.append(f"message = '{config.avro_message}'")
            if config.schema_registry_url:
                encode_params.append(f"schema.registry = '{config.schema_registry_url}'")
            if config.schema_registry_username:
                encode_params.append(f"schema.registry.username='{config.schema_registry_username}'")
            if config.schema_registry_password:
                encode_params.append(f"schema.registry.password='{config.schema_registry_password}'")
                
        elif config.encode_type.upper() == "PROTOBUF":
            if config.protobuf_message:
                encode_params.append(f"message = '{config.protobuf_message}'")
            if config.protobuf_access_key:
                encode_params.append(f"access_key = '{config.protobuf_access_key}'")
            if config.protobuf_secret_key:
                encode_params.append(f"secret_key = '{config.protobuf_secret_key}'")
            if config.protobuf_location:
                encode_params.append(f"location = '{config.protobuf_location}'")
                
        elif config.encode_type.upper() == "CSV":
            encode_params.append(f"without_header = '{str(config.csv_without_header).lower()}'")
            encode_params.append(f"delimiter = '{config.csv_delimiter}'")
        
        if encode_params:
            format_clause += f" (\n   {',\\n   '.join(encode_params)}\n)"
        
        # Add key encoding if specified
        if config.key_encode_type:
            format_clause += f" KEY ENCODE {config.key_encode_type}"
            
        return format_clause

    def discover_tables(
        self,
        config: KafkaConfig,
        database_name: Optional[str] = None
    ) -> List[TableInfo]:
        """
        Discover Kafka topics (not applicable for Kafka).
        Kafka doesn't have discoverable tables like databases.
        """
        logger.info(
            "Kafka doesn't support table discovery. Use topic information directly.")
        return [
            TableInfo(
                schema_name="kafka",
                table_name=config.topic,
                column_count=0,  # Unknown until table is created
                estimated_row_count=0  # Not applicable for streaming
            )
        ]

    def get_schemas(self, config: KafkaConfig) -> List[str]:
        """Get list of available schemas (not applicable for Kafka)."""
        logger.info("Kafka doesn't have schemas. Topics are the primary unit.")
        return ["kafka"]
