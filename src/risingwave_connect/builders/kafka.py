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
        specific_headers: Optional[Dict[str, str]] = None,  # {header_name: data_type}
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
            include_headers: Include all Kafka headers in table as STRUCT array
            include_key: Include Kafka key in table
            include_payload: Include Kafka payload in table (JSON format only)
            specific_headers: Include specific header fields {header_name: data_type}
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
                    include_headers, include_key, include_payload, specific_headers
                )
                sql_statements.append(source_sql)
            else:
                # Create table (data stored in RisingWave)
                table_sql = self._create_kafka_table_standalone(
                    config, table_name, table_schema,
                    include_offset, include_partition, include_timestamp, 
                    include_headers, include_key, include_payload, specific_headers
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
                    "format": f"{config.data_format} ENCODE {config.data_encode}",
                    "includes_metadata": {
                        "offset": include_offset,
                        "partition": include_partition,
                        "timestamp": include_timestamp,
                        "headers": include_headers,
                        "key": include_key,
                        "payload": include_payload,
                        "specific_headers": specific_headers or {}
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
        include_payload: bool = False,
        specific_headers: Optional[Dict[str, str]] = None
    ) -> str:
        """Create standalone Kafka source SQL statement (data not stored)."""
        
        # Build column definitions
        columns = []
        if table_schema:
            for col_name, col_type in table_schema.items():
                columns.append(f"    {col_name} {col_type}")
        else:
            # Auto-generate basic schema based on encode type
            if config.data_encode.upper() == "JSON":
                columns.extend([
                    "    user_id INT",
                    "    product_id VARCHAR", 
                    "    timestamp TIMESTAMP"
                ])
            elif config.data_encode.upper() == "BYTES":
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
        if include_payload and config.data_encode.upper() == "JSON":
            include_clauses.append("INCLUDE payload AS kafka_payload")
        
        # Add specific header fields
        if specific_headers:
            for header_name, data_type in specific_headers.items():
                include_clauses.append(f"INCLUDE header '{header_name}' {data_type} AS {header_name}")

        include_clause = "\n" + "\n".join(include_clauses) if include_clauses else ""

        # Build format clause with ENCODE parameters
        format_clause = self._build_format_clause(config)

        # Build WITH clause with connection properties
        properties = config.to_source_properties()
        with_clauses = []
        for key, value in properties.items():
            with_clauses.append(f"   {key}='{value}'")
        with_clause = ",\n".join(with_clauses)

        # Build the final SQL statement
        if column_definitions:
            sql = f"""CREATE SOURCE IF NOT EXISTS {source_name} (
{column_definitions}
){include_clause}
WITH (
{with_clause}
) {format_clause};"""
        else:
            # Schema-less source (like AVRO with schema registry)
            sql = f"""CREATE SOURCE IF NOT EXISTS {source_name}{include_clause}
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
        include_payload: bool = False,
        specific_headers: Optional[Dict[str, str]] = None
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
            if config.data_encode.upper() == "JSON":
                columns.extend([
                    "    user_id INT",
                    "    product_id VARCHAR", 
                    "    timestamp TIMESTAMP"
                ])
                if config.data_format.upper() == "UPSERT":
                    primary_key_columns.append("user_id")
            elif config.data_encode.upper() == "BYTES":
                columns.append("    id BYTEA")
            else:
                columns.append("    data JSONB")

        # Add primary key constraint if UPSERT format
        if config.data_format.upper() == "UPSERT":
            # For UPSERT, we need to handle the key inclusion properly
            if include_key:
                # If no explicit primary key in schema, use rw_key as primary key
                if not primary_key_columns:
                    # Add rw_key as primary key if not already defined in schema
                    if not any("rw_key" in col for col in columns):
                        columns.append("    primary key (rw_key)")
            elif not primary_key_columns:
                # If no key inclusion and no primary key defined, warn or error
                logger.warning("UPSERT format typically requires a primary key. Consider setting include_key=True or defining a primary key in table_schema.")

        column_definitions = ",\n".join(columns) if columns else ""

        # Build INCLUDE clauses for metadata
        include_clauses = []
        if include_key:
            if config.data_format.upper() == "UPSERT":
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
        if include_payload and config.data_encode.upper() == "JSON":
            include_clauses.append("INCLUDE payload AS kafka_payload")
        
        # Add specific header fields
        if specific_headers:
            for header_name, data_type in specific_headers.items():
                include_clauses.append(f"INCLUDE header '{header_name}' {data_type} AS {header_name}")

        include_clause = "\n" + "\n".join(include_clauses) if include_clauses else ""

        # Build format clause with ENCODE parameters
        format_clause = self._build_format_clause(config)

        # Build WITH clause with connection properties
        properties = config.to_source_properties()
        with_clauses = []
        for key, value in properties.items():
            with_clauses.append(f"   {key}='{value}'")
        with_clause = ",\n".join(with_clauses)

        # Build the final SQL statement
        if column_definitions:
            sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{column_definitions}
){include_clause}
WITH (
{with_clause}
) {format_clause};"""
        else:
            # Schema-less table (like AVRO with schema registry)
            sql = f"""CREATE TABLE IF NOT EXISTS {table_name}{include_clause}
WITH (
{with_clause}
) {format_clause};"""

        return sql

    def _build_format_clause(self, config: KafkaConfig) -> str:
        """Build the FORMAT clause with encoding parameters."""
        format_clause = f"FORMAT {config.data_format} ENCODE {config.data_encode}"
        
        # Get format/encoding specific parameters
        format_properties = config.get_format_encode_properties()
        
        if format_properties:
            encode_params = []
            for key, value in format_properties.items():
                # Handle different value types appropriately
                if isinstance(value, bool):
                    encode_params.append(f"{key} = '{str(value).lower()}'")
                else:
                    encode_params.append(f"{key} = '{value}'")
            
            # Format the parameters with proper indentation
            if len(encode_params) == 1:
                format_clause += f" ({encode_params[0]})"
            else:
                param_str = ',\n   '.join(encode_params)
                format_clause += f" (\n   {param_str}\n)"
        
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
