"""Template implementations for additional sink types."""

from __future__ import annotations
from typing import Optional, Dict, Any
from pydantic import Field, validator

from .base import SinkConfig, SinkPipeline, SinkResult


# =============================================================================
# SNOWFLAKE SINK
# =============================================================================

class SnowflakeConfig(SinkConfig):
    """Configuration for Snowflake sink."""

    sink_type: str = Field(default="snowflake", description="Sink type")

    # Snowflake connection parameters
    account: str = Field(..., description="Snowflake account identifier")
    warehouse: str = Field(..., description="Snowflake warehouse")
    database_name: str = Field(..., description="Snowflake database")
    snowflake_schema: str = Field(
        default="PUBLIC", description="Snowflake schema")
    username: str = Field(..., description="Snowflake username")
    password: str = Field(..., description="Snowflake password")
    role: Optional[str] = Field(None, description="Snowflake role")

    # Table configuration
    table_name: Optional[str] = Field(None, description="Target table name")
    data_type: str = Field(default="append-only", description="Sink data type")

    # Extra properties
    extra_properties: Dict[str, Any] = Field(default_factory=dict)


class SnowflakeSink(SinkPipeline):
    """Snowflake sink implementation."""

    def __init__(self, config: SnowflakeConfig):
        super().__init__(config)
        self.config: SnowflakeConfig = config

    def validate_config(self) -> bool:
        """Validate Snowflake configuration."""
        required_fields = ['account', 'warehouse',
                           'database_name', 'username', 'password']
        for field in required_fields:
            if not getattr(self.config, field):
                raise ValueError(f"{field} is required for Snowflake sink")
        return True

    def create_sink_sql(self, source_table: str, select_query: Optional[str] = None) -> str:
        """Generate CREATE SINK SQL for Snowflake."""
        self.validate_config()

        source_clause = f"AS {select_query}" if select_query else f"FROM {source_table}"
        target_table = self.config.table_name or self.config.sink_name

        with_props = [
            "connector='snowflake'",
            f"snowflake.account='{self._quote(self.config.account)}'",
            f"snowflake.warehouse='{self._quote(self.config.warehouse)}'",
            f"snowflake.database='{self._quote(self.config.database_name)}'",
            f"snowflake.schema='{self._quote(self.config.snowflake_schema)}'",
            f"snowflake.table='{self._quote(target_table)}'",
            f"snowflake.user='{self._quote(self.config.username)}'",
            f"snowflake.password='{self._quote(self.config.password)}'",
            f"type='{self.config.data_type}'"
        ]

        if self.config.role:
            with_props.append(
                f"snowflake.role='{self._quote(self.config.role)}'")

        # Add extra properties
        for key, value in self.config.extra_properties.items():
            with_props.append(f"{key}='{self._quote(str(value))}'")

        with_clause = ",\n    ".join(with_props)
        qualified_sink_name = f"{self.config.schema_name}.{self.config.sink_name}" if self.config.schema_name != "public" else self.config.sink_name

        return f"""CREATE SINK IF NOT EXISTS {qualified_sink_name}
{source_clause}
WITH (
    {with_clause}
);"""

    def _quote(self, value: str) -> str:
        """Quote SQL string values."""
        return value.replace("'", "''")


# =============================================================================
# ICEBERG SINK
# =============================================================================

class IcebergConfig(SinkConfig):
    """Configuration for Iceberg sink."""

    sink_type: str = Field(default="iceberg", description="Sink type")

    # Iceberg parameters
    catalog_name: str = Field(..., description="Iceberg catalog name")
    database_name: str = Field(..., description="Iceberg database")
    table_name: Optional[str] = Field(None, description="Target table name")
    warehouse_path: str = Field(..., description="Warehouse path")

    # S3 configuration for Iceberg
    s3_region: Optional[str] = Field(None, description="S3 region")
    s3_access_key: Optional[str] = Field(None, description="S3 access key")
    s3_secret_key: Optional[str] = Field(None, description="S3 secret key")

    data_type: str = Field(default="append-only", description="Sink data type")
    extra_properties: Dict[str, Any] = Field(default_factory=dict)


class IcebergSink(SinkPipeline):
    """Iceberg sink implementation."""

    def __init__(self, config: IcebergConfig):
        super().__init__(config)
        self.config: IcebergConfig = config

    def validate_config(self) -> bool:
        """Validate Iceberg configuration."""
        if not self.config.catalog_name:
            raise ValueError("catalog_name is required for Iceberg sink")
        if not self.config.warehouse_path:
            raise ValueError("warehouse_path is required for Iceberg sink")
        return True

    def create_sink_sql(self, source_table: str, select_query: Optional[str] = None) -> str:
        """Generate CREATE SINK SQL for Iceberg."""
        self.validate_config()

        source_clause = f"AS {select_query}" if select_query else f"FROM {source_table}"
        target_table = self.config.table_name or self.config.sink_name

        with_props = [
            "connector='iceberg'",
            f"catalog.name='{self._quote(self.config.catalog_name)}'",
            f"database.name='{self._quote(self.config.database_name)}'",
            f"table.name='{self._quote(target_table)}'",
            f"warehouse.path='{self._quote(self.config.warehouse_path)}'",
            f"type='{self.config.data_type}'"
        ]

        # Add S3 configuration if provided
        if self.config.s3_region:
            with_props.append(
                f"s3.region='{self._quote(self.config.s3_region)}'")
        if self.config.s3_access_key:
            with_props.append(
                f"s3.access.key='{self._quote(self.config.s3_access_key)}'")
        if self.config.s3_secret_key:
            with_props.append(
                f"s3.secret.key='{self._quote(self.config.s3_secret_key)}'")

        # Add extra properties
        for key, value in self.config.extra_properties.items():
            with_props.append(f"{key}='{self._quote(str(value))}'")

        with_clause = ",\n    ".join(with_props)
        qualified_sink_name = f"{self.config.schema_name}.{self.config.sink_name}" if self.config.schema_name != "public" else self.config.sink_name

        return f"""CREATE SINK IF NOT EXISTS {qualified_sink_name}
{source_clause}
WITH (
    {with_clause}
);"""

    def _quote(self, value: str) -> str:
        """Quote SQL string values."""
        return value.replace("'", "''")


# =============================================================================
# ELASTICSEARCH SINK
# =============================================================================

class ElasticsearchConfig(SinkConfig):
    """Configuration for Elasticsearch sink."""

    sink_type: str = Field(default="elasticsearch", description="Sink type")

    # Elasticsearch parameters
    url: str = Field(..., description="Elasticsearch URL")
    index_name: str = Field(..., description="Elasticsearch index name")
    username: Optional[str] = Field(None, description="Elasticsearch username")
    password: Optional[str] = Field(None, description="Elasticsearch password")

    # Index configuration
    id_field: Optional[str] = Field(
        None, description="Field to use as document ID")
    routing_field: Optional[str] = Field(None, description="Field for routing")

    data_type: str = Field(default="append-only", description="Sink data type")
    extra_properties: Dict[str, Any] = Field(default_factory=dict)


class ElasticsearchSink(SinkPipeline):
    """Elasticsearch sink implementation."""

    def __init__(self, config: ElasticsearchConfig):
        super().__init__(config)
        self.config: ElasticsearchConfig = config

    def validate_config(self) -> bool:
        """Validate Elasticsearch configuration."""
        if not self.config.url:
            raise ValueError("url is required for Elasticsearch sink")
        if not self.config.index_name:
            raise ValueError("index_name is required for Elasticsearch sink")
        return True

    def create_sink_sql(self, source_table: str, select_query: Optional[str] = None) -> str:
        """Generate CREATE SINK SQL for Elasticsearch."""
        self.validate_config()

        source_clause = f"AS {select_query}" if select_query else f"FROM {source_table}"

        with_props = [
            "connector='elasticsearch'",
            f"url='{self._quote(self.config.url)}'",
            f"index='{self._quote(self.config.index_name)}'",
            f"type='{self.config.data_type}'"
        ]

        # Add authentication if provided
        if self.config.username:
            with_props.append(
                f"username='{self._quote(self.config.username)}'")
        if self.config.password:
            with_props.append(
                f"password='{self._quote(self.config.password)}'")

        # Add optional fields
        if self.config.id_field:
            with_props.append(
                f"id.field='{self._quote(self.config.id_field)}'")
        if self.config.routing_field:
            with_props.append(
                f"routing.field='{self._quote(self.config.routing_field)}'")

        # Add extra properties
        for key, value in self.config.extra_properties.items():
            with_props.append(f"{key}='{self._quote(str(value))}'")

        with_clause = ",\n    ".join(with_props)
        qualified_sink_name = f"{self.config.schema_name}.{self.config.sink_name}" if self.config.schema_name != "public" else self.config.sink_name

        return f"""CREATE SINK IF NOT EXISTS {qualified_sink_name}
{source_clause}
WITH (
    {with_clause}
);"""

    def _quote(self, value: str) -> str:
        """Quote SQL string values."""
        return value.replace("'", "''")


# =============================================================================
# KAFKA SINK
# =============================================================================

class KafkaConfig(SinkConfig):
    """Configuration for Kafka sink."""

    sink_type: str = Field(default="kafka", description="Sink type")

    # Kafka parameters
    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    topic: str = Field(..., description="Kafka topic name")

    # Optional Kafka parameters
    security_protocol: Optional[str] = Field(
        None, description="Security protocol")
    sasl_mechanism: Optional[str] = Field(None, description="SASL mechanism")
    sasl_username: Optional[str] = Field(None, description="SASL username")
    sasl_password: Optional[str] = Field(None, description="SASL password")

    # Message configuration
    key_field: Optional[str] = Field(
        None, description="Field to use as message key")
    format_type: str = Field(default="JSON", description="Message format")

    data_type: str = Field(default="append-only", description="Sink data type")
    extra_properties: Dict[str, Any] = Field(default_factory=dict)


class KafkaSink(SinkPipeline):
    """Kafka sink implementation."""

    def __init__(self, config: KafkaConfig):
        super().__init__(config)
        self.config: KafkaConfig = config

    def validate_config(self) -> bool:
        """Validate Kafka configuration."""
        if not self.config.bootstrap_servers:
            raise ValueError("bootstrap_servers is required for Kafka sink")
        if not self.config.topic:
            raise ValueError("topic is required for Kafka sink")
        return True

    def create_sink_sql(self, source_table: str, select_query: Optional[str] = None) -> str:
        """Generate CREATE SINK SQL for Kafka."""
        self.validate_config()

        source_clause = f"AS {select_query}" if select_query else f"FROM {source_table}"

        with_props = [
            "connector='kafka'",
            f"properties.bootstrap.server='{self._quote(self.config.bootstrap_servers)}'",
            f"topic='{self._quote(self.config.topic)}'",
            f"type='{self.config.data_type}'"
        ]

        # Add security configuration if provided
        if self.config.security_protocol:
            with_props.append(
                f"properties.security.protocol='{self._quote(self.config.security_protocol)}'")
        if self.config.sasl_mechanism:
            with_props.append(
                f"properties.sasl.mechanism='{self._quote(self.config.sasl_mechanism)}'")
        if self.config.sasl_username:
            with_props.append(
                f"properties.sasl.username='{self._quote(self.config.sasl_username)}'")
        if self.config.sasl_password:
            with_props.append(
                f"properties.sasl.password='{self._quote(self.config.sasl_password)}'")

        # Add optional fields
        if self.config.key_field:
            with_props.append(
                f"key.field='{self._quote(self.config.key_field)}'")

        # Add extra properties
        for key, value in self.config.extra_properties.items():
            with_props.append(f"{key}='{self._quote(str(value))}'")

        with_clause = ",\n    ".join(with_props)
        qualified_sink_name = f"{self.config.schema_name}.{self.config.sink_name}" if self.config.schema_name != "public" else self.config.sink_name

        sql = f"""CREATE SINK IF NOT EXISTS {qualified_sink_name}
{source_clause}
WITH (
    {with_clause}
)
FORMAT {self.config.format_type};"""

        return sql

    def _quote(self, value: str) -> str:
        """Quote SQL string values."""
        return value.replace("'", "''")


def example_usage():
    """Show example usage of template sinks."""
    snowflake_config = SnowflakeConfig(
        sink_name="orders_to_snowflake",
        account="xy12345.us-east-1",
        warehouse="COMPUTE_WH",
        database_name="ANALYTICS",
        snowflake_schema="RAW_DATA",
        username="etl_user",
        password="secure_password",
        table_name="orders"
    )

    snowflake_sink = SnowflakeSink(snowflake_config)
    print(snowflake_sink.create_sink_sql("public.orders"))

    kafka_config = KafkaConfig(
        sink_name="events_to_kafka",
        bootstrap_servers="localhost:9092",
        topic="user_events",
        key_field="user_id",
        format_type="JSON"
    )

    kafka_sink = KafkaSink(kafka_config)
    print(kafka_sink.create_sink_sql("public.user_events"))


if __name__ == "__main__":
    example_usage()
