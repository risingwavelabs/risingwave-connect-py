"""Elasticsearch sink implementation for RisingWave."""

from __future__ import annotations
import logging
from typing import Optional, Dict, Any, Literal, List
from pydantic import Field, field_validator, model_validator

from .base import SinkConfig, SinkPipeline

logger = logging.getLogger(__name__)


class ElasticsearchConfig(SinkConfig):
    """Configuration for Elasticsearch sink."""

    sink_type: str = Field(default="elasticsearch", description="Sink type")

    # Required Elasticsearch parameters
    url: str = Field(..., min_length=1,
                     description="URL of the Elasticsearch REST API endpoint")
    
    # Index configuration (mutually exclusive)
    index: Optional[str] = Field(
        None, description="Name of the Elasticsearch index to write data to")
    index_column: Optional[str] = Field(
        None, description="Column whose value determines the target index dynamically")

    # Authentication
    username: Optional[str] = Field(
        None, description="Elasticsearch username for authentication")
    password: Optional[str] = Field(
        None, description="Elasticsearch password for authentication")

    # Primary key and ID configuration
    primary_key: Optional[str] = Field(
        None, description="Primary key of the sink source object")
    delimiter: str = Field(
        default=",", description="Delimiter for Elasticsearch ID when primary key has multiple columns")

    # Optional performance parameters
    routing_column: Optional[str] = Field(
        None, description="Column to use as routing key for shard-specific writes")
    retry_on_conflict: Optional[int] = Field(
        None, description="Number of retry attempts after optimistic locking conflict")
    batch_size_kb: Optional[int] = Field(
        None, description="Maximum size in kilobytes for each request batch")
    batch_num_messages: Optional[int] = Field(
        None, description="Maximum number of messages per request batch")
    concurrent_requests: Optional[int] = Field(
        None, description="Maximum number of concurrent threads for sending requests")

    # Schema management
    auto_schema_change: Optional[bool] = Field(
        None, description="Enable automatic schema changes")

    @model_validator(mode='after')
    def validate_index_config(self):
        """Validate that either index or index_column is specified, but not both."""
        if not self.index and not self.index_column:
            raise ValueError("Either 'index' or 'index_column' must be specified")
        if self.index and self.index_column:
            raise ValueError("'index' and 'index_column' are mutually exclusive")
        return self

    @model_validator(mode='after')
    def validate_auth_config(self):
        """Validate authentication configuration."""
        if bool(self.username) != bool(self.password):
            raise ValueError("Both 'username' and 'password' must be provided together")
        return self

    @field_validator('url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate Elasticsearch URL format."""
        if not v.startswith(('http://', 'https://')):
            raise ValueError("URL must start with 'http://' or 'https://'")
        return v

    @field_validator('batch_size_kb')
    @classmethod
    def validate_batch_size_kb(cls, v: Optional[int]) -> Optional[int]:
        """Validate batch size in KB."""
        if v is not None and v <= 0:
            raise ValueError("batch_size_kb must be positive")
        return v

    @field_validator('batch_num_messages')
    @classmethod
    def validate_batch_num_messages(cls, v: Optional[int]) -> Optional[int]:
        """Validate batch number of messages."""
        if v is not None and v <= 0:
            raise ValueError("batch_num_messages must be positive")
        return v

    @field_validator('concurrent_requests')
    @classmethod
    def validate_concurrent_requests(cls, v: Optional[int]) -> Optional[int]:
        """Validate concurrent requests."""
        if v is not None and v <= 0:
            raise ValueError("concurrent_requests must be positive")
        return v

    @field_validator('retry_on_conflict')
    @classmethod
    def validate_retry_on_conflict(cls, v: Optional[int]) -> Optional[int]:
        """Validate retry on conflict."""
        if v is not None and v < 0:
            raise ValueError("retry_on_conflict must be non-negative")
        return v

    def to_with_properties(self) -> Dict[str, Any]:
        """Convert configuration to RisingWave WITH properties format."""
        props = {
            'connector': 'elasticsearch',
            'url': self.url,
        }

        # Index configuration
        if self.index:
            props['index'] = self.index
        if self.index_column:
            props['index_column'] = self.index_column

        # Authentication
        if self.username:
            props['username'] = self.username
        if self.password:
            props['password'] = self.password

        # Primary key and delimiter
        if self.primary_key:
            props['primary_key'] = self.primary_key
        if self.delimiter != ",":  # Only include if non-default
            props['delimiter'] = self.delimiter

        # Optional performance parameters
        if self.routing_column:
            props['routing_column'] = self.routing_column
        if self.retry_on_conflict is not None:
            props['retry_on_conflict'] = str(self.retry_on_conflict)
        if self.batch_size_kb is not None:
            props['batch_size_kb'] = str(self.batch_size_kb)
        if self.batch_num_messages is not None:
            props['batch_num_messages'] = str(self.batch_num_messages)
        if self.concurrent_requests is not None:
            props['concurrent_requests'] = str(self.concurrent_requests)

        # Schema management
        if self.auto_schema_change is not None:
            props['auto.schema.change'] = str(self.auto_schema_change).lower()

        return props

    @classmethod
    def for_basic_auth(
        cls,
        url: str,
        index: str,
        username: str,
        password: str,
        primary_key: Optional[str] = None,
        **kwargs
    ) -> ElasticsearchConfig:
        """Create Elasticsearch config with basic authentication."""
        return cls(
            url=url,
            index=index,
            username=username,
            password=password,
            primary_key=primary_key,
            **kwargs
        )

    @classmethod
    def for_dynamic_index(
        cls,
        url: str,
        index_column: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ) -> ElasticsearchConfig:
        """Create Elasticsearch config with dynamic index based on column value."""
        return cls(
            url=url,
            index_column=index_column,
            username=username,
            password=password,
            **kwargs
        )

    @classmethod
    def for_high_throughput(
        cls,
        url: str,
        index: str,
        batch_size_kb: int = 5000,
        batch_num_messages: int = 1000,
        concurrent_requests: int = 5,
        **kwargs
    ) -> ElasticsearchConfig:
        """Create Elasticsearch config optimized for high throughput."""
        return cls(
            url=url,
            index=index,
            batch_size_kb=batch_size_kb,
            batch_num_messages=batch_num_messages,
            concurrent_requests=concurrent_requests,
            **kwargs
        )


class ElasticsearchSink(SinkPipeline):
    """Elasticsearch sink pipeline implementation."""

    def __init__(self, config: ElasticsearchConfig):
        super().__init__(config)
        self.config: ElasticsearchConfig = config

    def _generate_sink_name(self) -> str:
        """Generate a default sink name for Elasticsearch."""
        if self.config.index:
            index_clean = self.config.index.replace('-', '_').replace('.', '_').replace(' ', '_')
            return f"elasticsearch_{index_clean}_sink"
        elif self.config.index_column:
            column_clean = self.config.index_column.replace('-', '_').replace('.', '_').replace(' ', '_')
            return f"elasticsearch_dynamic_{column_clean}_sink"
        else:
            return "elasticsearch_sink"

    def create_sink_sql(
        self,
        source_name: Optional[str] = None,
        select_query: Optional[str] = None,
        if_not_exists: bool = True,
        include_set_statement: bool = True
    ) -> str:
        """
        Generate CREATE SINK SQL statement for Elasticsearch.
        
        Args:
            source_name: Name of the source table/view to sink from
            select_query: SELECT query to use instead of source_name
            if_not_exists: Whether to add IF NOT EXISTS clause
            include_set_statement: Whether to include SET sink_decouple = false statement
            
        Returns:
            Complete CREATE SINK SQL statement with required SET statement
        """
        if not source_name and not select_query:
            raise ValueError("Either source_name or select_query must be provided")
        if source_name and select_query:
            raise ValueError("source_name and select_query are mutually exclusive")

        # Build the CREATE SINK statement
        sink_clause = "CREATE SINK"
        if if_not_exists:
            sink_clause += " IF NOT EXISTS"
        
        sink_clause += f" {self.config.sink_name}"
        
        # Add FROM clause or AS SELECT
        if source_name:
            from_clause = f"FROM {source_name}"
        else:
            from_clause = f"AS {select_query}"

        # Build WITH properties
        with_props = self.config.to_with_properties()
        with_clause = "WITH (\n"
        
        # Format properties with proper indentation
        prop_lines = []
        for key, value in with_props.items():
            if isinstance(value, str) and not value.isdigit() and value.lower() not in ('true', 'false'):
                prop_lines.append(f"   {key} = '{value}'")
            else:
                prop_lines.append(f"   {key} = {value}")
        
        with_clause += ",\n".join(prop_lines)
        with_clause += "\n)"

        # Build the complete SQL with optional SET statement
        sql_parts = []
        
        if include_set_statement:
            sql_parts.append("SET sink_decouple = false;")
            sql_parts.append("")  # Empty line for readability
        
        sql_parts.append(f"""{sink_clause}
{from_clause}
{with_clause};""")

        return "\n".join(sql_parts)

    def create_sink_sql_only(
        self,
        source_name: Optional[str] = None,
        select_query: Optional[str] = None,
        if_not_exists: bool = True
    ) -> str:
        """
        Generate only the CREATE SINK SQL statement without SET statement.
        
        Args:
            source_name: Name of the source table/view to sink from
            select_query: SELECT query to use instead of source_name
            if_not_exists: Whether to add IF NOT EXISTS clause
            
        Returns:
            Only the CREATE SINK SQL statement (without SET sink_decouple)
        """
        return self.create_sink_sql(
            source_name=source_name,
            select_query=select_query,
            if_not_exists=if_not_exists,
            include_set_statement=False
        )

    def get_required_set_statements(self) -> List[str]:
        """
        Get the required SET statements that must be executed before creating this sink.
        
        Returns:
            List of SET statements required for Elasticsearch sink
        """
        return ["SET sink_decouple = false;"]

    def validate_config(self) -> List[str]:
        """Validate the Elasticsearch configuration and return any issues."""
        issues = []

        # Check required fields
        if not self.config.url:
            issues.append("URL is required")

        if not self.config.index and not self.config.index_column:
            issues.append("Either 'index' or 'index_column' must be specified")

        # Validate URL format
        if self.config.url and not self.config.url.startswith(('http://', 'https://')):
            issues.append("URL must start with 'http://' or 'https://'")

        # Check authentication completeness
        if bool(self.config.username) != bool(self.config.password):
            issues.append("Both username and password must be provided together")

        # Validate performance parameters
        if self.config.batch_size_kb is not None and self.config.batch_size_kb <= 0:
            issues.append("batch_size_kb must be positive")

        if self.config.batch_num_messages is not None and self.config.batch_num_messages <= 0:
            issues.append("batch_num_messages must be positive")

        if self.config.concurrent_requests is not None and self.config.concurrent_requests <= 0:
            issues.append("concurrent_requests must be positive")

        if self.config.retry_on_conflict is not None and self.config.retry_on_conflict < 0:
            issues.append("retry_on_conflict must be non-negative")

        return issues