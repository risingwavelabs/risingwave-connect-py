"""Iceberg sink implementation for RisingWave."""

from __future__ import annotations
import logging
from typing import Optional, Dict, Any, Literal
from pydantic import Field, field_validator, model_validator

from .base import SinkConfig, SinkPipeline, SinkResult

logger = logging.getLogger(__name__)


class IcebergConfig(SinkConfig):
    """Configuration for Iceberg sink."""

    sink_type: str = Field(default="iceberg", description="Sink type")

    # Required Iceberg parameters
    warehouse_path: str = Field(..., min_length=1,
                                description="Warehouse path for Iceberg tables")
    database_name: str = Field(..., min_length=1,
                               description="Target Iceberg database name")
    table_name: str = Field(..., min_length=1,
                            description="Target Iceberg table name")

    # Catalog configuration
    catalog_type: Literal["glue", "rest", "jdbc", "storage"] = Field(
        ..., description="Type of Iceberg catalog"
    )
    catalog_name: Optional[str] = Field(
        None, description="Catalog name (required for glue)")
    catalog_uri: Optional[str] = Field(
        None, description="Catalog URI (required for rest/jdbc)")
    catalog_credential: Optional[str] = Field(
        None, description="Catalog credentials (for rest)")
    catalog_jdbc_user: Optional[str] = Field(
        None, description="JDBC user (for jdbc catalog)")
    catalog_jdbc_password: Optional[str] = Field(
        None, description="JDBC password (for jdbc catalog)")

    # REST catalog specific
    catalog_rest_signing_region: Optional[str] = Field(
        None, description="REST catalog signing region")
    catalog_rest_signing_name: Optional[str] = Field(
        None, description="REST catalog signing name")
    catalog_rest_sigv4_enabled: Optional[bool] = Field(
        None, description="Enable SigV4 for REST catalog")

    # Sink configuration
    data_type: Literal["append-only", "upsert"] = Field(
        default="append-only", description="Type of sink"
    )
    primary_key: Optional[str] = Field(
        None, description="Primary key for upsert sinks")
    force_append_only: bool = Field(
        default=False, description="Force append-only mode from upsert source"
    )

    # S3-compatible storage configuration
    s3_region: Optional[str] = Field(
        None, description="S3 region (either s3.endpoint or s3.region must be specified)")
    s3_endpoint: Optional[str] = Field(
        None, description="S3 endpoint URL (either s3.endpoint or s3.region must be specified)")
    s3_access_key: Optional[str] = Field(
        None, description="S3 access key (required for S3)")
    s3_secret_key: Optional[str] = Field(
        None, description="S3 secret key (required for S3)")
    s3_path_style_access: Optional[bool] = Field(
        None, description="Use path-style access for S3 (optional)")
    enable_config_load: Optional[bool] = Field(
        None, description="Load warehouse credentials from environment variables (self-hosted only)")

    # Google Cloud Storage (GCS) configuration
    gcs_credential: Optional[str] = Field(
        None, description="Base64-encoded GCS service account credential")

    # Azure Blob Storage configuration
    azblob_account_name: Optional[str] = Field(
        None, description="Azure Storage account name")
    azblob_account_key: Optional[str] = Field(
        None, description="Azure Storage account key")
    azblob_endpoint_url: Optional[str] = Field(
        None, description="Azure Blob service endpoint URL")

    # Advanced features
    is_exactly_once: bool = Field(
        default=False, description="Enable exactly-once delivery")
    commit_checkpoint_interval: int = Field(
        default=60, description="Commit interval in checkpoints"
    )
    commit_retry_num: int = Field(
        default=8, description="Number of commit retries")

    # Table management
    create_table_if_not_exists: bool = Field(
        default=False, description="Create table if it doesn't exist"
    )

    # Compaction (Premium feature)
    enable_compaction: bool = Field(
        default=False, description="Enable Iceberg compaction")
    compaction_interval_sec: int = Field(
        default=3600, description="Compaction interval in seconds"
    )
    enable_snapshot_expiration: bool = Field(
        default=False, description="Enable snapshot expiration"
    )

    # Extra properties
    extra_properties: Dict[str, Any] = Field(
        default_factory=dict, description="Additional WITH properties"
    )

    @field_validator('primary_key')
    @classmethod
    def validate_primary_key(cls, v, info):
        """Validate primary key is provided for upsert sinks."""
        data_type = info.data.get('data_type')
        if data_type == 'upsert' and not v:
            raise ValueError("primary_key is required for upsert sinks")
        return v

    @field_validator('catalog_name')
    @classmethod
    def validate_catalog_name(cls, v, info):
        """Validate catalog name is provided for glue catalog."""
        catalog_type = info.data.get('catalog_type')
        if catalog_type == 'glue' and not v:
            raise ValueError("catalog_name is required for glue catalog")
        return v

    @field_validator('catalog_uri')
    @classmethod
    def validate_catalog_uri(cls, v, info):
        """Validate catalog URI is provided for rest/jdbc catalogs."""
        catalog_type = info.data.get('catalog_type')
        if catalog_type in ['rest', 'jdbc'] and not v:
            raise ValueError(
                f"catalog_uri is required for {catalog_type} catalog")
        return v

    @field_validator('commit_checkpoint_interval')
    @classmethod
    def validate_commit_interval(cls, v):
        """Validate commit checkpoint interval."""
        if v <= 0:
            raise ValueError("commit_checkpoint_interval must be positive")
        return v

    @field_validator('commit_retry_num')
    @classmethod
    def validate_commit_retry(cls, v):
        """Validate commit retry number."""
        if v < 0:
            raise ValueError("commit_retry_num must be non-negative")
        return v

    @field_validator('compaction_interval_sec')
    @classmethod
    def validate_compaction_interval(cls, v):
        """Validate compaction interval."""
        if v <= 0:
            raise ValueError("compaction_interval_sec must be positive")
        return v

    @model_validator(mode='after')
    def validate_object_storage_config(self):
        """Validate object storage configurations."""
        warehouse_path = self.warehouse_path.lower() if self.warehouse_path else ''

        # Validate S3 configuration
        if warehouse_path.startswith('s3://') or warehouse_path.startswith('s3a://'):
            if not self.s3_region and not self.s3_endpoint:
                raise ValueError(
                    "Either s3.region or s3.endpoint must be specified for S3 storage")

        # Validate Azure Blob Storage configuration
        elif warehouse_path.startswith('azblob://') or warehouse_path.startswith('abfss://'):
            if not self.azblob_account_name:
                raise ValueError(
                    "azblob.account_name is required for Azure Blob Storage")
            if not self.azblob_account_key:
                raise ValueError(
                    "azblob.account_key is required for Azure Blob Storage")

        # Validate S3 Tables configuration
        elif warehouse_path.startswith('arn:aws:s3tables:'):
            if self.catalog_type != 'rest':
                raise ValueError(
                    "catalog.type must be 'rest' for Amazon S3 Tables")
            if not self.catalog_rest_signing_region:
                raise ValueError(
                    "catalog.rest.signing_region is required for S3 Tables")
            if not self.catalog_rest_signing_name:
                raise ValueError(
                    "catalog.rest.signing_name is required for S3 Tables")
            if not self.catalog_rest_sigv4_enabled:
                raise ValueError(
                    "catalog.rest.sigv4_enabled must be true for S3 Tables")

        return self


class IcebergSink(SinkPipeline):
    """Iceberg sink implementation."""

    def __init__(self, config: IcebergConfig):
        super().__init__(config)
        self.config: IcebergConfig = config

    def validate_config(self) -> bool:
        """Validate Iceberg configuration."""
        if not self.config.warehouse_path:
            raise ValueError("warehouse_path is required for Iceberg sink")
        if not self.config.database_name:
            raise ValueError("database_name is required for Iceberg sink")
        if not self.config.table_name:
            raise ValueError("table_name is required for Iceberg sink")
        if not self.config.catalog_type:
            raise ValueError("catalog_type is required for Iceberg sink")

        # Validate catalog-specific requirements
        if self.config.catalog_type == 'glue' and not self.config.catalog_name:
            raise ValueError("catalog_name is required for glue catalog")
        if self.config.catalog_type in ['rest', 'jdbc'] and not self.config.catalog_uri:
            raise ValueError(
                f"catalog_uri is required for {self.config.catalog_type} catalog")
        if self.config.catalog_type == 'jdbc':
            if not self.config.catalog_jdbc_user:
                raise ValueError(
                    "catalog_jdbc_user is required for jdbc catalog")
            if not self.config.catalog_jdbc_password:
                raise ValueError(
                    "catalog_jdbc_password is required for jdbc catalog")

        # Validate upsert requirements
        if self.config.data_type == 'upsert' and not self.config.primary_key:
            raise ValueError("primary_key is required for upsert sinks")

        # Validate storage backend configuration
        warehouse_path = self.config.warehouse_path.lower()
        if warehouse_path.startswith('s3://') or warehouse_path.startswith('s3a://'):
            # S3-compatible storage validation
            if not self.config.s3_region and not self.config.s3_endpoint:
                raise ValueError(
                    "Either s3.region or s3.endpoint must be specified for S3 storage")
            # Access keys are optional if using IAM roles or enable_config_load
            if not self.config.enable_config_load and not (self.config.s3_access_key and self.config.s3_secret_key):
                logger.warning(
                    "S3 access credentials not provided. Ensure IAM roles are configured or enable_config_load is true.")
        elif warehouse_path.startswith('gs://'):
            # Google Cloud Storage validation
            if not self.config.gcs_credential:
                logger.warning(
                    "GCS credential not provided. Ensure ADC (Application Default Credentials) are configured.")
        elif warehouse_path.startswith('azblob://') or warehouse_path.startswith('abfss://'):
            # Azure Blob Storage validation
            if not self.config.azblob_account_name or not self.config.azblob_account_key:
                raise ValueError(
                    "azblob.account_name and azblob.account_key are required for Azure Blob Storage")
        elif warehouse_path.startswith('arn:aws:s3tables:'):
            # Amazon S3 Tables validation
            if self.config.catalog_type != 'rest':
                raise ValueError(
                    "catalog.type must be 'rest' for Amazon S3 Tables")
            if not self.config.catalog_rest_signing_region:
                raise ValueError(
                    "catalog.rest.signing_region is required for S3 Tables")
            if not self.config.catalog_rest_signing_name:
                raise ValueError(
                    "catalog.rest.signing_name is required for S3 Tables")
            if not self.config.catalog_rest_sigv4_enabled:
                raise ValueError(
                    "catalog.rest.sigv4_enabled must be true for S3 Tables")

        return True

    def create_sink_sql(self, source_table: str, select_query: Optional[str] = None) -> str:
        """Generate CREATE SINK SQL for Iceberg.

        Args:
            source_table: Name of source table to sink from
            select_query: Optional custom SELECT query

        Returns:
            SQL CREATE SINK statement
        """
        self.validate_config()

        # Build the FROM clause or AS clause
        if select_query:
            source_clause = f"AS {select_query}"
        else:
            source_clause = f"FROM {source_table}"

        # Build WITH properties
        with_props = [
            "connector='iceberg'",
            f"type='{self.config.data_type}'",
            f"warehouse.path='{self._quote(self.config.warehouse_path)}'",
            f"database.name='{self._quote(self.config.database_name)}'",
            f"table.name='{self._quote(self.config.table_name)}'",
            f"catalog.type='{self.config.catalog_type}'"
        ]

        # Add catalog-specific properties
        if self.config.catalog_name:
            with_props.append(
                f"catalog.name='{self._quote(self.config.catalog_name)}'")
        if self.config.catalog_uri:
            with_props.append(
                f"catalog.uri='{self._quote(self.config.catalog_uri)}'")
        if self.config.catalog_credential:
            with_props.append(
                f"catalog.credential='{self._quote(self.config.catalog_credential)}'")
        if self.config.catalog_jdbc_user:
            with_props.append(
                f"catalog.jdbc.user='{self._quote(self.config.catalog_jdbc_user)}'")
        if self.config.catalog_jdbc_password:
            with_props.append(
                f"catalog.jdbc.password='{self._quote(self.config.catalog_jdbc_password)}'")

        # Add REST catalog specific properties
        if self.config.catalog_rest_signing_region:
            with_props.append(
                f"catalog.rest.signing_region='{self._quote(self.config.catalog_rest_signing_region)}'")
        if self.config.catalog_rest_signing_name:
            with_props.append(
                f"catalog.rest.signing_name='{self._quote(self.config.catalog_rest_signing_name)}'")
        if self.config.catalog_rest_sigv4_enabled is not None:
            with_props.append(
                f"catalog.rest.sigv4_enabled='{str(self.config.catalog_rest_sigv4_enabled).lower()}'")

        # Add primary key for upsert
        if self.config.primary_key:
            with_props.append(
                f"primary_key='{self._quote(self.config.primary_key)}'")

        # Add force append only
        if self.config.force_append_only:
            with_props.append("force_append_only='true'")

        # Add S3-compatible storage properties
        if self.config.s3_region:
            with_props.append(
                f"s3.region='{self._quote(self.config.s3_region)}'")
        if self.config.s3_endpoint:
            with_props.append(
                f"s3.endpoint='{self._quote(self.config.s3_endpoint)}'")
        if self.config.s3_access_key:
            with_props.append(
                f"s3.access.key='{self._quote(self.config.s3_access_key)}'")
        if self.config.s3_secret_key:
            with_props.append(
                f"s3.secret.key='{self._quote(self.config.s3_secret_key)}'")
        if self.config.s3_path_style_access is not None:
            with_props.append(
                f"s3.path.style.access='{str(self.config.s3_path_style_access).lower()}'")
        if self.config.enable_config_load is not None:
            with_props.append(
                f"enable_config_load='{str(self.config.enable_config_load).lower()}'")

        # Add Google Cloud Storage properties
        if self.config.gcs_credential:
            with_props.append(
                f"gcs.credential='{self._quote(self.config.gcs_credential)}'")

        # Add Azure Blob Storage properties
        if self.config.azblob_account_name:
            with_props.append(
                f"azblob.account_name='{self._quote(self.config.azblob_account_name)}'")
        if self.config.azblob_account_key:
            with_props.append(
                f"azblob.account_key='{self._quote(self.config.azblob_account_key)}'")
        if self.config.azblob_endpoint_url:
            with_props.append(
                f"azblob.endpoint_url='{self._quote(self.config.azblob_endpoint_url)}'")

        # Add advanced features
        if self.config.is_exactly_once:
            with_props.append("is_exactly_once='true'")
        if self.config.commit_checkpoint_interval != 60:  # Only add if not default
            with_props.append(
                f"commit_checkpoint_interval={self.config.commit_checkpoint_interval}")
        if self.config.commit_retry_num != 8:  # Only add if not default
            with_props.append(
                f"commit_retry_num={self.config.commit_retry_num}")

        # Add table management
        if self.config.create_table_if_not_exists:
            with_props.append("create_table_if_not_exists=true")

        # Add compaction features
        if self.config.enable_compaction:
            with_props.append("enable_compaction=true")
            if self.config.compaction_interval_sec != 3600:  # Only add if not default
                with_props.append(
                    f"compaction_interval_sec={self.config.compaction_interval_sec}")
        if self.config.enable_snapshot_expiration:
            with_props.append("enable_snapshot_expiration=true")

        # Add extra properties
        for key, value in self.config.extra_properties.items():
            with_props.append(f"{key}='{self._quote(str(value))}'")

        with_clause = ",\n    ".join(with_props)

        # Generate full SQL
        qualified_sink_name = f"{self.config.schema_name}.{self.config.sink_name}" if self.config.schema_name != "public" else self.config.sink_name

        sql = f"""CREATE SINK IF NOT EXISTS {qualified_sink_name}
{source_clause}
WITH (
    {with_clause}
);"""

        return sql

    def _quote(self, value: str) -> str:
        """Quote SQL string values."""
        return value.replace("'", "''")

    def create_sink(self, source_table: str, select_query: Optional[str] = None) -> SinkResult:
        """Create Iceberg sink and return result.

        Args:
            source_table: Name of source table
            select_query: Optional custom SELECT query

        Returns:
            SinkResult with creation details
        """
        try:
            sql = self.create_sink_sql(source_table, select_query)
            return SinkResult(
                sink_name=self.config.sink_name,
                sink_type=self.config.sink_type,
                sql_statement=sql,
                source_table=source_table,
                success=True
            )
        except Exception as e:
            return SinkResult(
                sink_name=self.config.sink_name,
                sink_type=self.config.sink_type,
                sql_statement="",
                source_table=source_table,
                success=False,
                error_message=str(e)
            )
