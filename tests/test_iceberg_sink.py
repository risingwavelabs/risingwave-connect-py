"""Tests for Iceberg sink implementation."""

import pytest
from risingwave_connect.sinks.iceberg import IcebergConfig, IcebergSink


class TestIcebergConfig:
    """Test IcebergConfig validation."""

    def test_valid_config_storage_catalog(self):
        """Test valid configuration with storage catalog."""
        config = IcebergConfig(
            sink_name="test_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage",
            s3_region="us-west-2"
        )
        assert config.sink_name == "test_sink"
        assert config.catalog_type == "storage"
        assert config.data_type == "append-only"  # default

    def test_valid_config_glue_catalog(self):
        """Test valid configuration with Glue catalog."""
        config = IcebergConfig(
            sink_name="glue_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="glue",
            catalog_name="my_catalog",
            s3_region="us-west-2"
        )
        assert config.catalog_type == "glue"
        assert config.catalog_name == "my_catalog"

    def test_valid_config_rest_catalog(self):
        """Test valid configuration with REST catalog."""
        config = IcebergConfig(
            sink_name="rest_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="rest",
            catalog_uri="http://rest-catalog:8181",
            s3_region="us-west-2"
        )
        assert config.catalog_type == "rest"
        assert config.catalog_uri == "http://rest-catalog:8181"

    def test_valid_config_jdbc_catalog(self):
        """Test valid configuration with JDBC catalog."""
        config = IcebergConfig(
            sink_name="jdbc_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="jdbc",
            catalog_uri="jdbc:postgresql://postgres:5432/catalog",
            catalog_jdbc_user="user",
            catalog_jdbc_password="password",
            s3_region="us-west-2"
        )
        assert config.catalog_type == "jdbc"
        assert config.catalog_jdbc_user == "user"

    def test_upsert_with_primary_key(self):
        """Test upsert configuration with primary key."""
        config = IcebergConfig(
            sink_name="upsert_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage",
            data_type="upsert",
            primary_key="id",
            s3_region="us-west-2"
        )
        assert config.data_type == "upsert"
        assert config.primary_key == "id"

    def test_invalid_upsert_without_primary_key(self):
        """Test that upsert without primary key raises error."""
        with pytest.raises(ValueError, match="primary_key is required for upsert sinks"):
            IcebergConfig(
                sink_name="invalid_sink",
                warehouse_path="s3://bucket/warehouse",
                database_name="test_db",
                table_name="test_table",
                catalog_type="storage",
                data_type="upsert",
                s3_region="us-west-2"
            )

    def test_invalid_glue_without_catalog_name(self):
        """Test that glue catalog without name raises error."""
        with pytest.raises(ValueError, match="catalog_name is required for glue catalog"):
            IcebergConfig(
                sink_name="invalid_sink",
                warehouse_path="s3://bucket/warehouse",
                database_name="test_db",
                table_name="test_table",
                catalog_type="glue",
                s3_region="us-west-2"
            )

    def test_invalid_rest_without_uri(self):
        """Test that REST catalog without URI raises error."""
        with pytest.raises(ValueError, match="catalog_uri is required for rest catalog"):
            IcebergConfig(
                sink_name="invalid_sink",
                warehouse_path="s3://bucket/warehouse",
                database_name="test_db",
                table_name="test_table",
                catalog_type="rest",
                s3_region="us-west-2"
            )

    def test_invalid_commit_interval(self):
        """Test invalid commit interval."""
        with pytest.raises(ValueError, match="commit_checkpoint_interval must be positive"):
            IcebergConfig(
                sink_name="invalid_sink",
                warehouse_path="s3://bucket/warehouse",
                database_name="test_db",
                table_name="test_table",
                catalog_type="storage",
                commit_checkpoint_interval=0,
                s3_region="us-west-2"
            )


class TestIcebergSink:
    """Test IcebergSink functionality."""

    def test_storage_catalog_sql_generation(self):
        """Test SQL generation for storage catalog."""
        config = IcebergConfig(
            sink_name="test_sink",
            warehouse_path="s3a://yuxuan-iceberg-test/demo",
            database_name="demo_db",
            table_name="t2",
            catalog_type="storage",
            catalog_name="demo",
            data_type="append-only",
            force_append_only=True,
            create_table_if_not_exists=True,
            s3_region="us-east-1",
            s3_endpoint="https://s3.us-east-1.amazonaws.com",
            s3_access_key="test-access-key-id",
            s3_secret_key="test-secret-access-key"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("mv1", "SELECT * FROM mv1")

        # Verify the generated SQL contains expected elements
        assert "CREATE SINK IF NOT EXISTS test_sink" in sql
        assert "AS SELECT * FROM mv1" in sql
        assert "connector='iceberg'" in sql
        assert "type='append-only'" in sql
        assert "warehouse.path='s3a://yuxuan-iceberg-test/demo'" in sql
        assert "database.name='demo_db'" in sql
        assert "table.name='t2'" in sql
        assert "catalog.type='storage'" in sql
        assert "catalog.name='demo'" in sql
        assert "force_append_only='true'" in sql
        assert "create_table_if_not_exists=true" in sql
        assert "s3.region='us-east-1'" in sql
        assert "s3.endpoint='https://s3.us-east-1.amazonaws.com'" in sql

    def test_glue_catalog_sql_generation(self):
        """Test SQL generation for Glue catalog."""
        config = IcebergConfig(
            sink_name="glue_sink",
            warehouse_path="s3://my-bucket/warehouse",
            database_name="my_database",
            table_name="my_table",
            catalog_type="glue",
            catalog_name="my_catalog",
            s3_region="us-west-2",
            s3_access_key="access_key",
            s3_secret_key="secret_key"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("source_table")

        assert "FROM source_table" in sql
        assert "catalog.type='glue'" in sql
        assert "catalog.name='my_catalog'" in sql
        assert "s3.region='us-west-2'" in sql

    def test_upsert_sql_generation(self):
        """Test SQL generation for upsert sink."""
        config = IcebergConfig(
            sink_name="upsert_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage",
            data_type="upsert",
            primary_key="id,timestamp",
            is_exactly_once=True,
            s3_region="us-west-2"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("users")

        assert "type='upsert'" in sql
        assert "primary_key='id,timestamp'" in sql
        assert "is_exactly_once='true'" in sql

    def test_rest_catalog_sql_generation(self):
        """Test SQL generation for REST catalog."""
        config = IcebergConfig(
            sink_name="rest_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="rest",
            catalog_uri="http://rest-catalog:8181",
            catalog_credential="user:pass",
            catalog_rest_signing_region="us-east-1",
            catalog_rest_signing_name="s3tables",
            catalog_rest_sigv4_enabled=True,
            s3_region="us-east-1"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("source")

        assert "catalog.type='rest'" in sql
        assert "catalog.uri='http://rest-catalog:8181'" in sql
        assert "catalog.credential='user:pass'" in sql
        assert "catalog.rest.signing_region='us-east-1'" in sql
        assert "catalog.rest.signing_name='s3tables'" in sql
        assert "catalog.rest.sigv4_enabled='true'" in sql

    def test_jdbc_catalog_sql_generation(self):
        """Test SQL generation for JDBC catalog."""
        config = IcebergConfig(
            sink_name="jdbc_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="jdbc",
            catalog_uri="jdbc:postgresql://postgres:5432/catalog",
            catalog_jdbc_user="catalog_user",
            catalog_jdbc_password="catalog_password",
            s3_region="us-west-2"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("source")

        assert "catalog.type='jdbc'" in sql
        assert "catalog.uri='jdbc:postgresql://postgres:5432/catalog'" in sql
        assert "catalog.jdbc.user='catalog_user'" in sql
        assert "catalog.jdbc.password='catalog_password'" in sql

    def test_advanced_features_sql_generation(self):
        """Test SQL generation with advanced features."""
        config = IcebergConfig(
            sink_name="advanced_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage",
            commit_checkpoint_interval=10,
            commit_retry_num=5,
            enable_compaction=True,
            compaction_interval_sec=1800,
            enable_snapshot_expiration=True,
            s3_region="us-west-2"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("source")

        assert "commit_checkpoint_interval=10" in sql
        assert "commit_retry_num=5" in sql
        assert "enable_compaction=true" in sql
        assert "compaction_interval_sec=1800" in sql
        assert "enable_snapshot_expiration=true" in sql

    def test_gcs_configuration(self):
        """Test GCS configuration."""
        config = IcebergConfig(
            sink_name="gcs_sink",
            warehouse_path="gs://my-bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="rest",
            catalog_uri="http://catalog:8181",
            gcs_credential="base64-encoded-credential"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("source")

        assert "gcs.credential='base64-encoded-credential'" in sql

    def test_azure_configuration(self):
        """Test Azure configuration."""
        config = IcebergConfig(
            sink_name="azure_sink",
            warehouse_path="abfss://container@account.dfs.core.windows.net/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="rest",
            catalog_uri="http://catalog:8181",
            azblob_account_name="my_account",
            azblob_account_key="my_key"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("source")

        assert "azblob.account_name='my_account'" in sql
        assert "azblob.account_key='my_key'" in sql

    def test_validation_missing_warehouse_path(self):
        """Test validation fails for missing warehouse path."""
        config = IcebergConfig(
            sink_name="test_sink",
            warehouse_path="",
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage",
            s3_region="us-west-2"
        )

        sink = IcebergSink(config)
        with pytest.raises(ValueError, match="warehouse_path is required"):
            sink.validate_config()

    def test_validation_s3_missing_region(self):
        """Test validation fails for S3 without region."""
        config = IcebergConfig(
            sink_name="test_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage"
        )

        sink = IcebergSink(config)
        with pytest.raises(ValueError, match="s3_region is required for S3 storage"):
            sink.validate_config()

    def test_create_sink_success(self):
        """Test successful sink creation."""
        config = IcebergConfig(
            sink_name="test_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage",
            s3_region="us-west-2"
        )

        sink = IcebergSink(config)
        result = sink.create_sink("source_table")

        assert result.success
        assert result.sink_name == "test_sink"
        assert result.sink_type == "iceberg"
        assert result.source_table == "source_table"
        assert "CREATE SINK" in result.sql_statement

    def test_create_sink_failure(self):
        """Test sink creation with invalid configuration."""
        config = IcebergConfig(
            sink_name="test_sink",
            warehouse_path="",  # Invalid
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage",
            s3_region="us-west-2"
        )

        sink = IcebergSink(config)
        result = sink.create_sink("source_table")

        assert not result.success
        assert result.error_message is not None
        assert "warehouse_path is required" in result.error_message

    def test_extra_properties(self):
        """Test extra properties are included in SQL."""
        config = IcebergConfig(
            sink_name="test_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test_db",
            table_name="test_table",
            catalog_type="storage",
            s3_region="us-west-2",
            extra_properties={
                "custom_property": "custom_value",
                "another_prop": "another_value"
            }
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("source")

        assert "custom_property='custom_value'" in sql
        assert "another_prop='another_value'" in sql

    def test_sql_injection_protection(self):
        """Test SQL injection protection in string quoting."""
        config = IcebergConfig(
            sink_name="test_sink",
            warehouse_path="s3://bucket/warehouse",
            database_name="test'db",  # Contains single quote
            table_name="test_table",
            catalog_type="storage",
            s3_region="us-west-2"
        )

        sink = IcebergSink(config)
        sql = sink.create_sink_sql("source")

        # Single quote should be escaped
        assert "database.name='test''db'" in sql
