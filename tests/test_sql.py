import pytest
from risingwave_pipeline_sdk.sql_builders import build_create_source_postgres_cdc, build_create_table_from_source
from risingwave_pipeline_sdk.client import RisingWaveClient
from risingwave_pipeline_sdk.pipelines.postgres_cdc import PostgresCDCPipeline, PostgresCDCConfig


def test_build_create_source_minimal():
    """Test basic CREATE SOURCE SQL generation."""
    sql = build_create_source_postgres_cdc(
        "pg_source",
        hostname="localhost",
        port=5432,
        username="u",
        password="p",
        database="d",
    )
    assert "CREATE SOURCE pg_source" in sql
    assert "connector='postgres-cdc'" in sql
    assert "hostname='localhost'" in sql
    assert "database.name='d'" in sql


def test_build_create_table_wildcard():
    """Test CREATE TABLE with wildcard schema mapping."""
    sql = build_create_table_from_source(
        table_name="t",
        source_name="pg_source",
        pg_table="public.foo",
    )
    assert "CREATE TABLE t" in sql
    assert "FROM pg_source TABLE 'public.foo'" in sql
    assert "(*)" in sql or "*" in sql


def test_postgres_cdc_config():
    """Test PostgresCDCConfig model validation."""
    config = PostgresCDCConfig(
        postgres_host="localhost",
        postgres_database="testdb",
        postgres_user="testuser",
        publication_name="test_pub",
        auto_schema_change=True,
    )
    assert config.postgres_host == "localhost"
    assert config.postgres_database == "testdb"
    assert config.auto_schema_change is True
    assert config.publication_name == "test_pub"


def test_postgres_cdc_config_with_debezium():
    """Test PostgresCDCConfig with Debezium properties."""
    config = PostgresCDCConfig(
        postgres_host="localhost",
        postgres_database="testdb",
        debezium_properties={
            "schema.history.internal.skip.unparseable.ddl": "true",
            "transforms": "unwrap",
        }
    )
    assert "schema.history.internal.skip.unparseable.ddl" in config.debezium_properties
    assert config.debezium_properties["transforms"] == "unwrap"


def test_risingwave_client_dsn_parsing():
    """Test RisingWave client configuration."""
    # Test with explicit parameters
    client = RisingWaveClient(
        host="localhost",
        port=4566,
        user="root",
        database="dev"
    )
    assert client.config.host == "localhost"
    assert client.config.port == 4566
    assert client.config.user == "root"
    assert client.config.database == "dev"
    
    # Test with DSN
    client_dsn = RisingWaveClient("postgresql://root@localhost:4566/dev")
    # The DSN should be stored properly
    assert "postgresql://" in client_dsn._dsn
