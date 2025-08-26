#!/usr/bin/env python3
"""
Example: Iceberg Sink with RisingWave Pipeline SDK

This example demonstrates how to create an Iceberg sink using the RisingWave Pipeline SDK.
"""

from risingwave_pipeline_sdk import RisingWaveClient, PipelineBuilder, IcebergConfig


def main():
    # Initialize RisingWave client
    client = RisingWaveClient(
        host="localhost",
        port=4566,
        username="root",
        database="dev"
    )

    # Create pipeline builder
    builder = PipelineBuilder(client)

    # Configure Iceberg sink that matches the SQL example
    iceberg_config = IcebergConfig(
        sink_name="sink2",
        # Basic Iceberg configuration
        warehouse_path="s3a://yuxuan-iceberg-test/demo",
        database_name="demo_db",
        table_name="t2",
        catalog_type="storage",
        catalog_name="demo",

        # Sink behavior
        data_type="append-only",
        force_append_only=True,
        create_table_if_not_exists=True,

        # S3 configuration
        s3_region="us-east-1",
        s3_endpoint="https://s3.us-east-1.amazonaws.com",
        s3_access_key="your-aws-access-key-id",
        s3_secret_key="your-aws-secret-access-key"
    )

    # Create the sink from a materialized view with custom query
    result = builder.create_sink(
        sink_config=iceberg_config,
        source_tables=["mv1"],  # Source materialized view
        select_queries={"mv1": "SELECT * FROM mv1"},  # Custom SELECT query
        dry_run=True  # Set to False to actually execute
    )

    print("Iceberg Sink Creation Result:")
    print("=" * 50)
    for table, details in result.items():
        print(f"Table: {table}")
        print(f"SQL Statement:")
        print(details['sql'])
        print()


def example_storage_catalog():
    """Example using storage catalog (no external catalog service)."""

    iceberg_config = IcebergConfig(
        sink_name="events_sink",
        warehouse_path="s3://my-data-lake/warehouse",
        database_name="analytics",
        table_name="processed_events",
        catalog_type="storage",

        # Sink configuration
        data_type="append-only",

        # S3 configuration
        s3_region="us-west-2",
        s3_access_key="your-access-key",
        s3_secret_key="your-secret-key"
    )

    return iceberg_config


def example_glue_catalog():
    """Example using AWS Glue catalog."""

    iceberg_config = IcebergConfig(
        sink_name="glue_sink",
        warehouse_path="s3://my-bucket/warehouse",
        database_name="my_database",
        table_name="my_table",
        catalog_type="glue",
        catalog_name="my_catalog",

        # S3 configuration
        s3_region="us-west-2",
        s3_access_key="your-access-key",
        s3_secret_key="your-secret-key"
    )

    return iceberg_config


def example_rest_catalog():
    """Example using REST catalog (including AWS S3 Tables)."""

    iceberg_config = IcebergConfig(
        sink_name="rest_sink",
        warehouse_path="s3://my-bucket/warehouse",
        database_name="my_database",
        table_name="my_table",
        catalog_type="rest",
        catalog_uri="http://rest-catalog:8181",
        catalog_credential="username:password",

        # Sink configuration
        data_type="upsert",
        primary_key="id",

        # S3 configuration
        s3_access_key="your-access-key",
        s3_secret_key="your-secret-key"
    )

    return iceberg_config


def example_s3_tables():
    """Example using AWS S3 Tables (managed Iceberg service)."""

    iceberg_config = IcebergConfig(
        sink_name="s3_tables_sink",
        warehouse_path="s3://my-bucket/my-table-bucket/",
        database_name="analytics",
        table_name="user_metrics",
        catalog_type="rest",
        catalog_uri="https://s3tables.us-east-1.amazonaws.com/tables",

        # S3 Tables specific configuration
        catalog_rest_signing_region="us-east-1",
        catalog_rest_signing_name="s3tables",
        catalog_rest_sigv4_enabled=True,

        # Sink configuration
        data_type="upsert",
        primary_key="id",

        # S3 configuration
        s3_region="us-east-1"
    )

    return iceberg_config


def example_jdbc_catalog():
    """Example using JDBC catalog."""

    iceberg_config = IcebergConfig(
        sink_name="jdbc_sink",
        warehouse_path="s3://my-bucket/warehouse",
        database_name="my_database",
        table_name="my_table",
        catalog_type="jdbc",
        catalog_uri="jdbc:postgresql://postgres:5432/catalog",
        catalog_jdbc_user="catalog_user",
        catalog_jdbc_password="catalog_password",

        # S3 configuration
        s3_access_key="your-access-key",
        s3_secret_key="your-secret-key"
    )

    return iceberg_config


def example_advanced_features():
    """Example with advanced features enabled."""

    iceberg_config = IcebergConfig(
        sink_name="advanced_sink",
        warehouse_path="s3://data-lake/warehouse",
        database_name="critical",
        table_name="events",
        catalog_type="glue",
        catalog_name="my_catalog",

        # Advanced sink features
        data_type="upsert",
        primary_key="event_id",
        is_exactly_once=True,
        commit_checkpoint_interval=10,
        commit_retry_num=5,

        # Compaction features (Premium)
        enable_compaction=True,
        compaction_interval_sec=3600,
        enable_snapshot_expiration=True,

        # S3 configuration
        s3_region="us-west-2",
        s3_access_key="your-access-key",
        s3_secret_key="your-secret-key"
    )

    return iceberg_config


def example_gcs():
    """Example using Google Cloud Storage."""

    iceberg_config = IcebergConfig(
        sink_name="gcs_sink",
        warehouse_path="gs://my-bucket/warehouse",
        database_name="analytics",
        table_name="events",
        catalog_type="rest",
        catalog_uri="http://catalog-service:8181",

        # GCS configuration
        gcs_credential="base64-encoded-credential"
    )

    return iceberg_config


def example_azure():
    """Example using Azure Blob Storage."""

    iceberg_config = IcebergConfig(
        sink_name="azure_sink",
        warehouse_path="abfss://container@account.dfs.core.windows.net/warehouse",
        database_name="analytics",
        table_name="events",
        catalog_type="rest",
        catalog_uri="http://catalog-service:8181",

        # Azure configuration
        azblob_account_name="your_account",
        azblob_account_key="your_key"
    )

    return iceberg_config


if __name__ == "__main__":
    print("RisingWave Iceberg Sink Examples")
    print("=" * 40)

    # Run the main example
    main()

    # Show additional configuration examples
    print("\nAdditional Configuration Examples:")
    print("=" * 40)

    configs = [
        ("Storage Catalog", example_storage_catalog()),
        ("AWS Glue Catalog", example_glue_catalog()),
        ("REST Catalog", example_rest_catalog()),
        ("AWS S3 Tables", example_s3_tables()),
        ("JDBC Catalog", example_jdbc_catalog()),
        ("Advanced Features", example_advanced_features()),
        ("Google Cloud Storage", example_gcs()),
        ("Azure Blob Storage", example_azure())
    ]

    for name, config in configs:
        print(f"\n{name}:")
        print(f"  Catalog Type: {config.catalog_type}")
        print(f"  Data Type: {config.data_type}")
        print(f"  Warehouse Path: {config.warehouse_path}")
