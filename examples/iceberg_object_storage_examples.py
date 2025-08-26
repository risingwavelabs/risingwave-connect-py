"""
Examples demonstrating different object storage configurations for Iceberg sinks.

This module shows how to configure Iceberg sinks with various object storage backends:
- S3-compatible storage (AWS S3, MinIO)
- Google Cloud Storage (GCS)
- Azure Blob Storage
- Amazon S3 Tables

Based on: https://docs.risingwave.com/iceberg/byoi/object-storage
"""

from risingwave_pipeline_sdk.sinks.iceberg import IcebergConfig, IcebergSink


def example_s3_storage_sink():
    """Example: Iceberg sink with S3-compatible storage (AWS S3)."""
    print("=== S3 Storage Example ===")

    config = IcebergConfig(
        sink_name='s3_iceberg_sink',
        warehouse_path='s3://my-bucket/iceberg-warehouse',
        database_name='demo_db',
        table_name='events',
        catalog_type='storage',
        catalog_name='demo',

        # S3 configuration
        s3_region='us-east-1',
        s3_access_key='your-access-key',
        s3_secret_key='your-secret-key',

        # Table options
        data_type='append-only',
        force_append_only=True,
        create_table_if_not_exists=True
    )

    sink = IcebergSink(config)
    sql = sink.create_sink_sql('source_table')
    print(sql)
    return config


def example_s3_with_endpoint():
    """Example: Iceberg sink with S3-compatible storage using custom endpoint (MinIO)."""
    print("\n=== S3 with Custom Endpoint (MinIO) Example ===")

    config = IcebergConfig(
        sink_name='minio_iceberg_sink',
        warehouse_path='s3://my-bucket/iceberg-warehouse',
        database_name='demo_db',
        table_name='events',
        catalog_type='storage',
        catalog_name='demo',

        # MinIO configuration
        s3_endpoint='http://minio.example.com:9000',
        s3_access_key='minioadmin',
        s3_secret_key='minioadmin',
        s3_path_style_access=True,  # Required for MinIO

        # Table options
        data_type='append-only',
        create_table_if_not_exists=True
    )

    sink = IcebergSink(config)
    sql = sink.create_sink_sql('source_table')
    print(sql)
    return config


def example_s3_with_env_credentials():
    """Example: Iceberg sink with S3 using environment variables for credentials."""
    print("\n=== S3 with Environment Variables Example ===")

    config = IcebergConfig(
        sink_name='s3_env_iceberg_sink',
        warehouse_path='s3://my-bucket/iceberg-warehouse',
        database_name='demo_db',
        table_name='events',
        catalog_type='storage',
        catalog_name='demo',

        # Use environment variables for credentials
        s3_region='us-east-1',
        enable_config_load=True,  # Load credentials from environment

        # Table options
        data_type='append-only',
        create_table_if_not_exists=True
    )

    sink = IcebergSink(config)
    sql = sink.create_sink_sql('source_table')
    print(sql)
    return config


def example_gcs_storage_sink():
    """Example: Iceberg sink with Google Cloud Storage."""
    print("\n=== Google Cloud Storage Example ===")

    config = IcebergConfig(
        sink_name='gcs_iceberg_sink',
        warehouse_path='gs://my-bucket/iceberg-warehouse',
        database_name='demo_db',
        table_name='events',
        catalog_type='rest',
        catalog_uri='http://127.0.0.1:8181',
        catalog_name='demo',

        # GCS configuration
        gcs_credential='base64-encoded-service-account-key',

        # Table options
        data_type='append-only',
        force_append_only=True,
        create_table_if_not_exists=True
    )

    sink = IcebergSink(config)
    sql = sink.create_sink_sql('source_table')
    print(sql)
    return config


def example_azure_blob_storage_sink():
    """Example: Iceberg sink with Azure Blob Storage."""
    print("\n=== Azure Blob Storage Example ===")

    config = IcebergConfig(
        sink_name='azure_iceberg_sink',
        warehouse_path='azblob://my-container',
        database_name='demo_db',
        table_name='events',
        catalog_type='storage',
        catalog_name='demo',

        # Azure Blob Storage configuration
        azblob_account_name='mystorageaccount',
        azblob_account_key='storage-account-key',
        azblob_endpoint_url='https://mystorageaccount.blob.core.windows.net/',

        # Table options
        data_type='append-only',
        create_table_if_not_exists=True
    )

    sink = IcebergSink(config)
    sql = sink.create_sink_sql('source_table')
    print(sql)
    return config


def example_s3_tables_sink():
    """Example: Iceberg sink with Amazon S3 Tables."""
    print("\n=== Amazon S3 Tables Example ===")

    config = IcebergConfig(
        sink_name='s3_tables_iceberg_sink',
        warehouse_path='arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket',
        database_name='my_database',
        table_name='events',

        # S3 Tables requires REST catalog
        catalog_type='rest',
        catalog_uri='https://s3tables.us-east-1.amazonaws.com/iceberg',

        # S3 Tables specific configuration
        catalog_rest_signing_region='us-east-1',
        catalog_rest_sigv4_enabled=True,
        catalog_rest_signing_name='s3tables',

        # S3 credentials
        s3_access_key='your-aws-access-key-id',
        s3_secret_key='your-aws-secret-access-key',
        s3_region='us-east-1',

        # Table options
        data_type='upsert',
        primary_key='id',
        create_table_if_not_exists=True
    )

    sink = IcebergSink(config)
    sql = sink.create_sink_sql('source_table')
    print(sql)
    return config


def example_rest_catalog_with_credentials():
    """Example: Iceberg sink with REST catalog and credentials."""
    print("\n=== REST Catalog with Credentials Example ===")

    config = IcebergConfig(
        sink_name='rest_catalog_sink',
        warehouse_path='s3://my-bucket/iceberg-warehouse',
        database_name='demo_db',
        table_name='events',

        # REST catalog configuration
        catalog_type='rest',
        catalog_uri='http://catalog-service:8181',
        catalog_name='production',
        catalog_credential='base64-encoded-auth-token',

        # S3 configuration
        s3_region='us-west-2',
        s3_access_key='your-access-key',
        s3_secret_key='your-secret-key',

        # Table options
        data_type='append-only',
        create_table_if_not_exists=True,

        # Advanced features
        enable_compaction=True,
        compaction_interval_sec=1800,
        enable_snapshot_expiration=True
    )

    sink = IcebergSink(config)
    sql = sink.create_sink_sql('source_table')
    print(sql)
    return config


if __name__ == "__main__":
    """Run all examples to demonstrate different object storage configurations."""

    print("Iceberg Object Storage Configuration Examples")
    print("=" * 60)

    try:
        # S3 examples
        example_s3_storage_sink()
        example_s3_with_endpoint()
        example_s3_with_env_credentials()

        # Other cloud storage examples
        example_gcs_storage_sink()
        example_azure_blob_storage_sink()

        # Advanced examples
        example_s3_tables_sink()
        example_rest_catalog_with_credentials()

        print("\n✅ All examples generated successfully!")

    except Exception as e:
        print(f"\n❌ Error in examples: {e}")
        raise
