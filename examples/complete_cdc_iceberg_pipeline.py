#!/usr/bin/env python3
"""
This example demonstrates a complete end-to-end pipeline:
1. PostgreSQL CDC source
2. Direct Iceberg sink from source tables

This follows the integration pattern shown in the RisingWave documentation.

Prerequisites for local development:
1. RisingWave running locally on port 4566
2. PostgreSQL running locally on port 5432 with:
   - Username: postgres
   - Password: postgres
   - Database: postgres
3. Create the /tmp/iceberg-warehouse directory for local Iceberg storage
"""

from risingwave_pipeline_sdk import (
    RisingWaveClient, PipelineBuilder, PostgreSQLConfig, IcebergConfig
)


def create_cdc_to_iceberg_pipeline():
    """Create a complete CDC to Iceberg data pipeline.

    This demonstrates a direct CDC to Iceberg pipeline:
    1. PostgreSQL CDC source
    2. Direct Iceberg sink from source tables
    """

    # Initialize RisingWave client
    client = RisingWaveClient(
        host="localhost",  # Local RisingWave instance
        port=4566,
        username="root",  # Default local username
        password="",  # No password for local development
        database="dev"
    )

    # Create pipeline builder
    builder = PipelineBuilder(client)

    print("Setting up PostgreSQL CDC to Iceberg Pipeline...")

    # 1. Configure PostgreSQL CDC source
    postgres_config = PostgreSQLConfig(
        hostname="localhost",  # Local PostgreSQL instance
        port=5432,
        username="postgres",  # Default local PostgreSQL username
        password="postgres",  # Default local PostgreSQL password
        database="postgres",
        schema_name="public",
        ssl_mode="disabled",  # Disable SSL for local development
        auto_schema_change=True,
    )

    # 2. Create CDC pipeline for user events
    print("Creating PostgreSQL CDC source...")
    source_tables = ["random_table_1", "dashboard"]
    cdc_result = builder.create_postgresql_pipeline(
        config=postgres_config,
        # Include non-existent table to test permissive behavior
        table_selector=source_tables,
        dry_run=True  # Enable dry run for testing without actual connections
    )

    # print("Available tables:", cdc_result['available_tables'])
    # print("Selected tables:", cdc_result['selected_tables'])
    print()
    print("CDC Source SQL:")
    for sql in cdc_result['sql_statements']:
        print(sql)
        print()

    # 3. Configure Iceberg sink to read directly from source tables
    iceberg_config = IcebergConfig(
        # sink_name not specified - will auto-generate based on database_name

        # Iceberg configuration
        warehouse_path="file:///tmp/iceberg-warehouse",  # Local file system warehouse
        database_name="pg_cdc",
        table_name="pg_dashboard",
        catalog_type="storage",  # Use storage catalog for local development
        catalog_name="pg_cdc",

        # Sink configuration - append-only for raw CDC data
        data_type="append-only",
        force_append_only=True,

        # Advanced features for critical analytics data
        # is_exactly_once=True,
        create_table_if_not_exists=True,

        # Local configuration - no S3 credentials needed
        # s3_region="us-east-1",
        # s3_access_key="YOUR_ACCESS_KEY",
        # s3_secret_key="YOUR_SECRET_KEY"
    )

    # 4. Create Iceberg sink directly from source tables
    print("Creating Iceberg sink...")
    sink_result = builder.create_sink(
        sink_config=iceberg_config,
        # Include non-existent table to test permissive behavior
        source_tables=source_tables,
        dry_run=True  # Enable dry run for testing without actual connections
    )

    for sql in sink_result['sql_statements']:
        print(sql)
        print()

    # Display CDC results
    if 'success_messages' in cdc_result:
        print("\nCDC Creation Results:")
        for message in cdc_result['success_messages']:
            print(message)
        print(f"\nCDC Success Summary: {cdc_result['success_summary']}")

    # Display sink results
    if 'success_messages' in sink_result:
        print("\nSink Creation Results:")
        for message in sink_result['success_messages']:
            print(message)
        print(f"\nSink Success Summary: {sink_result['success_summary']}")

    # Show any failures
    if 'failed_statements' in cdc_result and cdc_result['failed_statements']:
        print("\nCDC Failures:")
        for failure in cdc_result['failed_statements']:
            print(f"❌ {failure['error']}")

    if 'failed_results' in sink_result and sink_result['failed_results']:
        print("\nSink Failures:")
        for failure in sink_result['failed_results']:
            print(f"❌ Sink '{failure.sink_name}': {failure.error_message}")

    return {
        'cdc_result': cdc_result,
        'sink_result': sink_result
    }


def main():
    # Example Postgres CDC to Iceberg Pipeline
    print("\nPostgres CDC to Iceberg Pipeline")
    print("-" * 50)
    create_cdc_to_iceberg_pipeline()


if __name__ == "__main__":
    main()
