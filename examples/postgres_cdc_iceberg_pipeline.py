#!/usr/bin/env python3
"""
This example demonstrates a complete end-to-end pipeline:
1. PostgreSQL CDC source
2. Direct Iceberg sink from source tables

This follows the integration pattern shown in the RisingWave documentation.

Prerequisites:
1. A running RisingWave instance (either in the cloud or locally)
2. PostgreSQL configured for CDC with:
    - Username: postgres
    - Password: postgres
    - Database: postgres
3. S3-compatible storage for Iceberg sink tables, or a local directory for testing
"""

from risingwave_pipeline_sdk import (
    RisingWaveClient, PipelineBuilder, PostgreSQLConfig, IcebergConfig
)


def create_cdc_to_iceberg_pipeline():
    """Create a complete CDC to Iceberg data pipeline."""

    # Initialize RisingWave client
    client = RisingWaveClient(
        host="localhost",
        port=4566,
        username="root",
        password="",
        database="dev"
    )

    # Create pipeline builder
    builder = PipelineBuilder(client)

    print("Setting up PostgreSQL CDC to Iceberg Pipeline...")

    # Configure PostgreSQL CDC source
    postgres_config = PostgreSQLConfig(
        hostname="localhost",
        port=5432,
        username="postgres",
        password="postgres",
        database="postgres",
        schema_name="public",
        ssl_mode="disabled",
        auto_schema_change=True,
    )

    # Create CDC pipeline
    print("Creating PostgreSQL CDC source...")
    source_tables = ["random_table_1", "dashboard"]
    cdc_result = builder.create_postgresql_pipeline(
        config=postgres_config,
        table_selector=source_tables,
        dry_run=True
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
        warehouse_path="file:///tmp/iceberg-warehouse",
        database_name="pg_cdc",
        table_name="pg_dashboard",
        catalog_type="storage",
        catalog_name="pg_cdc",

        # Sink configuration
        data_type="append-only",  # upsert
        force_append_only=True,

        # Advanced features for critical analytics data
        # is_exactly_once=True,
        create_table_if_not_exists=True,

        # S3 credentials
        s3_region="us-east-1",
        s3_access_key="YOUR_ACCESS_KEY",
        s3_secret_key="YOUR_SECRET_KEY"
    )

    # Create Iceberg sink
    print("Creating Iceberg sink...")
    sink_result = builder.create_sink(
        sink_config=iceberg_config,
        source_tables=source_tables,
        dry_run=True
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
    print("\nPostgres CDC to Iceberg Pipeline")
    print("-" * 50)
    create_cdc_to_iceberg_pipeline()


if __name__ == "__main__":
    main()
