#!/usr/bin/env python3
"""
Example: Complete Sink Name Management

This example demonstrates all sink name scenarios:
1. Auto-generated sink names (when not provided)
2. Custom sink names (when explicitly provided) 
3. Multiple sinks with auto-generation
"""

from risingwave_pipeline_sdk import (
    RisingWaveClient, PipelineBuilder, PostgreSQLConfig, IcebergConfig
)


def demo_complete_sink_management():
    """Comprehensive demonstration of sink name management."""

    print("Complete Sink Name Management Demo")
    print("=" * 50)

    # Initialize RisingWave client
    client = RisingWaveClient(
        host="test-risingwave",
        port=4566,
        username="demo_user",
        password="demo_pass",
        database="demo_db"
    )

    builder = PipelineBuilder(client)

    # Set up a simple CDC source first
    postgres_config = PostgreSQLConfig(
        hostname="postgres-server",
        port=5432,
        username="pg_user",
        password="pg_pass",
        database="source_db",
        schema_name="public",
        ssl_mode="required",
        auto_schema_change=True
    )

    print("\n🔄 Setting up CDC source...")
    cdc_result = builder.create_postgresql_pipeline(
        config=postgres_config,
        dry_run=True
    )
    print(f"CDC source created: {postgres_config.source_name}")

    # Example 1: Auto-generated sink names
    print("\n📊 1. Auto-Generated Sink Names")
    print("-" * 40)

    # Analytics sink - no sink_name provided
    analytics_config = IcebergConfig(
        warehouse_path="s3://company-datalake/analytics",
        database_name="customer_analytics",
        table_name="user_behavior",
        catalog_type="glue",
        catalog_name="analytics_catalog",
        data_type="append-only",
        s3_region="us-east-1",
        s3_access_key="analytics-key",
        s3_secret_key="analytics-secret"
    )

    # Reporting sink - no sink_name provided
    reporting_config = IcebergConfig(
        warehouse_path="s3://company-datalake/reports",
        database_name="business_metrics",
        table_name="daily_summaries",
        catalog_type="glue",
        catalog_name="reports_catalog",
        data_type="append-only",
        s3_region="us-east-1",
        s3_access_key="reports-key",
        s3_secret_key="reports-secret"
    )

    # Create sinks
    analytics_result = builder.create_sink(
        sink_config=analytics_config,
        source_tables=["user_events"],
        dry_run=True
    )

    reporting_result = builder.create_sink(
        sink_config=reporting_config,
        source_tables=["user_events"],
        dry_run=True
    )

    print(f"✅ Analytics sink auto-generated: {analytics_config.sink_name}")
    print(f"✅ Reporting sink auto-generated: {reporting_config.sink_name}")

    # Example 2: Custom sink names
    print("\n🎯 2. Custom Sink Names")
    print("-" * 25)

    # Custom named sink
    custom_config = IcebergConfig(
        sink_name="critical_business_metrics_sink",  # Explicit custom name
        warehouse_path="s3://company-datalake/critical",
        database_name="finance",
        table_name="transactions",
        catalog_type="glue",
        catalog_name="finance_catalog",
        data_type="append-only",
        s3_region="us-east-1",
        s3_access_key="finance-key",
        s3_secret_key="finance-secret"
    )

    custom_result = builder.create_sink(
        sink_config=custom_config,
        source_tables=["transactions"],
        dry_run=True
    )

    print(f"✅ Custom sink name used: {custom_config.sink_name}")

    # Example 3: Multiple tables to same sink type
    print("\n🔀 3. Multiple Tables with Auto-Generation")
    print("-" * 45)

    # Create sinks for different tables with auto-generated names
    tables = ["users", "orders", "products"]
    sink_configs = []

    for table in tables:
        config = IcebergConfig(
            warehouse_path=f"s3://company-datalake/{table}",
            database_name=f"{table}_warehouse",
            table_name=table,
            catalog_type="glue",
            catalog_name=f"{table}_catalog",
            data_type="append-only",
            s3_region="us-east-1",
            s3_access_key="multi-key",
            s3_secret_key="multi-secret"
        )

        result = builder.create_sink(
            sink_config=config,
            source_tables=[table],
            dry_run=True
        )

        sink_configs.append(config)
        print(f"✅ {table.capitalize()} sink: {config.sink_name}")

    # Show generated SQL snippets
    print("\n📋 4. Generated SQL Examples")
    print("-" * 30)

    print("Auto-generated sink SQL:")
    for result in analytics_result['sink_results']:
        lines = result.sql_statement.split('\n')
        print(f"  {lines[0]}")  # Just show the CREATE SINK line

    print("\nCustom named sink SQL:")
    for result in custom_result['sink_results']:
        lines = result.sql_statement.split('\n')
        print(f"  {lines[0]}")  # Just show the CREATE SINK line

    print("\n✨ Summary")
    print("-" * 15)
    print("✅ Sink names are completely optional")
    print(
        "✅ Auto-generated names follow pattern: {sink_type}_{database}_{cleaned}_sink")
    print("✅ Custom names take precedence when provided")
    print("✅ Special characters in database names are sanitized")
    print("✅ Each sink gets a unique, descriptive name")


if __name__ == "__main__":
    demo_complete_sink_management()
