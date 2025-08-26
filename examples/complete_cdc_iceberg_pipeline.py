#!/usr/bin/env python3
"""
Example: Complete CDC to Iceberg Pipeline

This example demonstrates a complete end-to-end pipeline:
1. PostgreSQL CDC source
2. Data transformation/aggregation
3. Iceberg sink for analytics

This follows the integration pattern shown in the RisingWave documentation.
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
        database="dev"
    )

    # Create pipeline builder
    builder = PipelineBuilder(client)

    print("Setting up CDC to Iceberg Pipeline...")

    # 1. Configure PostgreSQL CDC source
    postgres_config = PostgreSQLConfig(
        hostname="postgres",
        port=5432,
        username="user",
        password="password",
        database_name="app_db",
        schema_name="public",
        publication_name="my_publication",
        slot_name="my_slot"
    )

    # 2. Create CDC pipeline for user events
    print("Creating PostgreSQL CDC source...")
    cdc_result = builder.create_cdc_pipeline(
        source_name="user_events_cdc",
        config=postgres_config,
        table_names=["user_events", "users"],
        dry_run=True
    )

    print("CDC Source SQL:")
    for sql in cdc_result['sql_statements']:
        print(sql)
        print()

    # 3. Create materialized view for real-time analytics
    print("Creating analytics materialized view...")
    analytics_mv_sql = """
    CREATE MATERIALIZED VIEW hourly_user_metrics AS
    SELECT 
        ue.user_id,
        u.username,
        u.email,
        date_trunc('hour', ue.event_timestamp) as hour,
        COUNT(*) as event_count,
        COUNT(DISTINCT ue.session_id) as session_count,
        SUM(CASE WHEN ue.event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count
    FROM user_events ue
    JOIN users u ON ue.user_id = u.user_id
    GROUP BY ue.user_id, u.username, u.email, date_trunc('hour', ue.event_timestamp);
    """

    print(analytics_mv_sql)
    print()

    # 4. Configure Iceberg sink for analytics
    iceberg_config = IcebergConfig(
        sink_name="analytics_sink",

        # Iceberg configuration
        warehouse_path="s3://analytics-lake/warehouse",
        database_name="metrics",
        table_name="hourly_user_metrics",
        catalog_type="glue",
        catalog_name="analytics_catalog",

        # Sink configuration - upsert for incremental updates
        data_type="upsert",
        primary_key="user_id,hour",

        # Advanced features for critical analytics data
        is_exactly_once=True,
        create_table_if_not_exists=True,

        # S3 configuration
        s3_region="us-west-2",
        s3_access_key="your-access-key",
        s3_secret_key="your-secret-key"
    )

    # 5. Create Iceberg sink from the materialized view
    print("Creating Iceberg sink...")
    sink_result = builder.create_sink(
        sink_config=iceberg_config,
        source_tables=["hourly_user_metrics"],
        dry_run=True
    )

    print("Iceberg Sink SQL:")
    for table, details in sink_result.items():
        print(details['sql'])
        print()

    return {
        'cdc_result': cdc_result,
        'analytics_mv_sql': analytics_mv_sql,
        'sink_result': sink_result
    }


def create_raw_data_iceberg_pipeline():
    """Create a pipeline that streams raw CDC data to Iceberg for data lake."""

    # Initialize RisingWave client
    client = RisingWaveClient(
        host="localhost",
        port=4566,
        username="root",
        database="dev"
    )

    builder = PipelineBuilder(client)

    print("Setting up Raw Data to Iceberg Pipeline...")

    # Configure Iceberg sink for raw data (append-only)
    raw_data_iceberg = IcebergConfig(
        sink_name="raw_users_sink",

        # Iceberg configuration
        warehouse_path="s3://data-lake/warehouse",
        database_name="raw",
        table_name="users",
        catalog_type="glue",
        catalog_name="data_lake_catalog",

        # Sink configuration - append-only for raw data
        data_type="append-only",
        force_append_only=True,  # Convert CDC updates to inserts
        create_table_if_not_exists=True,

        # S3 configuration
        s3_region="us-west-2",
        s3_access_key="your-access-key",
        s3_secret_key="your-secret-key"
    )

    # Create sink for raw user data
    sink_result = builder.create_sink(
        sink_config=raw_data_iceberg,
        source_tables=["users"],  # Assuming users table from CDC
        dry_run=True
    )

    print("Raw Data Iceberg Sink SQL:")
    for table, details in sink_result.items():
        print(details['sql'])
        print()

    return sink_result


def create_multi_table_iceberg_pipeline():
    """Create a pipeline that sinks multiple tables to Iceberg."""

    client = RisingWaveClient(
        host="localhost",
        port=4566,
        username="root",
        database="dev"
    )

    builder = PipelineBuilder(client)

    print("Setting up Multi-Table Iceberg Pipeline...")

    # Configure Iceberg sink for multiple tables
    multi_table_config = IcebergConfig(
        sink_name="ecommerce_data_lake",

        # Iceberg configuration
        warehouse_path="s3://ecommerce-lake/warehouse",
        database_name="ecommerce",
        table_name="",  # Will be set per table
        catalog_type="storage",  # Using storage catalog for simplicity

        # Sink configuration
        data_type="append-only",
        force_append_only=True,
        create_table_if_not_exists=True,

        # S3 configuration
        s3_region="us-east-1",
        s3_endpoint="https://s3.us-east-1.amazonaws.com",
        s3_access_key="your-access-key",
        s3_secret_key="your-secret-key"
    )

    # Sink multiple tables
    tables_to_sink = ["users", "orders", "products", "order_items"]

    # Update table name for each table and create sinks
    for table in tables_to_sink:
        table_config = IcebergConfig(**multi_table_config.dict())
        table_config.table_name = table
        table_config.sink_name = f"iceberg_{table}_sink"

        sink_result = builder.create_sink(
            sink_config=table_config,
            source_tables=[table],
            dry_run=True
        )

        print(f"Iceberg Sink for {table}:")
        for _, details in sink_result.items():
            print(details['sql'])
            print()


def main():
    """Run all pipeline examples."""

    print("RisingWave CDC to Iceberg Pipeline Examples")
    print("=" * 50)

    # Example 1: Complete analytics pipeline
    print("\n1. Complete Analytics Pipeline (CDC → MV → Iceberg)")
    print("-" * 50)
    create_cdc_to_iceberg_pipeline()

    # Example 2: Raw data pipeline
    print("\n2. Raw Data Pipeline (CDC → Iceberg)")
    print("-" * 50)
    create_raw_data_iceberg_pipeline()

    # Example 3: Multi-table pipeline
    print("\n3. Multi-Table Pipeline")
    print("-" * 50)
    create_multi_table_iceberg_pipeline()


if __name__ == "__main__":
    main()
