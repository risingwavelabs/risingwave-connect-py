"""
Example: PostgreSQL CDC Pipeline with S3 and PostgreSQL Sinks

This example demonstrates the complete flow:
1. Discover tables in PostgreSQL source
2. Create PostgreSQL CDC pipeline (source + tables)
3. Create S3 sinks for selected tables
4. Create PostgreSQL sinks for selected tables
"""

from risingwave_pipeline_sdk import (
    RisingWaveClient,
    PipelineBuilder,
    PostgreSQLConfig,
    TableSelector,
    S3Config,
    PostgreSQLSinkConfig
)


def main():
    # Connect to RisingWave
    rw_client = RisingWaveClient(
        host="localhost",
        port=4566,
        user="root",
        database="dev"
    )

    # Configure PostgreSQL CDC source
    pg_source_config = PostgreSQLConfig(
        source_name="ecommerce_cdc_source",
        hostname="localhost",
        port=5432,
        username="postgres",
        password="secret",
        database="ecommerce",
        schema_name="public",

        # CDC Configuration
        publication_name="rw_publication",
        publication_create_enable=True,
        auto_schema_change=True,
        ssl_mode="prefer",

        # Advanced backfill configuration
        backfill_num_rows_per_split="100000",
        backfill_parallelism="8",
        backfill_as_even_splits=True
    )

    # Create pipeline builder
    builder = PipelineBuilder(rw_client)

    print("🔍 Step 1: Discovering available tables...")

    # Discover all available tables
    available_tables = builder.discover_postgresql_tables(pg_source_config)
    print(f"Found {len(available_tables)} tables")

    # Create table selector - include specific tables only
    table_selector = TableSelector(
        specific_tables=["public.users", "public.orders"]
    )

    print("🚀 Step 2: Creating PostgreSQL CDC pipeline...")

    # Create complete CDC pipeline with table discovery and selection
    pipeline_result = builder.create_postgresql_pipeline(
        config=pg_source_config,
        table_selector=table_selector,
        dry_run=False  # Set to True to see SQL without executing
    )

    selected_tables = [
        table.qualified_name for table in pipeline_result['selected_tables']]
    print(f"✅ Created CDC pipeline with {len(selected_tables)} tables:")
    for table in selected_tables:
        print(f"  - {table}")

    print("\n📦 Step 3: Creating S3 sinks for data archival...")

    # Configure S3 sink
    s3_config = S3Config(
        sink_name="ecommerce_s3_archive",
        region_name="us-east-1",
        bucket_name="my-data-lake",
        path="ecommerce/cdc/",
        access_key_id="your-access-key",
        secret_access_key="your-secret-key",
        data_type="append-only",
        format_type="PLAIN",
        encode_type="PARQUET"
    )

    # Create S3 sinks for all selected tables
    s3_result = builder.create_s3_sink(
        s3_config=s3_config,
        source_tables=selected_tables,
        dry_run=True  # Show SQL without executing
    )

    print(f"✅ Generated {len(s3_result['sink_results'])} S3 sinks:")
    for result in s3_result['sink_results']:
        print(f"  - {result.sink_name} (from {result.source_table})")
        print(f"    SQL: {result.sql_statement[:100]}...")

    print("\n🐘 Step 4: Creating PostgreSQL sinks for real-time sync...")

    # Configure PostgreSQL sink (to different database)
    pg_sink_config = PostgreSQLSinkConfig(
        sink_name="ecommerce_realtime_sync",
        hostname="localhost",
        port=5433,
        username="analytics_user",
        password="analytics_password",
        database="analytics",
        postgres_schema="real_time",
        data_type="append-only",
        ssl_mode="prefer"
    )

    # Create PostgreSQL sinks with custom queries for analytics
    custom_queries = {
        "public.users": "SELECT id, name, email, created_at, updated_at FROM public.users WHERE active = true",
        "public.orders": "SELECT * FROM public.orders"
    }

    pg_sink_result = builder.create_postgresql_sink(
        pg_sink_config=pg_sink_config,
        source_tables=selected_tables,
        select_queries=custom_queries,
        dry_run=True  # Show SQL without executing
    )

    print(
        f"✅ Generated {len(pg_sink_result['sink_results'])} PostgreSQL sinks:")
    for result in pg_sink_result['sink_results']:
        print(f"  - {result.sink_name} (from {result.source_table})")
        print(f"    SQL: {result.sql_statement[:100]}...")

    print("\n" + "="*60)
    print("🎉 Complete Pipeline Created!")
    print("Architecture:")
    print("  PostgreSQL (Source) → RisingWave → S3 (Archive)")
    print("                               → PostgreSQL (Analytics)")
    print("\nWhat was created:")
    print(f"  ✅ 1 CDC Source: {pg_source_config.source_name}")
    print(
        f"  ✅ {len(selected_tables)} Tables: {', '.join([t.split('.')[-1] for t in selected_tables])}")
    print(f"  ✅ {len(s3_result['sink_results'])} S3 Sinks for archival")
    print(
        f"  ✅ {len(pg_sink_result['sink_results'])} PostgreSQL Sinks for analytics")


def show_generated_sql():
    """Show examples of generated SQL statements."""

    print("\n" + "="*60)
    print("📄 Example Generated SQL Statements")
    print("="*60)

    # S3 Sink Example
    print("\n🔸 S3 Sink SQL:")
    s3_config = S3Config(
        sink_name="users_s3_archive",
        region_name="us-east-1",
        bucket_name="my-data-lake",
        path="ecommerce/users/",
        access_key_id="AKIA...",
        secret_access_key="secret...",
    )

    from risingwave_pipeline_sdk.sinks.s3 import S3Sink
    s3_sink = S3Sink(s3_config)
    s3_sql = s3_sink.create_sink_sql("public.users")
    print(s3_sql)

    # PostgreSQL Sink Example
    print("\n🔸 PostgreSQL Sink SQL:")
    pg_config = PostgreSQLSinkConfig(
        sink_name="users_analytics_sync",
        hostname="localhost",
        username="analytics_user",
        password="password",
        database="analytics",
        postgres_schema="real_time"
    )

    from risingwave_pipeline_sdk.sinks.postgresql import PostgreSQLSink
    pg_sink = PostgreSQLSink(pg_config)
    pg_sql = pg_sink.create_sink_sql(
        "public.users",
        "SELECT id, name, email, created_at FROM public.users WHERE active = true"
    )
    print(pg_sql)


if __name__ == "__main__":
    try:
        main()
        show_generated_sql()
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nNote: This example requires RisingWave and PostgreSQL to be running.")
        print("Set dry_run=True to see generated SQL without executing.")
