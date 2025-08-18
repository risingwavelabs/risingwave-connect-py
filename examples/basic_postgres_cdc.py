"""
Example: Basic PostgreSQL CDC Pipeline

This example shows how to create a simple PostgreSQL CDC pipeline
using the modern RisingWave Pipeline SDK API.
"""

from risingwave_pipeline_sdk import (
    RisingWaveClient,
    PipelineBuilder,
    PostgreSQLConfig,
    TableSelector
)


def main():
    # Connect to RisingWave
    client = RisingWaveClient(
        host="localhost",
        port=4566,
        user="root",
        database="dev"
    )

    # Configure PostgreSQL CDC source
    pg_config = PostgreSQLConfig(
        source_name="ecommerce_cdc",
        hostname="localhost",
        port=5432,
        username="postgres",
        password="secret",
        database="ecommerce",
        schema_name="public",
        publication_name="rw_publication",
        slot_name="ecommerce_slot",
        auto_schema_change=True
    )

    # Create pipeline builder
    builder = PipelineBuilder(client)

    # Create table selector for specific tables
    table_selector = TableSelector(
        specific_tables=["public.users", "public.orders"]
    )

    # Create the complete pipeline
    result = builder.create_postgresql_pipeline(
        config=pg_config,
        table_selector=table_selector,
        dry_run=False
    )

    print(f"Created source: {pg_config.source_name}")
    print(f"Created tables: {len(result['selected_tables'])}")
    for table in result['selected_tables']:
        print(f"  - {table.qualified_name}")
    print("Pipeline setup complete!")


if __name__ == "__main__":
    main()
