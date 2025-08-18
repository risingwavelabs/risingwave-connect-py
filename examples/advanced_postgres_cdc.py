"""
Example: Advanced PostgreSQL CDC Configuration

This example demonstrates advanced configuration options including
SSL, custom properties, and pattern-based table selection.
"""

from risingwave_pipeline_sdk import (
    RisingWaveClient,
    PipelineBuilder,
    PostgreSQLConfig,
    TableSelector
)


def main():
    client = RisingWaveClient(
        host="localhost",
        port=4566,
        user="root",
        database="dev"
    )

    # Advanced PostgreSQL configuration
    pg_config = PostgreSQLConfig(
        source_name="production_cdc",
        hostname="localhost",
        port=5432,
        username="postgres",
        password="secret",
        database="mydb",
        schema_name="public",

        # CDC settings
        slot_name="production_slot",
        publication_name="rw_publication",
        publication_create_enable=True,
        auto_schema_change=True,

        # SSL configuration
        ssl_mode="prefer",

        # Advanced backfill configuration
        backfill_num_rows_per_split="100000",
        backfill_parallelism="8",
        backfill_as_even_splits=True,

        # Advanced Debezium properties
        debezium_properties={
            "schema.history.internal.skip.unparseable.ddl": "true",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "decimal.handling.mode": "string"
        },
    )

    # Create pipeline builder
    builder = PipelineBuilder(client)

    # Pattern-based table selection
    table_selector = TableSelector(
        include_patterns=["customer*", "order*", "product*", "inventory"],
        exclude_patterns=["*_temp", "*_backup"]
    )

    # Create complete pipeline with advanced configuration
    result = builder.create_postgresql_pipeline(
        config=pg_config,
        table_selector=table_selector,
        dry_run=False
    )

    print(f"Created source: {pg_config.source_name}")
    print(f"Selected tables: {len(result['selected_tables'])}")
    for table in result['selected_tables']:
        print(f"  - {table.qualified_name}")

    print("Advanced pipeline setup complete!")


if __name__ == "__main__":
    main()
