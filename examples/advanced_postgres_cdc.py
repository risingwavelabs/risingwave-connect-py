"""
Example: Advanced PostgreSQL CDC Configuration

This example demonstrates advanced configuration options including
SSL, Debezium properties, and multiple table creation.
"""

from risingwave_pipeline_sdk import RisingWaveClient, PostgresCDCPipeline, PostgresCDCConfig


def main():
    client = RisingWaveClient(
        "postgresql://oauth_default:@rwc-g1j2i9rs24f0aort385vpie7b0-my-project-0813.test-useast1-eks-a.risingwave-cloud.xyz: 4566/dev?sslmode=require")

    config = PostgresCDCConfig(
        postgres_host="postgres.example.com",
        postgres_port=5432,
        postgres_user="replication_user",
        postgres_password="secure_password",
        postgres_database="production_db",
        postgres_schema="public",

        # CDC settings
        slot_name="production_slot",
        publication_name="rw_publication",
        publication_create_enable=True,
        auto_schema_change=True,

        # SSL configuration
        ssl_mode="require",
        ssl_root_cert="/path/to/ca.pem",

        # Advanced Debezium properties
        debezium_properties={
            "schema.history.internal.skip.unparseable.ddl": "true",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "decimal.handling.mode": "string"
        },
    )

    # Create pipeline with advanced config
    pipeline = PostgresCDCPipeline(client=client, config=config)

    # Create complete pipeline with multiple tables
    table_mappings = {
        "customers": "public.customers",
        "orders": "public.orders",
        "order_items": "public.order_items",
        "products": "public.products",
        "inventory": "warehouse.inventory"
    }

    source, tables = pipeline.create_pipeline(
        source_name="production_cdc",
        tables=table_mappings,
        source_schema="public",
        table_schema="staging"
    )

    print(f"Created source: {source.qualified_name}")
    print("Created tables:")
    for table in tables:
        print(f"  - {table.qualified_name}")

    print("Advanced pipeline setup complete!")


if __name__ == "__main__":
    main()
