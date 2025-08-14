"""
Example: Basic PostgreSQL CDC Pipeline

This example shows how to create a simple PostgreSQL CDC pipeline
using the RisingWave Pipeline SDK.
"""

from risingwave_pipeline_sdk import RisingWaveClient, PostgresCDCPipeline

def main():
    # Connect to RisingWave
    client = RisingWaveClient(
        host="localhost",
        port=4566,
        user="root",
        database="dev"
    )
    
    # Create a PostgreSQL CDC pipeline
    pipeline = PostgresCDCPipeline(
        client=client,
        postgres_host="localhost",
        postgres_port=5432,
        postgres_user="postgres",
        postgres_password="secret",
        postgres_database="ecommerce",
        postgres_schema="public"
    )
    
    # Create a CDC source
    source = pipeline.create_source(
        name="ecommerce_cdc",
        publication_name="rw_publication",
        slot_name="ecommerce_slot",
        auto_schema_change=True
    )
    
    print(f"Created source: {source.qualified_name}")
    
    # Create tables from the source
    users_table = pipeline.create_table(
        name="users",
        source=source,
        postgres_table="public.users"
    )
    
    orders_table = pipeline.create_table(
        name="orders", 
        source=source,
        postgres_table="public.orders",
        include_timestamp_as="cdc_timestamp"
    )
    
    print(f"Created tables: {users_table.qualified_name}, {orders_table.qualified_name}")
    print("Pipeline setup complete!")

if __name__ == "__main__":
    main()
