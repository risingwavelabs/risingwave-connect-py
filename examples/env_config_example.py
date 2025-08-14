"""
Example: Environment-based Configuration

This example shows how to configure the pipeline using environment variables,
which is useful for different deployment environments.
"""

import os
from risingwave_pipeline_sdk import RisingWaveClient, PostgresCDCPipeline

def main():
    # Configuration via environment variables
    # RW_HOST, RW_PORT, RW_USER, RW_DATABASE
    # PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE
    
    client = RisingWaveClient(
        host=os.getenv("RW_HOST", "localhost"),
        port=int(os.getenv("RW_PORT", "4566")),
        user=os.getenv("RW_USER", "root"),
        password=os.getenv("RW_PASSWORD"),
        database=os.getenv("RW_DATABASE", "dev")
    )
    
    pipeline = PostgresCDCPipeline(
        client=client,
        postgres_host=os.getenv("PG_HOST", "localhost"),
        postgres_port=int(os.getenv("PG_PORT", "5432")),
        postgres_user=os.getenv("PG_USER", "postgres"),
        postgres_password=os.getenv("PG_PASSWORD", ""),
        postgres_database=os.getenv("PG_DATABASE", "mydb"),
        postgres_schema=os.getenv("PG_SCHEMA", "public"),
        
        # CDC-specific env vars
        slot_name=os.getenv("CDC_SLOT_NAME"),
        publication_name=os.getenv("CDC_PUBLICATION", "rw_publication"),
        auto_schema_change=os.getenv("CDC_AUTO_SCHEMA", "false").lower() == "true"
    )
    
    # Create source
    source = pipeline.create_source(
        name=os.getenv("SOURCE_NAME", "postgres_cdc"),
        schema=os.getenv("RW_SCHEMA", "public")
    )
    
    # Create table if specified
    table_name = os.getenv("TABLE_NAME")
    pg_table = os.getenv("PG_TABLE")
    
    if table_name and pg_table:
        table = pipeline.create_table(
            name=table_name,
            source=source,
            postgres_table=pg_table,
            schema=os.getenv("RW_SCHEMA", "public")
        )
        print(f"Created table: {table.qualified_name}")
    
    print(f"Created source: {source.qualified_name}")
    print("Environment-based pipeline setup complete!")

if __name__ == "__main__":
    main()
