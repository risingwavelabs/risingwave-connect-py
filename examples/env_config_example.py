"""Environment-based configuration example."""

import os
from risingwave_connect import (
    RisingWaveClient,
    ConnectBuilder,
    PostgreSQLConfig,
    TableSelector
)


def main():
    # Configuration via environment variables

    client = RisingWaveClient(
        host=os.getenv("RW_HOST", "localhost"),
        port=int(os.getenv("RW_PORT", "4566")),
        user=os.getenv("RW_USER", "root"),
        password=os.getenv("RW_PASSWORD"),
        database=os.getenv("RW_DATABASE", "dev")
    )

    # PostgreSQL configuration from environment
    pg_config = PostgreSQLConfig(
        source_name=os.getenv("SOURCE_NAME", "postgres_cdc"),
        hostname=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        username=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
        database=os.getenv("PG_DATABASE", "mydb"),
        schema_name=os.getenv("PG_SCHEMA", "public"),
        ssl_mode=os.getenv("PG_SSL_MODE", "disabled"),
        slot_name=os.getenv("CDC_SLOT_NAME"),
        publication_name=os.getenv("CDC_PUBLICATION", "rw_publication"),
        auto_schema_change=os.getenv(
            "CDC_AUTO_SCHEMA", "false").lower() == "true"
    )

    # Create connect builder
    builder = ConnectBuilder(client)

    # Table selection from environment
    table_names = os.getenv("TABLE_NAMES", "").split(
        ",") if os.getenv("TABLE_NAMES") else []
    table_patterns = os.getenv("TABLE_PATTERNS", "").split(
        ",") if os.getenv("TABLE_PATTERNS") else []

    if table_names:
        table_selector = TableSelector(specific_tables=table_names)
    elif table_patterns:
        table_selector = TableSelector(include_patterns=table_patterns)
    else:
        table_selector = TableSelector(include_all=True)

    # Create connection
    result = builder.create_postgresql_pipeline(
        config=pg_config,
        table_selector=table_selector,
        dry_run=os.getenv("DRY_RUN", "false").lower() == "true"
    )

    print(f"Created source: {pg_config.source_name}")
    print(f"Selected tables: {len(result['selected_tables'])}")
    for table in result['selected_tables']:
        print(f"  - {table.qualified_name}")
    print("Environment-based connection setup complete!")


if __name__ == "__main__":
    main()
