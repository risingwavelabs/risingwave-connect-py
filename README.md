# RisingWave Connect

A Python SDK for connecting to RisingWave with PostgreSQL CDC, automatic table discovery, and multiple sink destinations.

## Features

- **PostgreSQL CDC Integration**: Complete Change Data Capture support with automatic schema discovery
- **Table-Level Filtering**: Optimized table selection with pattern matching and validation
- **Column-Level Filtering**: Selective column replication with type control and primary key validation
- **Multiple Sink Support**: Iceberg, S3, and PostgreSQL destinations
- **Advanced CDC Configuration**: SSL, backfilling, publication management, and more
- **Performance Optimization**: Efficient discovery for specific tables, avoiding full database scans
- **SQL Generation**: Automatically generates optimized RisingWave SQL statements

## Installation

```bash
# Using uv (recommended)
uv add risingwave-connect-py

# Using pip
pip install risingwave-connect-py
```

## Quick Start

```python
from risingwave_connect import (
    RisingWaveClient,
    ConnectBuilder,
    PostgreSQLConfig,
    TableSelector
)

# Connect to RisingWave
client = RisingWaveClient("postgresql://root@localhost:4566/dev")

# Configure PostgreSQL CDC
config = PostgreSQLConfig(
    hostname="localhost",
    port=5432,
    username="postgres",
    password="secret",
    database="mydb",
    auto_schema_change=True
)

# Create connector with table selection
builder = ConnectBuilder(client)
result = builder.create_postgresql_connection(
    config=config,
    table_selector=TableSelector(include_patterns=["users", "orders"])
)

print(f"Created CDC source with {len(result['selected_tables'])} tables")
```

## Table Discovery and Selection

### Discover Available Tables

```python
# Discover all available tables
available_tables = builder.discover_postgresql_tables(config)

for table in available_tables:
    print(f"{table.qualified_name} - {table.row_count} rows")
```

### Table-Level Filtering

```python
# Select specific tables
table_selector = ["users", "orders", "products"]

# Using TableSelector for specific tables
from risingwave_connect.discovery.base import TableSelector
table_selector = TableSelector(specific_tables=["users", "orders"])

# Pattern-based selection (checks all tables, then filters)
table_selector = TableSelector(
    include_patterns=["user_*", "order_*"],
    exclude_patterns=["*_temp", "*_backup"]
)

# Include all tables except specific ones
table_selector = TableSelector(
    include_all=True,
    exclude_patterns=["temp_*", "backup_*"]
)
```

### Column-Level Filtering

Select specific columns, control types, and ensure primary key consistency.

```python
from risingwave_connect.discovery.base import (
    TableColumnConfig, ColumnSelection, TableInfo
)

# Define table info
users_table = TableInfo(
    schema_name="public",
    table_name="users",
    table_type="BASE TABLE"
)

# Select specific columns with type control
users_columns = [
    ColumnSelection(
        column_name="id",
        is_primary_key=True,
        risingwave_type="INT"  # Override type if needed
    ),
    ColumnSelection(
        column_name="name",
        risingwave_type="VARCHAR",
        is_nullable=False
    ),
    ColumnSelection(
        column_name="email",
        risingwave_type="VARCHAR"
    ),
    ColumnSelection(
        column_name="created_at",
        risingwave_type="TIMESTAMP"
    )
    # Note: Excluding sensitive columns like 'password_hash'
]

# Create table configuration
users_config = TableColumnConfig(
    table_info=users_table,
    selected_columns=users_columns,
    custom_table_name="clean_users"  # Optional: custom name in RisingWave
)

# Apply column filtering
column_configs = {"users": users_config}

result = builder.create_postgresql_connection(
    config=postgres_config,
    table_selector=["users", "orders"],
    column_configs=column_configs  # NEW parameter
)
```

**Generated SQL with Column Filtering**:

```sql
CREATE TABLE clean_users (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR,
    created_at TIMESTAMP
)
FROM postgres_source TABLE 'public.users';
```

## PostgreSQL CDC Configuration

```python
config = PostgreSQLConfig(
    # Connection details
    hostname="localhost",
    port=5432,
    username="postgres",
    password="secret",
    database="mydb",
    schema_name="public",

    # CDC settings
    auto_schema_change=True,
    publication_name="rw_publication",
    slot_name="rw_slot",

    # SSL configuration
    ssl_mode="require",
    ssl_root_cert="/path/to/ca.pem",

    # Performance tuning
    backfill_parallelism="8",
    backfill_num_rows_per_split="100000",
    backfill_as_even_splits=True
)
```

### Schema Evolution

RisingWave supports automatic schema changes for PostgreSQL CDC sources when `auto_schema_change=True` is enabled:

For detailed information about schema evolution capabilities and limitations, see the [RisingWave Schema Evolution Documentation](https://docs.risingwave.com/ingestion/sources/postgresql/pg-cdc#automatic-schema-changes).

### Supported Data Types

RisingWave supports a comprehensive set of PostgreSQL data types for CDC replication. The SDK automatically maps PostgreSQL types to compatible RisingWave types.

For the complete list of supported PostgreSQL data types and their RisingWave equivalents, see the [RisingWave Supported Data Types Documentation](https://docs.risingwave.com/ingestion/sources/postgresql/pg-cdc#supported-data-types).

## Sink Destinations

### Iceberg Data Lake

```python
from risingwave_connect import IcebergConfig

iceberg_config = IcebergConfig(
    sink_name="analytics_lake",
    warehouse_path="s3://my-warehouse/",
    database_name="analytics",
    table_name="events",
    catalog_type="storage",

    # S3 configuration
    s3_region="us-east-1",
    s3_access_key="your-access-key",
    s3_secret_key="your-secret-key",

    # Data type
    data_type="append-only",
    force_append_only=True
)

# Create sink
builder.create_sink(iceberg_config, ["events", "users"])
```

## Complete Connection Examples

### Basic CDC with All Columns

```python
# Simple table selection with all columns
result = builder.create_postgresql_connection(
    config=postgres_config,
    table_selector=["users", "orders", "products"]  # Fast: only checks these tables
)

selected_tables = [t.qualified_name for t in result['selected_tables']]
```

### Advanced CDC with Column Filtering

```python
from risingwave_connect.discovery.base import (
    TableColumnConfig, ColumnSelection, TableInfo
)

# Configure selective columns for multiple tables
users_config = TableColumnConfig(
    table_info=TableInfo(schema_name="public", table_name="users"),
    selected_columns=[
        ColumnSelection(column_name="id", is_primary_key=True, risingwave_type="INT"),
        ColumnSelection(column_name="name", risingwave_type="VARCHAR", is_nullable=False),
        ColumnSelection(column_name="email", risingwave_type="VARCHAR"),
        ColumnSelection(column_name="created_at", risingwave_type="TIMESTAMP")
    ],
    custom_table_name="clean_users"
)

orders_config = TableColumnConfig(
    table_info=TableInfo(schema_name="public", table_name="orders"),
    selected_columns=[
        ColumnSelection(column_name="order_id", is_primary_key=True, risingwave_type="BIGINT"),
        ColumnSelection(column_name="user_id", risingwave_type="INT", is_nullable=False),
        ColumnSelection(column_name="total_amount", risingwave_type="DECIMAL(10,2)"),
        ColumnSelection(column_name="status", risingwave_type="VARCHAR"),
        ColumnSelection(column_name="created_at", risingwave_type="TIMESTAMP")
    ]
)

# Apply column configurations
column_configs = {
    "users": users_config,
    "orders": orders_config
    # No config for 'products' - will include all columns
}

# Create CDC with column filtering
cdc_result = builder.create_postgresql_connection(
    config=postgres_config,
    table_selector=["users", "orders", "products"],
    column_configs=column_configs
)

# Create sinks for filtered data
selected_tables = [t.qualified_name for t in cdc_result['selected_tables']]
builder.create_s3_sink(s3_config, selected_tables)
builder.create_sink(iceberg_config, selected_tables)
```

### Multi-Destination Data Pipeline

```python
# 1. Set up CDC source with filtering
cdc_result = builder.create_postgresql_connection(
    config=postgres_config,
    table_selector=TableSelector(include_patterns=["user_*", "order_*"])
)

selected_tables = [t.qualified_name for t in cdc_result['selected_tables']]

# 2. Create multiple data destinations
builder.create_s3_sink(s3_config, selected_tables)  # Data lake
builder.create_postgresql_sink(analytics_config, selected_tables)  # Analytics
builder.create_sink(iceberg_config, selected_tables)  # Iceberg warehouse
```

## Examples

The `examples/` directory contains complete working examples:

- **`postgres_cdc_iceberg_pipeline.py`** - End-to-end CDC to Iceberg pipeline
- **`table_column_filtering_example.py`** - Comprehensive table and column filtering examples

## Development

```bash
# Clone and set up development environment
git clone https://github.com/risingwavelabs/risingwave-connect-py.git
cd risingwave-connect-py

# Install with development dependencies
uv venv
source .venv/bin/activate
uv pip install -e .[dev]

# Run tests
pytest

# Format code
ruff format .
```

## Requirements

- Python ≥ 3.10
- RisingWave instance (local or cloud)
- PostgreSQL with CDC enabled
- Required Python packages: `psycopg[binary]`, `pydantic`

## License

Apache 2.0 License
