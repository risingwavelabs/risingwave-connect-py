# RisingWave Pipeline SDK

A Python SDK for building RisingWave data pipelines with PostgreSQL CDC, automatic table discovery, and multiple sink destinations.

## Features

- **PostgreSQL CDC Integration**: Complete Change Data Capture support with automatic schema discovery
- **Flexible Table Selection**: Pattern-based, interactive, or programmatic table selection
- **Multiple Sink Support**: Iceberg, S3, and PostgreSQL destinations
- **Advanced CDC Configuration**: SSL, backfilling, publication management, and more
- **SQL Generation**: Automatically generates optimized RisingWave SQL statements

## Installation

```bash
# Using uv (recommended)
uv add risingwave-pipeline-sdk

# Using pip
pip install risingwave-pipeline-sdk
```

## Quick Start

```python
from risingwave_pipeline_sdk import (
    RisingWaveClient,
    PipelineBuilder,
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

# Create pipeline with table selection
builder = PipelineBuilder(client)
result = builder.create_postgresql_pipeline(
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

### Flexible Table Selection

```python
# Select specific tables
TableSelector(specific_tables=["users", "orders", "products"])

# Pattern-based selection
TableSelector(
    include_patterns=["user_*", "order_*"],
    exclude_patterns=["*_temp", "*_backup"]
)

# Include all tables except specific ones
TableSelector(
    include_all=True,
    exclude_patterns=["temp_*", "backup_*"]
)
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

## Sink Destinations

### Iceberg Data Lake

```python
from risingwave_pipeline_sdk import IcebergConfig

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

### S3 Data Archive

```python
from risingwave_pipeline_sdk import S3Config

s3_config = S3Config(
    sink_name="data_archive",
    bucket_name="my-data-bucket",
    path="raw-data/",
    region_name="us-east-1",
    access_key_id="your-access-key",
    secret_access_key="your-secret-key",

    # Format configuration
    format_type="PLAIN",
    encode_type="PARQUET"
)

builder.create_s3_sink(s3_config, ["users", "orders"])
```

### PostgreSQL Analytics Database

```python
from risingwave_pipeline_sdk import PostgreSQLSinkConfig

analytics_config = PostgreSQLSinkConfig(
    sink_name="analytics_db",
    hostname="analytics.example.com",
    port=5432,
    username="analytics_user",
    password="password",
    database="analytics",
    postgres_schema="real_time"
)

# Create sink with custom transformations
custom_queries = {
    "users": "SELECT id, name, email, created_at FROM users WHERE active = true",
    "orders": "SELECT * FROM orders WHERE status != 'cancelled'"
}

builder.create_postgresql_sink(
    analytics_config,
    ["users", "orders"],
    select_queries=custom_queries
)
```

## Complete Pipeline Example

```python
# 1. Set up CDC source
cdc_result = builder.create_postgresql_pipeline(
    config=postgres_config,
    table_selector=TableSelector(include_patterns=["user_*", "order_*"])
)

selected_tables = [t.qualified_name for t in cdc_result['selected_tables']]

# 2. Create multiple sinks
builder.create_s3_sink(s3_config, selected_tables)  # Data lake
builder.create_postgresql_sink(analytics_config, selected_tables)  # Analytics
builder.create_sink(iceberg_config, selected_tables)  # Iceberg warehouse
```

## Examples

The `examples/` directory contains complete working examples:

- **`postgres_cdc_iceberg_pipeline.py`** - End-to-end CDC to Iceberg pipeline
- **`interactive_discovery.py`** - Interactive table discovery and selection
- **`env_config_example.py`** - Environment variable based configuration

## Environment Configuration

Configure using environment variables for production deployments:

```bash
# RisingWave connection
export RW_HOST=localhost
export RW_PORT=4566
export RW_USER=root
export RW_DATABASE=dev

# PostgreSQL CDC source
export PG_HOST=localhost
export PG_PORT=5432
export PG_USER=postgres
export PG_PASSWORD=secret
export PG_DATABASE=mydb

# Table selection
export TABLE_PATTERNS="users,orders,products"
```

## Development

```bash
# Clone and set up development environment
git clone https://github.com/risingwavelabs/risingwave-pipeline-sdk.git
cd risingwave-pipeline-sdk

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
