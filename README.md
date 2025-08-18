# RisingWave Pipeline SDK

A Python SDK for creating and managing RisingWave data pipelines with **automatic table discovery and selection**. Build streaming data pipelines from PostgreSQL, MySQL, Kafka, and other sources using a simple Python API.

## 🚀 Features

- **🔍 Table Discovery**: Automatically discover tables, schemas, and metadata from source databases
- **📋 Table Selection**: Flexible table selection with patterns, exclusions, or interactive selection
- **⚡ Advanced Backfill**: Configure parallel backfilling with custom row splits and parallelism
- **🛠️ Easy Pipeline Creation**: High-level API to create RisingWave sources, tables, and materialized views
- **🐘 PostgreSQL CDC**: Full support for PostgreSQL Change Data Capture with automatic schema mapping
- **🔧 Flexible Configuration**: Configure via Python objects, environment variables, or CLI flags
- **🔗 Connection Management**: Automatic connection pooling and error handling
- **📜 SQL Generation**: Generates optimized RisingWave SQL statements
- **💻 Rich CLI**: Command-line interface with table discovery and interactive selection

## 📦 Installation

```bash
# Install with uv (recommended)
uv add risingwave-pipeline-sdk

# Or with pip
pip install risingwave-pipeline-sdk
```

## 🎯 Quick Start

### Table Discovery and Selection

```python
from risingwave_pipeline_sdk import (
    RisingWaveClient,
    PipelineBuilder,
    PostgreSQLConfig,
    TableSelector
)

# Connect to RisingWave
rw_client = RisingWaveClient("postgresql://root@localhost:4566/dev")

# Configure PostgreSQL with advanced options
pg_config = PostgreSQLConfig(
    source_name="ecommerce_cdc",
    hostname="localhost",
    port=5432,
    username="postgres",
    password="secret",
    database="ecommerce",
    schema_name="public",

    # CDC Configuration
    auto_schema_change=True,
    publication_name="rw_publication",
    ssl_mode="require",

    # Advanced backfill configuration
    backfill_num_rows_per_split="100000",
    backfill_parallelism="8",
    backfill_as_even_splits=True
)

# Create pipeline builder
builder = PipelineBuilder(rw_client)

# 🔍 Discover available tables
available_tables = builder.discover_postgresql_tables(pg_config)
print(f"Found {len(available_tables)} tables:")
for table in available_tables:
    print(f"  - {table.qualified_name} ({table.row_count} rows)")

# 📋 Select tables using flexible criteria
table_selector = TableSelector(
    include_patterns=["user_*", "order_*", "product_*"],
    exclude_patterns=["*_temp", "*_backup"]
)

# 🚀 Create complete pipeline
result = builder.create_postgresql_pipeline(
    config=pg_config,
    table_selector=table_selector
)

print(f"✅ Created source: {pg_config.source_name}")
print(f"✅ Created {len(result['selected_tables'])} tables")
```

### Generated SQL Example

The SDK automatically generates optimized SQL like this:

```sql
-- Step 1: Create the shared CDC source
CREATE SOURCE IF NOT EXISTS ecommerce_cdc WITH (
    connector='postgres-cdc',
    hostname='localhost',
    port='5432',
    username='postgres',
    password='secret',
    database.name='ecommerce',
    schema.name='public',
    ssl.mode='require',
    publication.name='rw_publication',
    publication.create.enable='true',
    auto.schema.change='true'
);

-- Step 2: Create tables with advanced backfill
CREATE TABLE IF NOT EXISTS users (*)
WITH (
    backfill.num_rows_per_split='100000',
    backfill.parallelism='8',
    backfill.as_even_splits='true'
)
FROM ecommerce_cdc
TABLE 'public.users';

CREATE TABLE IF NOT EXISTS orders (*)
WITH (
    backfill.num_rows_per_split='100000',
    backfill.parallelism='8',
    backfill.as_even_splits='true'
)
FROM ecommerce_cdc
TABLE 'public.orders';
```

## 💻 CLI Usage

### Table Discovery

```bash
# Discover tables in PostgreSQL database
risingwave-pipeline postgres discover \
  --pg-host localhost --pg-user postgres --pg-password secret \
  --pg-db ecommerce --show-details

# Output:
# 🔍 Discovering tables in schema 'public'...
# ✓ Found 15 tables in schema 'public':
#
# ┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
# ┃ Table Name   ┃ Type       ┃ Row Count ┃ Size (MB) ┃ Comment       ┃
# ┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
# │ users        │ BASE TABLE │     50000 │      12.5 │ User accounts │
# │ orders       │ BASE TABLE │    125000 │      45.2 │ Order data    │
# │ products     │ BASE TABLE │      5000 │       2.1 │ Product info  │
# └──────────────┴────────────┴───────────┴───────────┴───────────────┘
```

### Interactive Pipeline Creation

```bash
# Interactive pipeline creation with table selection
risingwave-pipeline postgres create-pipeline \
  --rw-host localhost --pg-host localhost \
  --pg-user postgres --pg-password secret \
  --source-name ecommerce_cdc --interactive

# The CLI will guide you through:
# 1. 🔗 Connection testing
# 2. 🔍 Table discovery
# 3. 📋 Interactive table selection
# 4. 🚀 Pipeline creation
```

### Advanced Pipeline Creation

```bash
# Create pipeline with specific configuration
risingwave-pipeline postgres create-pipeline \
  --rw-host localhost --rw-port 4566 --rw-user root --rw-db dev \
  --pg-host database.example.com --pg-port 5432 --pg-user rwuser --pg-password secret \
  --pg-db production --source-name prod_cdc \
  --include-tables users,orders,products \
  --auto-schema-change --ssl-mode require \
  --backfill-rows-per-split 100000 --backfill-parallelism 8
```

## 📋 Table Selection Options

The SDK provides flexible table selection methods:

### 1. Specific Tables

```python
TableSelector(specific_tables=["public.users", "public.orders"])
```

### 2. Pattern-Based Selection

```python
TableSelector(
    include_patterns=["user_*", "order_*"],
    exclude_patterns=["*_temp", "*_archive"]
)
```

### 3. Include All with Exclusions

```python
TableSelector(
    include_all=True,
    exclude_patterns=["staging_*", "temp_*"]
)
```

### 4. CLI Options

```bash
# Include all tables
--include-all

# Include specific tables
--include-tables users,orders,products

# Exclude certain tables
--exclude-tables temp_data,archive_logs

# Interactive selection
--interactive
```

## 📤 Sink Configuration

The SDK supports creating sinks to various destinations after setting up your sources:

### S3 Data Lake Sink

```python
from risingwave_pipeline_sdk import S3Config, PipelineBuilder

# Configure S3 sink for data archival
s3_config = S3Config(
    sink_name="ecommerce_data_lake",
    region_name="us-east-1",
    bucket_name="my-data-lake",
    path="ecommerce/cdc_data/",
    access_key_id="your-access-key",
    secret_access_key="your-secret-key",

    # Data format configuration
    data_type="append-only",
    format_type="PLAIN",
    encode_type="PARQUET",
    force_append_only=True
)

# Create S3 sinks for selected tables
builder = PipelineBuilder(rw_client)
s3_result = builder.create_s3_sink(
    s3_config=s3_config,
    source_tables=["public.users", "public.orders"],
    dry_run=True  # See generated SQL
)

# Generated SQL:
# CREATE SINK IF NOT EXISTS ecommerce_data_lake_public_users
# FROM public.users
# WITH (
#     connector='s3',
#     s3.region_name='us-east-1',
#     s3.bucket_name='my-data-lake',
#     s3.path='ecommerce/cdc_data/',
#     type='append-only'
# )
# FORMAT PLAIN ENCODE PARQUET(force_append_only=true);
```

### PostgreSQL Analytics Sink

```python
from risingwave_pipeline_sdk import PostgreSQLSinkConfig

# Configure PostgreSQL sink for real-time analytics
pg_sink_config = PostgreSQLSinkConfig(
    sink_name="realtime_analytics",
    hostname="analytics-db.example.com",
    port=5432,
    username="analytics_user",
    password="analytics_password",
    database="analytics",
    postgres_schema="real_time",
    data_type="append-only",
    ssl_mode="require"
)

# Create PostgreSQL sinks with custom queries
custom_queries = {
    "public.users": "SELECT id, name, email, created_at FROM public.users WHERE active = true",
    "public.orders": "SELECT order_id, user_id, total, status, created_at FROM public.orders"
}

pg_result = builder.create_postgresql_sink(
    pg_sink_config=pg_sink_config,
    source_tables=["public.users", "public.orders"],
    select_queries=custom_queries,
    dry_run=True
)

# Generated SQL:
# CREATE SINK IF NOT EXISTS realtime_analytics_public_users
# AS SELECT id, name, email, created_at FROM public.users WHERE active = true
# WITH (
#     connector='postgres',
#     postgres.host='analytics-db.example.com',
#     postgres.port='5432',
#     postgres.user='analytics_user',
#     postgres.password='analytics_password',
#     postgres.database='analytics',
#     postgres.table='real_time.realtime_analytics_public_users',
#     type='append-only'
# );
```

### Complete Pipeline: Source → RisingWave → Multiple Sinks

```python
# 1. Create PostgreSQL CDC source
pipeline_result = builder.create_postgresql_pipeline(
    config=pg_source_config,
    table_selector=table_selector
)

selected_tables = [table.qualified_name for table in pipeline_result['selected_tables']]

# 2. Create S3 sink for data lake archival
s3_result = builder.create_s3_sink(s3_config, selected_tables)

# 3. Create PostgreSQL sink for real-time analytics
pg_result = builder.create_postgresql_sink(pg_sink_config, selected_tables)

# 4. Create additional sinks (templates available)
# - Snowflake for data warehouse
# - Kafka for event streaming
# - Elasticsearch for search
# - Iceberg for data lake
```

## ⚙️ Advanced Configuration

### PostgreSQL CDC with All Options

```python
from risingwave_pipeline_sdk import PostgreSQLConfig

config = PostgreSQLConfig(
    # Basic connection
    source_name="advanced_cdc",
    hostname="postgres.example.com",
    port=5432,
    username="rwuser",
    password="secret",
    database="production",
    schema_name="public",

    # CDC Configuration
    slot_name="rw_slot_production",
    publication_name="rw_publication",
    publication_create_enable=True,
    auto_schema_change=True,

    # SSL Configuration
    ssl_mode="require",
    ssl_root_cert="/path/to/ca.pem",

    # Backfill Configuration
    backfill_num_rows_per_split="100000",
    backfill_parallelism="8",
    backfill_as_even_splits=True,

    # Advanced Debezium Properties
    debezium_properties={
        "schema.history.internal.skip.unparseable.ddl": "true",
        "decimal.handling.mode": "string",
        "transforms": "unwrap"
    },

    # Extra WITH Properties
    extra_properties={
        "wal2json.message.prefix": "rw_"
    }
)
```

## 🔧 Extensibility

The SDK is designed for easy extension to other database types:

```python
# Template for adding new source types
from risingwave_pipeline_sdk.sources.templates import (
    MySQLConfig, MySQLDiscovery, MySQLPipeline,
    SQLServerConfig, MongoDBConfig, KafkaConfig
)

# Each source type follows the same pattern:
# 1. Config class with source-specific options
# 2. Discovery class for table/schema introspection
# 3. Pipeline class for SQL generation
```

See `src/risingwave_pipeline_sdk/sources/templates.py` for implementation templates.

## 🏗️ Development

```bash
git clone <repo>
cd risingwave-pipeline-sdk
uv venv
source .venv/bin/activate
uv pip install -e .[dev]
pytest
```

## 📄 License

MIT License
