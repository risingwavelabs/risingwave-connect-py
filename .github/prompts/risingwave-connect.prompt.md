---
mode: agent
---

# RisingWave Connect Copilot Prompt

## Overview

RisingWave Connect simplifies connecting to RisingWave, a streaming database. It supports table discovery, connection creation, and integration with sources like PostgreSQL/MySQL/MongoDB and sinks like S3/Iceberg/Snowflake/Redshift.

## Development

This project uses [uv](https://github.com/astral-sh/uv) for fast Python package management and builds. See the `[tool.uv]` section in `pyproject.toml`.

Before running any Python code, ensure the agent is operating within a virtual environment (venv). This guarantees dependencies are isolated and consistent.

- Check for an active venv (`sys.prefix` or `VIRTUAL_ENV`).
- If not in venv, prompt to activate or create one using `uv venv .venv` and activate with `source .venv/bin/activate`.
- All Python commands should run inside the venv context.

##

```sql
CREATE SOURCE Template
CREATE SOURCE [IF NOT EXISTS] source_name (
column_name data_type [AS source_column_name] [NOT NULL],
...
[, PRIMARY KEY (column_name, ...)]
)
WITH (
connector='connector_name',
connector_property='value',
...
)
FORMAT format_type ENCODE encode_type (
... -- Format-specific options
);
```

Schema definition: Define columns with data types

- WITH clause: Specify connector and connection properties
- FORMAT/ENCODE: Define data format (PLAIN, UPSERT, DEBEZIUM) and encoding (JSON, AVRO, CSV, etc.)

```sql
CREATE SINK [IF NOT EXISTS] sink_name
[FROM sink_from | AS select_query]
WITH (
connector='connector_name',
connector_parameter='value',
...
)
[FORMAT data_format ENCODE data_encode [(
format_parameter='value'
)]];
```

Key components:

- FROM clause: Specify source (materialized view or table) OR use AS with SELECT query
- WITH clause: Specify connector and connection properties
- FORMAT/ENCODE: Optional for most sinks, required for Kafka, Kinesis, Pulsar, Redis
