---
mode: agent
---

# RisingWave Connect Copilot Prompt

## Overview

RisingWave Connect simplifies connecting to RisingWave, a streaming database. It supports table discovery, connection creation, and integration with sources like PostgreSQL/MySQL/MongoDB and sinks like S3/Iceberg/Snowflake/Redshift.

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
