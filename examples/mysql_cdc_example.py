#!/usr/bin/env python3
"""
MySQL CDC Source Examples for RisingWave Connect
===============================================

This example demonstrates how to create MySQL CDC source connections
with different configurations and table discovery.
"""

import logging
from risingwave_connect.client import RisingWaveClient
from risingwave_connect.connect_builder import ConnectBuilder, create_mysql_cdc_source_connection
from risingwave_connect.sources.mysql import MySQLConfig
from risingwave_connect.discovery.base import TableSelector


# Simple column config class for filtering
class SimpleColumnConfig:
    def __init__(self, included_columns=None, excluded_columns=None):
        self.included_columns = included_columns
        self.excluded_columns = excluded_columns


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    print("🔗 MySQL CDC Source Examples for RisingWave")
    print("=" * 60)

    # Initialize RisingWave client (dry run mode for examples)
    client = RisingWaveClient(
        host="localhost", port=4566, user="root", password="")
    builder = ConnectBuilder(client)

    # Example 1: Basic MySQL CDC Connection
    print("\n💼 Basic MySQL CDC Example")
    print("=" * 40)

    mysql_config = MySQLConfig(
        hostname="mysql-server.company.com",
        port=3306,
        username="cdc_user",
        password="cdc_password",
        database="ecommerce",
        server_id=1001
    )

    # Discover available tables
    try:
        tables = builder.discover_mysql_tables(mysql_config)
        print(f"📋 Discovered {len(tables)} tables:")
        for table in tables:
            print(
                f"  • {table.schema_name}.{table.table_name} ({table.estimated_row_count} rows)")
    except Exception as e:
        print(f"⚠️  Table discovery failed: {e}")

    # Create CDC connection for specific tables
    result = builder.create_mysql_connection(
        config=mysql_config,
        table_selector=["users", "orders"],
        include_timestamp=True,
        include_database_name=True,
        include_table_name=True,
        dry_run=True
    )

    if result.get("success_summary", {}).get("overall_success"):
        print(
            f"✅ Created {result['success_summary']['total_tables']} CDC tables!")
        for table_info in result['table_info']:
            print(
                f"  📊 {table_info['table_name']}: {table_info['estimated_rows']} rows")

        print("\n🔍 Example queries:")
        print("  -- Check record counts")
        print("  SELECT count(*) FROM users;")
        print("  SELECT count(*) FROM orders;")

        print("\n  -- Query with metadata")
        print("  SELECT database_name, table_name, count(*)")
        print("  FROM users GROUP BY database_name, table_name;")

        print("\n  -- Track recent changes")
        print("  SELECT * FROM orders ORDER BY commit_ts DESC LIMIT 100;")

    else:
        print(
            f"❌ Error: {result.get('success_summary', {}).get('error', 'Unknown error')}")

    # Example 2: Advanced MySQL CDC with SSL and Column Filtering
    print("\n💼 Advanced MySQL CDC with SSL Example")
    print("=" * 50)

    secure_mysql_config = MySQLConfig(
        hostname="secure-mysql.company.com",
        port=3306,
        username="secure_user",
        password="secure_password",
        database="analytics",
        server_id=1002,
        ssl_mode="verify_ca",
        ssl_ca="/path/to/ca-cert.pem",
        connection_timeout=60,
        heartbeat_interval=5000
    )

    # Configure column filtering for tables
    column_configs = {
        "customer_data": SimpleColumnConfig(
            included_columns=["customer_id", "email", "created_at", "status"],
            excluded_columns=["password_hash", "ssn", "credit_card"]
        ),
        "order_items": SimpleColumnConfig(
            included_columns=["order_id", "product_id", "quantity", "price"],
            excluded_columns=["internal_notes"]
        )
    }

    # Use convenience function for CDC connection
    result = create_mysql_cdc_source_connection(
        rw_client=client,
        mysql_config=secure_mysql_config,
        include_all_tables=True,
        exclude_tables=["temp_*", "backup_*"],
        column_configs=column_configs,
        include_timestamp=True,
        include_database_name=True,
        include_table_name=True,
        dry_run=True
    )

    if result.get("success_summary", {}).get("overall_success"):
        print(
            f"✅ Created {result['success_summary']['total_tables']} filtered CDC tables!")
        for table_name in result['success_summary']['created_tables']:
            print(f"  📊 {table_name}: filtered columns")

        print("\n🔍 Example analytics queries:")
        print("  -- Customer growth over time")
        print("  SELECT DATE_TRUNC('month', created_at) as month,")
        print("         COUNT(*) as new_customers")
        print("  FROM customer_data")
        print("  WHERE status = 'active'")
        print("  GROUP BY month ORDER BY month;")

        print("\n  -- Real-time order monitoring")
        print("  SELECT COUNT(*) as orders_count,")
        print("         SUM(quantity * price) as total_value")
        print("  FROM order_items")
        print("  WHERE commit_ts > NOW() - INTERVAL '1 hour';")

    else:
        print(
            f"❌ Error: {result.get('success_summary', {}).get('error', 'Unknown error')}")

    # Example 3: MySQL CDC with Custom Schema Discovery
    print("\n💼 MySQL CDC with Schema Selection Example")
    print("=" * 50)

    multi_schema_config = MySQLConfig(
        hostname="mysql-cluster.company.com",
        port=3306,
        username="analytics_user",
        password="analytics_password",
        database="warehouse",
        server_id=1003
    )

    # Get available schemas
    try:
        schemas = builder.get_mysql_schemas(multi_schema_config)
        print(f"📋 Available schemas: {', '.join(schemas)}")
    except Exception as e:
        print(f"⚠️  Schema discovery failed: {e}")

    # Create table selector for specific patterns
    table_selector = TableSelector(
        include_patterns=["fact_*", "dim_*"],
        exclude_patterns=["*_temp", "*_backup"]
    )

    result = builder.create_mysql_connection(
        config=multi_schema_config,
        table_selector=table_selector,
        include_timestamp=True,
        dry_run=True
    )

    if result.get("success_summary", {}).get("overall_success"):
        print(
            f"✅ Created {result['success_summary']['total_tables']} warehouse tables!")

        print("\n🔍 Data warehouse queries:")
        print("  -- Fact table summary")
        print("  SELECT table_name, COUNT(*) as records")
        print("  FROM (")
        for table in result['success_summary']['created_tables']:
            if table.startswith('fact_'):
                print(
                    f"    SELECT '{table}' as table_name, COUNT(*) FROM {table}")
                print("    UNION ALL")
        print("  ) GROUP BY table_name;")

    else:
        print(
            f"❌ Error: {result.get('success_summary', {}).get('error', 'Unknown error')}")

    # Print generated SQL
    print(f"\n🔍 Generated SQL Statements ({len(result['sql_statements'])}):")
    for i, sql in enumerate(result['sql_statements'], 1):
        print(f"-- Statement {i}")
        print(sql)
        print("-" * 80)


if __name__ == "__main__":
    main()
