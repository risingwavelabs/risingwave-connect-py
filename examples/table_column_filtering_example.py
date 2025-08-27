#!/usr/bin/env python3
"""
Usage example for table-level and column-level filtering in RisingWave CDC.

This example demonstrates:
1. Table-level filtering - selecting specific tables
2. Column-level filtering - selecting specific columns with type control
3. Primary key validation and consistency
4. Custom table naming in RisingWave
"""

from risingwave_connect import RisingWaveClient, ConnectBuilder, PostgreSQLConfig
from risingwave_connect.discovery.base import (
    TableSelector, TableColumnConfig, ColumnSelection, TableInfo
)


def example_usage():
    """Example of using table and column filtering."""

    print("RisingWave CDC: Table and Column Filtering Example")
    print("=" * 60)

    # Initialize RisingWave client
    client = RisingWaveClient(
        host="localhost",
        port=4566,
        username="root",
        password="",
        database="dev"
    )

    builder = ConnectBuilder(client)

    # Configure PostgreSQL source
    postgres_config = PostgreSQLConfig(
        hostname="your-postgres-host.com",
        port=5432,
        username="your_username",
        password="your_password",
        database="your_database",
        schema_name="public",
        ssl_mode="required",
        auto_schema_change=True,
    )

    # Example 1: Basic table filtering (no column filtering)
    print("📋 Example 1: Table-level filtering only")
    print("-" * 40)

    tables_to_sync = ["users", "orders", "products"]

    result = builder.create_postgresql_connection(
        config=postgres_config,
        table_selector=tables_to_sync,
        dry_run=True  # Safe for demo
    )

    print(f"✅ Would sync {len(tables_to_sync)} tables with all columns")
    print("Generated SQL example:")
    print(result['sql_statements'][1])  # Show first table creation
    print()

    # Example 2: Column-level filtering
    print("🔧 Example 2: Column-level filtering")
    print("-" * 40)

    # Define which columns to include for the 'users' table
    users_table = TableInfo(
        schema_name="public",
        table_name="users",
        table_type="BASE TABLE"
    )

    users_columns = [
        ColumnSelection(
            column_name="id",
            is_primary_key=True,
            risingwave_type="INT"  # Ensure correct type
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
        # Note: Intentionally excluding sensitive columns like 'password_hash'
    ]

    users_config = TableColumnConfig(
        table_info=users_table,
        selected_columns=users_columns,
        custom_table_name="clean_users"  # Different name in RisingWave
    )

    # Define columns for 'orders' table
    orders_table = TableInfo(
        schema_name="public",
        table_name="orders",
        table_type="BASE TABLE"
    )

    orders_columns = [
        ColumnSelection(
            column_name="order_id",
            is_primary_key=True,
            risingwave_type="BIGINT"
        ),
        ColumnSelection(
            column_name="user_id",
            risingwave_type="INT",
            is_nullable=False
        ),
        ColumnSelection(
            column_name="total_amount",
            risingwave_type="DECIMAL(10,2)"  # Specific precision
        ),
        ColumnSelection(
            column_name="status",
            risingwave_type="VARCHAR"
        ),
        ColumnSelection(
            column_name="created_at",
            risingwave_type="TIMESTAMP"
        )
    ]

    orders_config = TableColumnConfig(
        table_info=orders_table,
        selected_columns=orders_columns
        # No custom_table_name - will use 'orders'
    )

    # Column configurations map
    column_configs = {
        "users": users_config,
        "orders": orders_config
        # No config for 'products' - will include all columns
    }

    result = builder.create_postgresql_connection(
        config=postgres_config,
        table_selector=["users", "orders", "products"],
        column_configs=column_configs,
        dry_run=True
    )

    print("✅ Column filtering configured")
    print("Table configurations:")
    print("  • users → clean_users (4 selected columns)")
    print("  • orders → orders (5 selected columns)")
    print("  • products → products (all columns)")
    print()

    print("Generated SQL for column-filtered table:")
    # Show first table with column filtering
    for sql in result['sql_statements'][1:2]:
        print(sql)
    print()

    # Example 3: Validation and error handling
    print("⚠️  Example 3: Validation and error handling")
    print("-" * 40)

    print("What happens when validation fails:")
    print("1. Missing tables → ValueError with specific missing table names")
    print("2. Missing columns → ValueError with specific missing column names")
    print("3. Invalid primary keys → ValueError with primary key consistency errors")
    print("4. Type mismatches → Warnings but continues with type overrides")
    print()

    # Example 4: Performance considerations
    print("⚡ Example 4: Performance optimization")
    print("-" * 40)

    print("Performance benefits:")
    print("✅ Table-level filtering: Only checks specified tables (not all 1000+ tables)")
    print("✅ Column-level filtering: Only fetches metadata for selected columns")
    print("✅ Validation: Catches errors before expensive CDC setup")
    print("✅ Type mapping: Automatic PostgreSQL → RisingWave type conversion")
    print()

    print("🎯 RECOMMENDED USAGE PATTERNS")
    print("=" * 60)
    print()
    print("1️⃣ For simple CDC (all columns):")
    print("   table_selector = ['table1', 'table2']")
    print("   column_configs = None")
    print()
    print("2️⃣ For selective column CDC:")
    print("   • Define ColumnSelection for each column you want")
    print("   • Ensure primary key columns are included and marked")
    print("   • Use TableColumnConfig to group column selections")
    print("   • Pass column_configs dict to create_postgresql_connection()")
    print()
    print("3️⃣ For sensitive data filtering:")
    print("   • Exclude columns like passwords, SSNs, etc.")
    print("   • Include only business-relevant columns")
    print("   • Use custom table names for cleaner RisingWave schema")
    print()
    print("4️⃣ For data type control:")
    print("   • Override PostgreSQL types with risingwave_type parameter")
    print("   • Useful for DECIMAL precision, VARCHAR sizes, etc.")
    print("   • Ensures consistent types across environments")


if __name__ == "__main__":
    example_usage()
