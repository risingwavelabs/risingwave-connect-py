#!/usr/bin/env python3
"""
SQL Server CDC Pipeline Example

This example demonstrates how to set up a SQL Server CDC pipeline using risingwave-connect-py.
It shows how to:
1. Configure SQL Server CDC source
2. Discover available tables  
3. Set up CDC tables with various options
4. Handle multiple tables and schemas
"""

import logging
from risingwave_connect import (
    RisingWaveClient,
    ConnectBuilder,
    SQLServerConfig,
    TableSelector
)

# Configure logging for better debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def advanced_sqlserver_cdc_example():
    """Advanced example: Multiple tables with metadata tracking."""

    print("\n💼 Advanced SQL Server CDC Example")
    print("=" * 50)

    try:
        rw_client = RisingWaveClient(
            host="localhost",
            port=4566,
            username="",
            password="",
            database="dev"
        )

        config = SQLServerConfig(
            source_name="adventureworks_cdc_source",
            hostname="localhost",
            port=1433,
            username="sa",
            password="YourPassword123",
            database="AdventureWorks2019",
            table_name="Sales.*"  # all tables in Sales schema
        )

        # Discover available tables first
        builder = ConnectBuilder(rw_client)
        # tables = builder.discover_sqlserver_tables(config, schema_name="Sales")

        # print(f"📊 Found {len(tables)} tables in Sales schema:")
        # for table in tables:
        #     print(f"  - {table.qualified_name} ({table.row_count:,} rows)")

        # Select specific tables
        # table_selector = TableSelector(
        #     # only tables with these patterns
        #     include_patterns=["*Customer*", "*Order*"],
        #     # exclude temp/test tables
        #     exclude_patterns=["*Temp*", "*Test*"]
        # )

        # Create CDC with metadata tracking
        result = builder.create_sqlserver_connection(
            config=config,
            table_selector=["movies"],
            include_timestamp=True,        # Track when changes occurred
            include_database_name=True,    # Track source database
            include_schema_name=True,      # Track source schema
            include_table_name=True,       # Track source table
            dry_run=True  # Change to False to actually execute the SQL
        )

        # Report results
        if "success_summary" in result and result["success_summary"]["overall_success"]:
            print(f"✅ Created {len(result['selected_tables'])} CDC tables!")

            for table in result["selected_tables"]:
                row_info = f"{table.row_count:,} rows" if table.row_count is not None else "unknown rows"
                print(f"  📊 {table.table_name}: {row_info}")

            print("\n🔍 Example queries:")
            print("  -- Check record counts")
            for table in result["selected_tables"]:
                print(f"  SELECT count(*) FROM {table.table_name};")

            print("\n  -- Query with metadata")
            print("  SELECT database_name, schema_name, table_name, count(*)")
            print(
                "  FROM SalesOrderHeader GROUP BY database_name, schema_name, table_name;")

            print("\n  -- Track recent changes")
            print("  SELECT TOP 100 * FROM SalesOrderHeader ORDER BY commit_ts DESC;")

        elif "sql_statements" in result and len(result["sql_statements"]) > 0:
            print("✅ SQL generation successful!")
            for i, stmt in enumerate(result["sql_statements"], 1):
                print(f"\n📝 SQL Statement {i}:")
                print("-" * 80)
                print(stmt)
                print("-" * 80)
        else:
            print("❌ Some operations failed:")
            if "failed_statements" in result:
                for error in result["failed_statements"]:
                    print(f"  {error['error']}")
            else:
                print(f"  Result: {result}")

        # Show generated SQL in dry run mode
        if result.get("dry_run") and "sql_statements" in result:
            print(
                f"\n🔍 Generated SQL Statements ({len(result['sql_statements'])}):")
            for i, stmt in enumerate(result["sql_statements"], 1):
                print(stmt)
                print("-" * 80)

    except Exception as e:
        print(f"❌ Advanced example failed: {e}")
        print("\n💡 Troubleshooting tips:")
        print("  1. Ensure SQL Server Agent is running")
        print("  2. Enable CDC on the database: EXEC sys.sp_cdc_enable_db")
        print("  3. Enable CDC on specific tables:")
        print("     EXEC sys.sp_cdc_enable_table @source_schema='Sales', @source_name='Customer', @role_name=NULL")
        print("  4. Check CDC status: SELECT name, is_cdc_enabled FROM sys.databases")


def discover_sqlserver_example():
    """Discovery example: Explore SQL Server database structure."""

    print("\n🔍 SQL Server Discovery Example")
    print("=" * 50)

    try:
        config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="YourPassword123",
            database="AdventureWorks2019",
            table_name="dbo.*"  # discover all tables in dbo schema
        )

        # No client needed for discovery
        builder = ConnectBuilder(rw_client=None)

        # Get available schemas
        try:
            schemas = builder.get_sqlserver_schemas(config)
            print(f"📂 Available schemas ({len(schemas)}):")
            for schema in schemas:
                print(f"  - {schema}")
        except Exception as e:
            print(f"❌ Could not discover schemas: {e}")
            return

        # Get tables for each schema (limit to first 3 schemas)
        for schema in schemas[:3]:
            try:
                tables = builder.discover_sqlserver_tables(
                    config, schema_name=schema)
                print(f"\n📊 Tables in '{schema}' schema ({len(tables)}):")
                for table in tables[:10]:  # Show first 10 tables
                    row_info = f"{table.row_count:,} rows" if table.row_count else "unknown rows"
                    print(f"  - {table.qualified_name} ({row_info})")
                if len(tables) > 10:
                    print(f"  ... and {len(tables) - 10} more tables")
            except Exception as e:
                print(f"❌ Could not discover tables in schema '{schema}': {e}")

    except Exception as e:
        print(f"❌ Discovery failed: {e}")
        print("\n💡 Check connection details and permissions")


if __name__ == "__main__":
    print("🔗 SQL Server CDC Examples for RisingWave")
    print("=" * 60)

    # Note: These examples require:
    # 1. SQL Server with CDC enabled
    # 2. pyodbc and SQL Server ODBC driver
    # 3. Valid connection credentials
    try:
        advanced_sqlserver_cdc_example()
    except Exception as e:
        print(f"Advanced example failed as expected: {e.__class__.__name__}")

    # try:
    #     discover_sqlserver_example()
    # except Exception as e:
    #     print(f"Discovery example failed as expected: {e.__class__.__name__}")
