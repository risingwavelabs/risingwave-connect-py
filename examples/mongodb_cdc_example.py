#!/usr/bin/env python3
"""
MongoDB CDC Pipeline Example

This example demonstrates how to set up a MongoDB CDC pipeline using risingwave-connect-py.
It shows how to:
1. Configure MongoDB CDC source
2. Discover available collections  
3. Set up CDC tables with various options
4. Handle multiple collections and databases
"""

import logging
from risingwave_connect import (
    RisingWaveClient,
    ConnectBuilder,
    MongoDBConfig,
    TableSelector
)

# Configure logging for better debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def basic_mongodb_cdc_example():
    """Basic example: Single collection CDC without metadata."""

    print("🎬 Basic MongoDB CDC Example")
    print("=" * 50)

    try:
        rw_client = RisingWaveClient(
            host="localhost",
            port=4566,
            username="",
            password="",
            database="dev"
        )

        config = MongoDBConfig(
            source_name="movies_cdc_source",
            mongodb_url="mongodb+srv://user:password@cluster0.ri23vla.mongodb.net",
            collection_name="sample_mflix.movies"  # specific collection
        )

        # Create the CDC connection
        builder = ConnectBuilder(rw_client)
        result = builder.create_mongodb_connection(
            config=config,
            dry_run=True  # Set to False to actually execute
        )

        # Check results
        if "success_summary" in result and result["success_summary"]["overall_success"]:
            print("✅ MongoDB CDC setup successful!")
            print(f"Generated SQL:\n{result['sql_statements'][0]}")
        elif "sql_statements" in result and len(result["sql_statements"]) > 0:
            print("✅ SQL generation successful!")
            for stmt in result["sql_statements"]:
                print(f"Generated SQL:\n{stmt}")
        else:
            print("❌ Setup failed:")
            if "failed_statements" in result:
                for error in result["failed_statements"]:
                    print(f"  {error['error']}")
            else:
                print(f"  Result: {result}")

    except Exception as e:
        print(f"❌ Example failed: {e}")


def advanced_mongodb_cdc_example():
    """Advanced example: Multiple collections with metadata tracking."""

    print("\n💬 Advanced MongoDB CDC Example")
    print("=" * 50)

    try:
        rw_client = RisingWaveClient(
            host="localhost",
            port=4566,
            username="",
            password="",
            database="dev"
        )

        config = MongoDBConfig(
            source_name="mflix_cdc_source",
            mongodb_url="mongodb+srv://user:password@cluster0.ri23vla.mongodb.net",
            collection_name="sample_mflix.*"  # all collections in database
        )

        # Discover available collections first
        builder = ConnectBuilder(rw_client)
        collections = builder.discover_mongodb_collections(config)

        print(f"📊 Found {len(collections)} collections:")
        for collection in collections:
            print(
                f"  - {collection.qualified_name} ({collection.row_count:,} documents)")

        # select specific collections
        table_selector = TableSelector(
            # only tables with these patterns
            include_patterns=["*movies*", "*comments*"],
            exclude_patterns=["*test*"]  # Exclude test collections
        )

        # Create CDC with metadata tracking
        result = builder.create_mongodb_connection(
            config=config,
            table_selector=table_selector,
            include_commit_timestamp=True,    # Track when changes occurred
            include_database_name=True,       # Track source database
            include_collection_name=True,     # Track source collection
            dry_run=True  # Change to False to actually execute the SQL
        )

        # Report results
        if "success_summary" in result and result["success_summary"]["overall_success"]:
            print(f"✅ Created {len(result['selected_tables'])} CDC tables!")

            for table in result["selected_tables"]:
                print(f"  📊 {table.table_name}: {table.row_count:,} documents")

            print("\n🔍 Example queries:")
            print("  -- Check record counts")
            for table in result["selected_tables"]:
                print(f"  SELECT count(*) FROM {table.table_name};")

            print("\n  -- Query with metadata")
            print("  SELECT database_name, collection_name, count(*)")
            print("  FROM movies GROUP BY database_name, collection_name;")

            print("\n  -- Extract specific fields")
            print("  SELECT _id, payload->>'title' as title FROM movies LIMIT 10;")

        elif "sql_statements" in result and len(result["sql_statements"]) > 0:
            print("✅ SQL generation successful!")
            for stmt in result["sql_statements"]:
                print(f"Generated SQL:\n{stmt}\n")
        else:
            print("❌ Some operations failed:")
            if "failed_statements" in result:
                for error in result["failed_statements"]:
                    print(f"  {error['error']}")
            else:
                print(f"  Result: {result}")

    except Exception as e:
        print(f"❌ Advanced example failed: {e}")


if __name__ == "__main__":
    print("🔗 MongoDB CDC Examples for RisingWave")
    print("=" * 60)

    try:
        basic_mongodb_cdc_example()
    except Exception as e:
        print(f"Basic example failed as expected: {e.__class__.__name__}")

    try:
        advanced_mongodb_cdc_example()
    except Exception as e:
        print(f"Advanced example failed as expected: {e.__class__.__name__}")
