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
    create_mongodb_cdc_source_connection
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def example_mongodb_cdc_multi_collection():
    """MongoDB CDC setup for multiple collections using patterns."""

    rw_client = RisingWaveClient(host="localhost", port=4566)

    # Configure MongoDB CDC for all collections in a database
    mongodb_config = MongoDBConfig(
        source_name="mongodb_cdc_multi",
        mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
        collection_name="mydb.*"  # All collections in 'mydb' database
    )

    try:
        builder = ConnectBuilder(rw_client)

        # Create MongoDB CDC connection with metadata columns
        result = builder.create_mongodb_connection(
            mongodb_config,
            include_commit_timestamp=True,
            include_database_name=True,
            include_collection_name=True,
            dry_run=True
        )

        print("🔍 Multi-collection CDC pipeline (dry run):")
        print(f"Collections discovered: {len(result['available_tables'])}")

        for table in result["available_tables"]:
            print(f"  - {table.qualified_name} ({table.row_count} documents)")

        print("\nGenerated SQL:")
        for sql in result["sql_statements"]:
            print(f"\n{sql}\n" + "="*50)

    except Exception as e:
        logger.error(f"Failed to create multi-collection CDC pipeline: {e}")


def example_mongodb_cdc_cross_database():
    """MongoDB CDC setup across multiple databases."""

    rw_client = RisingWaveClient(host="localhost", port=4566)

    # Configure MongoDB CDC for collections across multiple databases
    mongodb_config = MongoDBConfig(
        source_name="mongodb_cdc_cross_db",
        mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
        # Mix of patterns and specific collections
        collection_name="db1.*, db2.orders, analytics.events"
    )

    try:
        # Use the convenience function
        result = create_mongodb_cdc_source_connection(
            rw_client,
            mongodb_config,
            include_all_collections=True,
            include_commit_timestamp=True,
            include_database_name=True,
            include_collection_name=True,
            dry_run=True
        )

        print("🔍 Cross-database CDC pipeline (dry run):")
        print(f"Collections discovered: {len(result['available_tables'])}")

        # Group by database
        db_collections = {}
        for table in result["available_tables"]:
            db_name = table.schema_name
            if db_name not in db_collections:
                db_collections[db_name] = []
            db_collections[db_name].append(table.table_name)

        for db_name, collections in db_collections.items():
            print(f"  Database '{db_name}': {', '.join(collections)}")

        print(
            f"\nTotal SQL statements generated: {len(result['sql_statements'])}")

    except Exception as e:
        logger.error(f"Failed to create cross-database CDC pipeline: {e}")


def example_mongodb_discovery():
    """Demonstrate MongoDB discovery capabilities."""

    mongodb_config = MongoDBConfig(
        mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
        collection_name="test.*"  # This is required but not used for discovery
    )

    try:
        # No RisingWave client needed for discovery
        builder = ConnectBuilder(None)

        # Discover databases
        print("🔍 Discovering MongoDB databases...")
        databases = builder.get_mongodb_databases(mongodb_config)
        print(f"Available databases: {', '.join(databases)}")

        # Discover collections in specific databases
        for db_name in databases[:2]:  # Limit to first 2 databases
            print(f"\n📁 Collections in database '{db_name}':")
            collections = builder.discover_mongodb_collections(
                mongodb_config, db_name)

            for collection in collections:
                size_mb = collection.size_bytes / \
                    (1024 * 1024) if collection.size_bytes else 0
                print(
                    f"  - {collection.table_name}: {collection.row_count:,} docs, {size_mb:.1f} MB")

    except Exception as e:
        logger.error(f"Failed to discover MongoDB resources: {e}")


def example_mongodb_custom_table_creation():
    """Example of creating MongoDB CDC tables with custom configurations."""

    rw_client = RisingWaveClient(host="localhost", port=4566)

    mongodb_config = MongoDBConfig(
        mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
        collection_name="ecommerce.orders"
    )

    try:
        builder = ConnectBuilder(rw_client)

        # Create connection with specific options
        result = builder.create_mongodb_connection(
            mongodb_config,
            include_commit_timestamp=True,
            include_database_name=True,
            include_collection_name=True,
            dry_run=True
        )

        print("🔍 Custom MongoDB CDC table configuration:")
        for sql in result["sql_statements"]:
            print(f"\n{sql}")

    except Exception as e:
        logger.error(f"Failed to create custom MongoDB CDC table: {e}")


if __name__ == "__main__":
    print("2. Multi-collection CDC with patterns:")
    example_mongodb_cdc_multi_collection()

    print("\n" + "="*60 + "\n")

    print("3. Cross-database CDC:")
    example_mongodb_cdc_cross_database()

    print("\n" + "="*60 + "\n")

    print("4. MongoDB discovery:")
    example_mongodb_discovery()

    print("\n" + "="*60 + "\n")

    print("5. Custom table creation:")
    example_mongodb_custom_table_creation()

    print("\n✨ All examples completed! Set dry_run=False to execute the SQL.")
