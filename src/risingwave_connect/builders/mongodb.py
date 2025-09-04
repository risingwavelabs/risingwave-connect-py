"""MongoDB source builder."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union

from .base import BaseSourceBuilder
from ..discovery.base import TableSelector, TableInfo
from ..sources.mongodb import MongoDBConfig, MongoDBDiscovery, MongoDBSourceConnection

logger = logging.getLogger(__name__)


class MongoDBBuilder(BaseSourceBuilder):
    """MongoDB CDC source builder."""

    def create_connection(
        self,
        config: MongoDBConfig,
        table_selector: Optional[Union[TableSelector, List[str]]] = None,
        dry_run: bool = False,
        include_commit_timestamp: bool = False,
        include_database_name: bool = False,
        include_collection_name: bool = False
    ) -> Dict[str, Any]:
        """Create a complete MongoDB CDC connection with collection discovery."""
        # Initialize discovery and connection
        discovery = MongoDBDiscovery(config)
        mongodb_source = MongoDBSourceConnection(self.rw_client, config)

        # Test connection (skip in dry run mode)
        if not dry_run:
            connection_test = discovery.test_connection()
            if not connection_test.get("success"):
                raise ConnectionError(
                    f"Cannot connect to MongoDB at {config.mongodb_url}")

        # For MongoDB, we need to discover collections based on the patterns in config
        available_tables = []

        # Parse collection patterns from config
        patterns = config.get_collection_patterns()

        for pattern in patterns:
            if '.' in pattern:
                db_part, collection_part = pattern.split('.', 1)

                # If it's a wildcard pattern like 'db.*', discover all collections in that database
                if collection_part == '*':
                    collections = discovery.list_tables(db_part)
                    available_tables.extend(collections)
                else:
                    # Specific collection - check if it exists
                    specific_tables = discovery.check_specific_tables([
                                                                      pattern])
                    available_tables.extend(specific_tables)
            else:
                # Pattern without database - use default database or error
                if config.database_name:
                    full_pattern = f"{config.database_name}.{pattern}"
                    specific_tables = discovery.check_specific_tables(
                        [full_pattern])
                    available_tables.extend(specific_tables)
                else:
                    logger.warning(
                        f"Collection pattern '{pattern}' lacks database name and no default database specified")

        logger.info(f"Found {len(available_tables)} collections")

        # Select collections: if table_selector is not specified, use all discovered collections
        if table_selector is None:
            table_selector = TableSelector(include_all=True)
        elif isinstance(table_selector, list):
            # Convert list of collection names to TableSelector
            table_selector = TableSelector(specific_tables=table_selector)

        selected_tables = table_selector.select_tables(available_tables)
        logger.info(f"Selected {len(selected_tables)} collections for CDC")

        # Generate SQL
        sql_statements = []

        # Add source creation
        sql_statements.append(mongodb_source.create_source_sql())

        # Add table creations
        for table in selected_tables:
            table_sql = mongodb_source.create_table_sql(
                table,
                include_commit_timestamp=include_commit_timestamp,
                include_database_name=include_database_name,
                include_collection_name=include_collection_name
            )
            sql_statements.append(table_sql)

        # Execute SQL statements if not dry run
        execution_results = []
        if not dry_run and sql_statements:
            try:
                # Execute source creation
                source_result = self.rw_client.execute_sql(sql_statements[0])
                execution_results.append({
                    "sql": sql_statements[0],
                    "success": source_result.get("success", True),
                    "message": source_result.get("message", "Source created")
                })

                # Execute table creations
                for i, table_sql in enumerate(sql_statements[1:], 1):
                    try:
                        table_result = self.rw_client.execute_sql(table_sql)
                        execution_results.append({
                            "sql": table_sql,
                            "success": table_result.get("success", True),
                            "message": table_result.get("message", f"Table {i} created")
                        })
                    except Exception as e:
                        execution_results.append({
                            "sql": table_sql,
                            "success": False,
                            "message": f"Failed to execute table SQL: {str(e)}"
                        })

            except Exception as e:
                logger.error(f"Failed to execute source SQL: {e}")
                execution_results.append({
                    "sql": sql_statements[0],
                    "success": False,
                    "message": f"Failed to create source: {str(e)}"
                })

        return {
            "success_summary": {
                "overall_success": True,
                "total_tables": len(selected_tables),
                "successful_tables": len(selected_tables),
                "failed_tables": 0
            },
            "selected_tables": selected_tables,
            "sql_statements": sql_statements,
            "execution_results": execution_results if not dry_run else [],
            "dry_run": dry_run,
            "executed": not dry_run
        }

    def discover_tables(
        self,
        config: MongoDBConfig,
        database_name: Optional[str] = None
    ) -> List[TableInfo]:
        """Discover available collections in MongoDB database."""
        discovery = MongoDBDiscovery(config)

        connection_test = discovery.test_connection()
        if not connection_test.get("success"):
            raise ConnectionError(
                f"Cannot connect to MongoDB at {config.mongodb_url}")

        return discovery.list_tables(database_name)

    def get_schemas(self, config: MongoDBConfig) -> List[str]:
        """Get list of available databases in MongoDB."""
        discovery = MongoDBDiscovery(config)

        connection_test = discovery.test_connection()
        if not connection_test.get("success"):
            raise ConnectionError(
                f"Cannot connect to MongoDB at {config.mongodb_url}")

        return discovery.list_schemas()
