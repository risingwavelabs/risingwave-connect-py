"""MongoDB-specific discovery and pipeline implementation."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Union
from contextlib import contextmanager

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from pydantic import BaseModel, Field, field_validator

from ..discovery.base import (
    DatabaseDiscovery,
    SourceConnection,
    SourceConfig,
    TableInfo,
    ColumnInfo,
    ColumnSelection
)

logger = logging.getLogger(__name__)


class MongoDBConfig(SourceConfig):
    """MongoDB-specific configuration.

    Args:
        mongodb_url: MongoDB connection string (e.g., 'mongodb://localhost:27017/?replicaSet=rs0')
        collection_name: Collection name(s) to ingest data from. Use database.collection format.
                        Supports patterns like 'db.*' for all collections in a database,
                        or 'db1.*, db2.*' for collections across multiple databases.
        database_name: MongoDB database name (optional, can be inferred from mongodb_url or collection_name)
    """

    mongodb_url: str
    collection_name: str
    database_name: Optional[str] = None

    hostname: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None

    @field_validator('mongodb_url')
    @classmethod
    def validate_mongodb_url(cls, v):
        """Validate MongoDB URL format."""
        if not v:
            raise ValueError("mongodb_url is required")
        if not v.startswith(('mongodb://', 'mongodb+srv://')):
            raise ValueError(
                "mongodb_url must start with 'mongodb://' or 'mongodb+srv://'")
        return v

    @field_validator('collection_name')
    @classmethod
    def validate_collection_name(cls, v):
        """Validate collection name pattern."""
        if not v:
            raise ValueError("collection_name is required")
        return v

    def get_database_names(self) -> List[str]:
        """Extract database names from collection patterns."""
        databases = set()
        for pattern in self.collection_name.split(','):
            pattern = pattern.strip()
            if '.' in pattern:
                db_name = pattern.split('.')[0]
                databases.add(db_name)
            elif self.database_name:
                databases.add(self.database_name)
        return list(databases)

    def get_collection_patterns(self) -> List[str]:
        """Get list of collection patterns."""
        return [pattern.strip() for pattern in self.collection_name.split(',')]


class MongoDBDiscovery(DatabaseDiscovery):
    """MongoDB database discovery implementation."""

    def __init__(self, config: MongoDBConfig):
        self.config = config
        self._client = None

    @contextmanager
    def _connection(self):
        """Get MongoDB connection."""
        client = MongoClient(self.config.mongodb_url,
                             serverSelectionTimeoutMS=5000)
        try:
            yield client
        finally:
            client.close()

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self._connection() as client:
                # Test connection by pinging the server
                client.admin.command('ping')
                return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def list_schemas(self) -> List[str]:
        """List all databases (equivalent to schemas in relational DBs)."""
        with self._connection() as client:
            # Filter out system databases
            system_dbs = {'admin', 'config', 'local'}
            return [db for db in client.list_database_names() if db not in system_dbs]

    def list_tables(self, schema_name: Optional[str] = None) -> List[TableInfo]:
        """List collections in specified database or all databases."""
        tables = []

        with self._connection() as client:
            if schema_name:
                if schema_name in client.list_database_names():
                    db = client[schema_name]
                    for collection_name in db.list_collection_names():
                        if not collection_name.startswith('system.'):
                            try:
                                stats = db.command(
                                    "collStats", collection_name)
                                doc_count = stats.get('count', 0)
                                size_bytes = stats.get('size', 0)
                            except Exception:
                                doc_count = 0
                                size_bytes = 0

                            tables.append(TableInfo(
                                schema_name=schema_name,
                                table_name=collection_name,
                                table_type='COLLECTION',
                                row_count=doc_count,
                                size_bytes=size_bytes,
                                comment=f"MongoDB collection"
                            ))
            else:
                for db_name in self.list_schemas():
                    db = client[db_name]
                    for collection_name in db.list_collection_names():
                        if not collection_name.startswith('system.'):
                            try:
                                stats = db.command(
                                    "collStats", collection_name)
                                doc_count = stats.get('count', 0)
                                size_bytes = stats.get('size', 0)
                            except Exception:
                                doc_count = 0
                                size_bytes = 0

                            tables.append(TableInfo(
                                schema_name=db_name,
                                table_name=collection_name,
                                table_type='COLLECTION',
                                row_count=doc_count,
                                size_bytes=size_bytes,
                                comment=f"MongoDB collection"
                            ))

        return tables

    def check_specific_tables(self, table_names: List[str], schema_name: Optional[str] = None) -> List[TableInfo]:
        """Check if specific collections exist and return their info.

        Args:
            table_names: List of collection names to check (can include database.collection format)
            schema_name: Default database if collection names don't include database

        Returns:
            List of TableInfo for collections that exist
        """
        if not table_names:
            return []

        tables = []

        with self._connection() as client:
            for table_name in table_names:
                if '.' in table_name:
                    db_name, collection_name = table_name.split('.', 1)
                else:
                    db_name = schema_name or self.config.database_name or 'test'
                    collection_name = table_name

                if db_name in client.list_database_names():
                    db = client[db_name]
                    if collection_name in db.list_collection_names():
                        try:
                            stats = db.command("collStats", collection_name)
                            doc_count = stats.get('count', 0)
                            size_bytes = stats.get('size', 0)
                        except Exception:
                            doc_count = 0
                            size_bytes = 0

                        tables.append(TableInfo(
                            schema_name=db_name,
                            table_name=collection_name,
                            table_type='COLLECTION',
                            row_count=doc_count,
                            size_bytes=size_bytes,
                            comment=f"MongoDB collection"
                        ))

        return tables

    def get_table_columns(self, schema_name: str, table_name: str) -> List[ColumnInfo]:
        """Get column information for a MongoDB collection.

        For MongoDB CDC, we typically have a standard schema:
        - _id: Document ID (Primary Key)
        - payload: JSONB containing the full document
        """
        # MongoDB CDC in RisingWave typically uses this standard schema
        columns = [
            ColumnInfo(
                column_name="_id",
                data_type="jsonb",
                is_nullable=False,
                is_primary_key=True,
                ordinal_position=1
            ),
            ColumnInfo(
                column_name="payload",
                data_type="jsonb",
                is_nullable=True,
                is_primary_key=False,
                ordinal_position=2
            )
        ]

        return columns

    def validate_column_selection(self, table_info: TableInfo, column_selections: List[ColumnSelection]) -> Dict[str, Any]:
        """Validate column selection against MongoDB CDC schema.

        Args:
            table_info: Collection information
            column_selections: List of column selections

        Returns:
            Dictionary with validation results
        """
        actual_columns = self.get_table_columns(
            table_info.schema_name, table_info.table_name)
        actual_column_map = {col.column_name: col for col in actual_columns}

        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'column_mapping': {},
            'primary_key_columns': [],
            'missing_columns': [],
            'type_overrides': {}
        }

        selected_column_names = [col.column_name for col in column_selections]
        pk_columns = [
            col.column_name for col in column_selections if col.is_primary_key]

        for col_selection in column_selections:
            col_name = col_selection.column_name

            if col_name not in actual_column_map:
                validation_result['valid'] = False
                validation_result['errors'].append(
                    f"Column '{col_name}' is not valid for MongoDB CDC. "
                    f"Valid columns are: {list(actual_column_map.keys())}"
                )
                validation_result['missing_columns'].append(col_name)
                continue

            actual_col = actual_column_map[col_name]

            if col_selection.risingwave_type:
                rw_type = col_selection.risingwave_type
                validation_result['type_overrides'][col_name] = rw_type
            else:
                rw_type = "JSONB" if actual_col.data_type == "jsonb" else "VARCHAR"

            if col_selection.is_primary_key and not actual_col.is_primary_key:
                validation_result['errors'].append(
                    f"Column '{col_name}' is marked as primary key in selection but is not a primary key in MongoDB CDC schema"
                )
                validation_result['valid'] = False
            elif actual_col.is_primary_key and not col_selection.is_primary_key:
                validation_result['warnings'].append(
                    f"Column '{col_name}' is a primary key in MongoDB CDC schema but not marked as such in selection"
                )

            validation_result['column_mapping'][col_name] = {
                'mongodb_type': actual_col.data_type,
                'risingwave_type': rw_type,
                'is_nullable': col_selection.is_nullable if col_selection.is_nullable is not None else actual_col.is_nullable,
                'is_primary_key': col_selection.is_primary_key,
                'ordinal_position': actual_col.ordinal_position
            }

        if not pk_columns:
            validation_result['errors'].append(
                "No primary key columns specified. The '_id' column must be included and marked as primary key for MongoDB CDC."
            )
            validation_result['valid'] = False

        if '_id' not in selected_column_names:
            validation_result['errors'].append(
                "The '_id' column must be included for MongoDB CDC."
            )
            validation_result['valid'] = False
        elif '_id' not in pk_columns:
            validation_result['errors'].append(
                "The '_id' column must be marked as primary key for MongoDB CDC."
            )
            validation_result['valid'] = False

        validation_result['primary_key_columns'] = pk_columns

        return validation_result


class MongoDBSourceConnection(SourceConnection):
    """MongoDB CDC source connection implementation."""

    def __init__(self, rw_client, config: MongoDBConfig):
        super().__init__(rw_client, config)
        self.config: MongoDBConfig = config

    def _generate_source_name(self) -> str:
        """Generate a default source name for MongoDB."""
        db_names = self.config.get_database_names()
        if db_names:
            clean_db = db_names[0].replace(
                '-', '_').replace('.', '_').replace(' ', '_')
            base_name = f"mongodb_cdc_{clean_db}"
            if len(db_names) > 1:
                base_name += "_multi"
        else:
            base_name = "mongodb_cdc_source"

        return base_name

    def create_source_sql(self) -> str:
        """Generate CREATE SOURCE SQL for MongoDB CDC."""
        with_items = [
            "connector='mongodb-cdc'",
            f"mongodb.url='{self._escape_sql_string(self.config.mongodb_url)}'",
            f"collection.name='{self._escape_sql_string(self.config.collection_name)}'",
        ]

        with_clause = ",\n    ".join(with_items)

        return f"""-- Step 1: Create the MongoDB CDC source {self.config.source_name}
CREATE SOURCE IF NOT EXISTS {self.config.source_name} WITH (
    {with_clause}
);"""

    def create_table_sql(self, table_info: TableInfo, **kwargs) -> str:
        """Generate CREATE TABLE SQL for MongoDB CDC.

        Args:
            table_info: Collection information
            **kwargs: Additional parameters including:
                - table_name: Override table name in RisingWave
                - rw_schema: RisingWave schema name
                - include_commit_timestamp: Whether to include commit timestamp (default: False)
                - include_database_name: Whether to include database name metadata (default: False)
                - include_collection_name: Whether to include collection name metadata (default: False)
                - column_config: TableColumnConfig for column filtering (usually not needed for MongoDB)
        """
        table_name = kwargs.get('table_name', table_info.table_name)
        rw_schema = kwargs.get('rw_schema', 'public')
        include_commit_timestamp = kwargs.get(
            'include_commit_timestamp', False)
        include_database_name = kwargs.get('include_database_name', False)
        include_collection_name = kwargs.get('include_collection_name', False)

        qualified_table_name = f"{rw_schema}.{table_name}" if rw_schema != "public" else table_name

        columns = ["_id JSONB PRIMARY KEY", "payload JSONB"]

        include_clauses = []
        if include_commit_timestamp:
            include_clauses.append("INCLUDE TIMESTAMP AS commit_ts")
        if include_database_name:
            include_clauses.append("INCLUDE DATABASE_NAME AS database_name")
        if include_collection_name:
            include_clauses.append(
                "INCLUDE COLLECTION_NAME AS collection_name")

        # Create column definition
        columns_sql = ",\n    ".join(columns)

        include_sql = ""
        if include_clauses:
            include_sql = "\n" + "\n".join(include_clauses)

        # Format document count for comment (handle None case)
        doc_count_str = f"{table_info.row_count:,}" if table_info.row_count is not None else "unknown"

        return f"""-- MongoDB CDC Table: {qualified_table_name}
-- Source: {table_info.qualified_name} ({doc_count_str} documents)
CREATE TABLE IF NOT EXISTS {qualified_table_name} (
    {columns_sql}
){include_sql}
WITH (
    connector='mongodb-cdc',
    mongodb.url='{self._escape_sql_string(self.config.mongodb_url)}',
    collection.name='{self._escape_sql_string(table_info.qualified_name)}'
);"""

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes in SQL strings."""
        return value.replace("'", "''") if value else ""
