"""Tests for MongoDB CDC source implementation."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from risingwave_connect.sources.mongodb import MongoDBConfig, MongoDBDiscovery, MongoDBSourceConnection
from risingwave_connect.discovery.base import TableInfo


class TestMongoDBConfig:
    """Test MongoDB configuration validation."""

    def test_valid_config(self):
        """Test valid MongoDB configuration."""
        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )
        assert config.mongodb_url == "mongodb://localhost:27017/?replicaSet=rs0"
        assert config.collection_name == "mydb.users"

    def test_invalid_mongodb_url(self):
        """Test invalid MongoDB URL validation."""
        with pytest.raises(ValueError, match="mongodb_url must start with"):
            MongoDBConfig(
                mongodb_url="invalid://localhost:27017",
                collection_name="mydb.users"
            )

    def test_empty_collection_name(self):
        """Test empty collection name validation."""
        with pytest.raises(ValueError, match="collection_name is required"):
            MongoDBConfig(
                mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
                collection_name=""
            )

    def test_get_database_names(self):
        """Test extracting database names from collection patterns."""
        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="db1.*, db2.orders, db3.events"
        )
        db_names = config.get_database_names()
        assert set(db_names) == {"db1", "db2", "db3"}

    def test_get_collection_patterns(self):
        """Test getting collection patterns."""
        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="db1.*, db2.orders, db3.events"
        )
        patterns = config.get_collection_patterns()
        assert patterns == ["db1.*", "db2.orders", "db3.events"]


class TestMongoDBDiscovery:
    """Test MongoDB discovery functionality."""

    @patch('risingwave_connect.sources.mongodb.MongoClient')
    def test_test_connection_success(self, mock_mongo_client):
        """Test successful connection test."""
        mock_client = Mock()
        mock_client_instance = Mock()
        mock_client_instance.admin.command = Mock()
        mock_mongo_client.return_value = mock_client_instance

        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )
        discovery = MongoDBDiscovery(config)

        assert discovery.test_connection() is True
        mock_client_instance.admin.command.assert_called_once_with('ping')

    @patch('risingwave_connect.sources.mongodb.MongoClient')
    def test_test_connection_failure(self, mock_mongo_client):
        """Test connection test failure."""
        from pymongo.errors import ConnectionFailure
        mock_client_instance = Mock()
        mock_client_instance.admin.command.side_effect = ConnectionFailure(
            "Connection failed")
        mock_mongo_client.return_value = mock_client_instance

        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )
        discovery = MongoDBDiscovery(config)

        assert discovery.test_connection() is False

    @patch('risingwave_connect.sources.mongodb.MongoClient')
    def test_list_schemas(self, mock_mongo_client):
        """Test listing databases (schemas)."""
        mock_client_instance = Mock()
        mock_client_instance.list_database_names.return_value = [
            "admin", "config", "local", "mydb", "testdb"]
        mock_mongo_client.return_value = mock_client_instance

        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )
        discovery = MongoDBDiscovery(config)

        schemas = discovery.list_schemas()
        # System databases filtered out
        assert set(schemas) == {"mydb", "testdb"}

    @patch('risingwave_connect.sources.mongodb.MongoClient')
    def test_list_tables(self, mock_mongo_client):
        """Test listing collections in a database."""
        mock_client_instance = Mock()
        mock_client_instance.list_database_names.return_value = [
            "mydb", "testdb"]
        mock_db = Mock()
        mock_db.list_collection_names.return_value = [
            "users", "orders", "system.indexes"]
        mock_db.command.return_value = {"count": 100, "size": 1024}
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        mock_mongo_client.return_value = mock_client_instance

        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )
        discovery = MongoDBDiscovery(config)

        tables = discovery.list_tables("mydb")

        # Should filter out system collections
        table_names = [t.table_name for t in tables]
        assert set(table_names) == {"users", "orders"}
        assert all(t.table_type == "COLLECTION" for t in tables)

    @patch('risingwave_connect.sources.mongodb.MongoClient')
    def test_check_specific_tables(self, mock_mongo_client):
        """Test checking specific collections."""
        mock_client_instance = Mock()
        mock_client_instance.list_database_names.return_value = [
            "mydb", "testdb"]

        # Mock database access
        mock_db = Mock()
        mock_db.list_collection_names.return_value = ["users", "orders"]
        mock_db.command.return_value = {"count": 50, "size": 512}
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)

        mock_mongo_client.return_value = mock_client_instance

        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )
        discovery = MongoDBDiscovery(config)

        tables = discovery.check_specific_tables(
            ["mydb.users", "testdb.orders"])

        assert len(tables) == 2
        assert all(t.table_type == "COLLECTION" for t in tables)

    def test_get_table_columns(self):
        """Test getting table columns for MongoDB CDC."""
        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )
        discovery = MongoDBDiscovery(config)

        columns = discovery.get_table_columns("mydb", "users")

        assert len(columns) == 2
        assert columns[0].column_name == "_id"
        assert columns[0].is_primary_key is True
        assert columns[1].column_name == "payload"
        assert columns[1].is_primary_key is False


class TestMongoDBSourceConnection:
    """Test MongoDB source connection."""

    def test_generate_source_name(self):
        """Test automatic source name generation."""
        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="my-db.users, test_db.orders"
        )

        mock_client = Mock()
        connection = MongoDBSourceConnection(mock_client, config)

        # Should contain mongodb_cdc and one of the database names
        source_name = connection.config.source_name
        assert "mongodb_cdc" in source_name
        assert any(db in source_name for db in ["my_db", "test_db"])
        assert "multi" in source_name  # Should indicate multiple databases

    def test_create_source_sql(self):
        """Test CREATE SOURCE SQL generation."""
        config = MongoDBConfig(
            source_name="test_mongodb_source",
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.*"
        )

        mock_client = Mock()
        connection = MongoDBSourceConnection(mock_client, config)

        sql = connection.create_source_sql()

        assert "CREATE SOURCE IF NOT EXISTS test_mongodb_source" in sql
        assert "connector='mongodb-cdc'" in sql
        assert "mongodb.url='mongodb://localhost:27017/?replicaSet=rs0'" in sql
        assert "collection.name='mydb.*'" in sql

    def test_create_table_sql_basic(self):
        """Test basic CREATE TABLE SQL generation."""
        config = MongoDBConfig(
            source_name="test_mongodb_source",
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )

        mock_client = Mock()
        connection = MongoDBSourceConnection(mock_client, config)

        table_info = TableInfo(
            schema_name="mydb",
            table_name="users",
            table_type="COLLECTION"
        )

        sql = connection.create_table_sql(table_info)

        assert "CREATE TABLE IF NOT EXISTS users" in sql
        assert "_id JSONB PRIMARY KEY" in sql
        assert "payload JSONB" in sql
        assert "connector='mongodb-cdc'" in sql
        assert "collection.name='mydb.users'" in sql

    def test_create_table_sql_with_metadata(self):
        """Test CREATE TABLE SQL with metadata columns."""
        config = MongoDBConfig(
            source_name="test_mongodb_source",
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )

        mock_client = Mock()
        connection = MongoDBSourceConnection(mock_client, config)

        table_info = TableInfo(
            schema_name="mydb",
            table_name="users",
            table_type="COLLECTION"
        )

        sql = connection.create_table_sql(
            table_info,
            include_commit_timestamp=True,
            include_database_name=True,
            include_collection_name=True
        )

        assert "INCLUDE TIMESTAMP AS commit_ts" in sql
        assert "DATABASE_NAME AS database_name" in sql
        assert "COLLECTION_NAME AS collection_name" in sql

    def test_escape_sql_string(self):
        """Test SQL string escaping."""
        config = MongoDBConfig(
            mongodb_url="mongodb://localhost:27017/?replicaSet=rs0",
            collection_name="mydb.users"
        )

        mock_client = Mock()
        connection = MongoDBSourceConnection(mock_client, config)

        # Test escaping single quotes
        escaped = connection._escape_sql_string("test'string'with'quotes")
        assert escaped == "test''string''with''quotes"

        # Test empty string
        escaped = connection._escape_sql_string("")
        assert escaped == ""

        # Test None
        escaped = connection._escape_sql_string(None)
        assert escaped == ""
