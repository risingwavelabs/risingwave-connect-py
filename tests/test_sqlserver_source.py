"""Tests for SQL Server CDC source implementation."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from risingwave_connect.sources.sqlserver import SQLServerConfig, SQLServerDiscovery, SQLServerSourceConnection
from risingwave_connect.discovery.base import TableInfo


class TestSQLServerConfig:
    """Test SQL Server configuration validation."""

    def test_valid_config(self):
        """Test valid SQL Server configuration."""
        config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.users"
        )
        assert config.hostname == "localhost"
        assert config.port == 1433
        assert config.username == "sa"
        assert config.database == "testdb"
        assert config.table_name == "dbo.users"
        assert config.schema_name == "dbo"  # default

    def test_custom_schema(self):
        """Test configuration with custom schema."""
        config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            schema_name="sales",
            table_name="sales.orders"
        )
        assert config.schema_name == "sales"

    def test_invalid_hostname(self):
        """Test invalid hostname validation."""
        with pytest.raises(ValueError, match="hostname is required"):
            SQLServerConfig(
                hostname="",
                port=1433,
                username="sa",
                password="password123",
                database="testdb",
                table_name="dbo.users"
            )

    def test_invalid_port(self):
        """Test invalid port validation."""
        with pytest.raises(ValueError, match="port must be between 1 and 65535"):
            SQLServerConfig(
                hostname="localhost",
                port=0,
                username="sa",
                password="password123",
                database="testdb",
                table_name="dbo.users"
            )

    def test_empty_database(self):
        """Test empty database validation."""
        with pytest.raises(ValueError, match="database is required"):
            SQLServerConfig(
                hostname="localhost",
                port=1433,
                username="sa",
                password="password123",
                database="",
                table_name="dbo.users"
            )

    def test_empty_table_name(self):
        """Test empty table name validation."""
        with pytest.raises(ValueError, match="table_name is required"):
            SQLServerConfig(
                hostname="localhost",
                port=1433,
                username="sa",
                password="password123",
                database="testdb",
                table_name=""
            )

    def test_get_schema_names(self):
        """Test extracting schema names from table patterns."""
        config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.*, sales.orders, hr.employees"
        )
        schema_names = config.get_schema_names()
        assert set(schema_names) == {"dbo", "sales", "hr"}

    def test_get_table_patterns(self):
        """Test getting table patterns."""
        config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.*, sales.orders, hr.employees"
        )
        patterns = config.get_table_patterns()
        assert patterns == ["dbo.*", "sales.orders", "hr.employees"]

    def test_get_connection_string(self):
        """Test connection string generation."""
        config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.users",
            database_encrypt=True
        )
        conn_str = config.get_connection_string()
        assert "SERVER=localhost,1433" in conn_str
        assert "DATABASE=testdb" in conn_str
        assert "UID=sa" in conn_str
        assert "PWD=password123" in conn_str
        assert "Encrypt=yes" in conn_str

    def test_get_connection_string_no_encrypt(self):
        """Test connection string generation without encryption."""
        config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.users",
            database_encrypt=False
        )
        conn_str = config.get_connection_string()
        assert "Encrypt=no" in conn_str


class TestSQLServerDiscovery:
    """Test SQL Server discovery functionality."""

    def setup_method(self):
        """Set up test configuration."""
        self.config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.users"
        )

    @patch('risingwave_connect.sources.sqlserver.pyodbc')
    def test_test_connection_success(self, mock_pyodbc):
        """Test successful connection test."""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        # Mock query results
        mock_cursor.fetchone.side_effect = [
            MagicMock(version="Microsoft SQL Server 2019"),
            MagicMock(database_name="testdb", default_schema="dbo")
        ]

        discovery = SQLServerDiscovery(self.config)
        result = discovery.test_connection()

        assert result["success"] is True
        assert "Microsoft SQL Server 2019" in result["server_version"]
        assert result["database_name"] == "testdb"

    @patch('risingwave_connect.sources.sqlserver.pyodbc', None)
    def test_test_connection_no_pyodbc(self):
        """Test connection when pyodbc is not available."""
        discovery = SQLServerDiscovery(self.config)

        with pytest.raises(ImportError, match="pyodbc is required"):
            with discovery.get_connection():
                pass

    @patch('risingwave_connect.sources.sqlserver.pyodbc')
    def test_test_connection_failure(self, mock_pyodbc):
        """Test failed connection test."""
        mock_pyodbc.connect.side_effect = Exception("Connection failed")
        mock_pyodbc.Error = Exception

        discovery = SQLServerDiscovery(self.config)
        result = discovery.test_connection()

        assert result["success"] is False
        assert "Connection failed" in result["message"]

    @patch('risingwave_connect.sources.sqlserver.pyodbc')
    def test_list_schemas(self, mock_pyodbc):
        """Test listing schemas."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        # Mock schema results
        mock_cursor.fetchall.return_value = [
            MagicMock(SCHEMA_NAME="dbo"),
            MagicMock(SCHEMA_NAME="sales"),
            MagicMock(SCHEMA_NAME="hr")
        ]

        discovery = SQLServerDiscovery(self.config)
        schemas = discovery.list_schemas()

        assert schemas == ["dbo", "sales", "hr"]

    @patch('risingwave_connect.sources.sqlserver.pyodbc')
    def test_list_tables(self, mock_pyodbc):
        """Test listing tables."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        # Mock table results
        mock_cursor.fetchall.return_value = [
            MagicMock(TABLE_SCHEMA="dbo", TABLE_NAME="users",
                      TABLE_TYPE="BASE TABLE"),
            MagicMock(TABLE_SCHEMA="dbo", TABLE_NAME="orders",
                      TABLE_TYPE="BASE TABLE")
        ]

        discovery = SQLServerDiscovery(self.config)
        tables = discovery.list_tables("dbo")

        assert len(tables) == 2
        assert tables[0].schema_name == "dbo"
        assert tables[0].table_name == "users"
        assert tables[0].qualified_name == "dbo.users"
        # row_count is not fetched in list_tables
        assert tables[0].row_count is None

    @patch('risingwave_connect.sources.sqlserver.pyodbc')
    def test_check_specific_tables(self, mock_pyodbc):
        """Test checking specific tables."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        # Mock table results
        mock_cursor.fetchall.return_value = [
            MagicMock(TABLE_SCHEMA="dbo", TABLE_NAME="users", row_count=1000),
            MagicMock(TABLE_SCHEMA="dbo", TABLE_NAME="orders", row_count=5000),
            MagicMock(TABLE_SCHEMA="sales",
                      TABLE_NAME="customers", row_count=2000)
        ]

        discovery = SQLServerDiscovery(self.config)

        # Test specific table
        tables = discovery.check_specific_tables(["dbo.users"])
        assert len(tables) == 1
        assert tables[0].qualified_name == "dbo.users"

        # Test schema pattern
        tables = discovery.check_specific_tables(["dbo.*"])
        assert len(tables) == 2
        assert all(table.schema_name == "dbo" for table in tables)

    @patch('risingwave_connect.sources.sqlserver.pyodbc')
    def test_get_table_columns(self, mock_pyodbc):
        """Test getting table columns."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        # Mock column results
        mock_cursor.fetchall.return_value = [
            MagicMock(COLUMN_NAME="id", DATA_TYPE="int",
                      IS_NULLABLE="NO", ORDINAL_POSITION=1, IS_PRIMARY_KEY=1),
            MagicMock(COLUMN_NAME="name", DATA_TYPE="varchar",
                      IS_NULLABLE="YES", ORDINAL_POSITION=2, IS_PRIMARY_KEY=0),
            MagicMock(COLUMN_NAME="email", DATA_TYPE="varchar",
                      IS_NULLABLE="YES", ORDINAL_POSITION=3, IS_PRIMARY_KEY=0)
        ]

        discovery = SQLServerDiscovery(self.config)
        columns = discovery.get_table_columns("dbo", "users")

        assert len(columns) == 3
        assert columns[0].column_name == "id"
        assert columns[0].data_type == "INTEGER"  # mapped type
        assert columns[0].is_primary_key is True
        assert columns[0].is_nullable is False

        assert columns[1].column_name == "name"
        assert columns[1].data_type == "VARCHAR"
        assert columns[1].is_primary_key is False
        assert columns[1].is_nullable is True

    def test_map_sqlserver_type_to_risingwave(self):
        """Test SQL Server to RisingWave type mapping."""
        discovery = SQLServerDiscovery(self.config)

        # Test numeric types
        assert discovery._map_sqlserver_type_to_risingwave("int") == "INTEGER"
        assert discovery._map_sqlserver_type_to_risingwave(
            "bigint") == "BIGINT"
        assert discovery._map_sqlserver_type_to_risingwave(
            "decimal") == "DECIMAL"
        assert discovery._map_sqlserver_type_to_risingwave(
            "float") == "DOUBLE PRECISION"

        # Test string types
        assert discovery._map_sqlserver_type_to_risingwave(
            "varchar") == "VARCHAR"
        assert discovery._map_sqlserver_type_to_risingwave(
            "nvarchar") == "VARCHAR"
        assert discovery._map_sqlserver_type_to_risingwave("text") == "TEXT"

        # Test date/time types
        assert discovery._map_sqlserver_type_to_risingwave(
            "datetime") == "TIMESTAMP"
        assert discovery._map_sqlserver_type_to_risingwave("date") == "DATE"
        assert discovery._map_sqlserver_type_to_risingwave("time") == "TIME"

        # Test other types
        assert discovery._map_sqlserver_type_to_risingwave("bit") == "BOOLEAN"
        assert discovery._map_sqlserver_type_to_risingwave(
            "uniqueidentifier") == "UUID"
        assert discovery._map_sqlserver_type_to_risingwave(
            "unknown_type") == "TEXT"


class TestSQLServerSourceConnection:
    """Test SQL Server source connection."""

    def setup_method(self):
        """Set up test configuration."""
        self.config = SQLServerConfig(
            source_name="test_source",
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.users"
        )
        self.mock_client = Mock()

    def test_generate_source_name(self):
        """Test source name generation."""
        connection = SQLServerSourceConnection(self.mock_client, self.config)
        source_name = connection._generate_source_name()
        assert source_name == "sqlserver_cdc_testdb"

    def test_generate_source_name_with_special_chars(self):
        """Test source name generation with special characters in database name."""
        config = SQLServerConfig(
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="test-db.name",
            table_name="dbo.users"
        )
        connection = SQLServerSourceConnection(self.mock_client, config)
        source_name = connection._generate_source_name()
        assert source_name == "sqlserver_cdc_test_db_name"

    def test_create_source_sql(self):
        """Test SQL source creation."""
        connection = SQLServerSourceConnection(self.mock_client, self.config)
        sql = connection.create_source_sql()

        assert "CREATE SOURCE IF NOT EXISTS test_source" in sql
        assert "connector='sqlserver-cdc'" in sql
        assert "hostname='localhost'" in sql
        assert "port='1433'" in sql
        assert "username='sa'" in sql
        assert "password='password123'" in sql
        assert "database.name='testdb'" in sql

    def test_create_source_sql_with_encryption(self):
        """Test SQL source creation with encryption."""
        config = SQLServerConfig(
            source_name="test_source",
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.users",
            database_encrypt=True
        )
        connection = SQLServerSourceConnection(self.mock_client, config)
        sql = connection.create_source_sql()

        assert "database.encrypt='true'" in sql

    def test_create_table_sql_basic(self):
        """Test basic table SQL creation."""
        table_info = TableInfo(
            schema_name="dbo",
            table_name="users",
            row_count=1000
        )

        # Mock the discovery to return columns
        with patch('risingwave_connect.sources.sqlserver.SQLServerDiscovery') as mock_discovery_class:
            mock_discovery = mock_discovery_class.return_value
            mock_discovery.get_table_columns.return_value = [
                Mock(column_name="id", data_type="INTEGER",
                     is_nullable=False, is_primary_key=True, ordinal_position=1),
                Mock(column_name="name", data_type="VARCHAR",
                     is_nullable=True, is_primary_key=False, ordinal_position=2)
            ]

            connection = SQLServerSourceConnection(
                self.mock_client, self.config)
            sql = connection.create_table_sql(table_info)

            assert "CREATE TABLE IF NOT EXISTS users" in sql
            assert "id INTEGER PRIMARY KEY" in sql
            assert "name VARCHAR" in sql
            assert "FROM test_source TABLE 'dbo.users'" in sql
            assert "1,000 rows" in sql

    def test_create_table_sql_with_metadata(self):
        """Test table SQL creation with metadata columns."""
        table_info = TableInfo(
            schema_name="dbo",
            table_name="users",
            row_count=1000
        )

        # Mock the discovery to return columns
        with patch('risingwave_connect.sources.sqlserver.SQLServerDiscovery') as mock_discovery_class:
            mock_discovery = mock_discovery_class.return_value
            mock_discovery.get_table_columns.return_value = [
                Mock(column_name="id", data_type="INTEGER",
                     is_nullable=False, is_primary_key=True, ordinal_position=1)
            ]

            connection = SQLServerSourceConnection(
                self.mock_client, self.config)
            sql = connection.create_table_sql(
                table_info,
                include_timestamp=True,
                include_database_name=True,
                include_schema_name=True,
                include_table_name=True
            )

            assert "INCLUDE timestamp AS commit_ts" in sql
            assert "INCLUDE database_name AS database_name" in sql
            assert "INCLUDE schema_name AS schema_name" in sql
            assert "INCLUDE table_name AS table_name" in sql

    def test_create_table_sql_with_options(self):
        """Test table SQL creation with custom options."""
        config = SQLServerConfig(
            source_name="test_source",
            hostname="localhost",
            port=1433,
            username="sa",
            password="password123",
            database="testdb",
            table_name="dbo.users",
            snapshot=False,
            snapshot_interval=2,
            snapshot_batch_size=500
        )

        table_info = TableInfo(
            schema_name="dbo",
            table_name="users",
            row_count=1000
        )

        # Mock the discovery to return columns
        with patch('risingwave_connect.sources.sqlserver.SQLServerDiscovery') as mock_discovery_class:
            mock_discovery = mock_discovery_class.return_value
            mock_discovery.get_table_columns.return_value = [
                Mock(column_name="id", data_type="INTEGER",
                     is_nullable=False, is_primary_key=True, ordinal_position=1)
            ]

            connection = SQLServerSourceConnection(self.mock_client, config)
            sql = connection.create_table_sql(table_info)

            assert "snapshot='false'" in sql
            assert "snapshot.interval='2'" in sql
            assert "snapshot.batch_size='500'" in sql

    def test_escape_sql_string(self):
        """Test SQL string escaping."""
        connection = SQLServerSourceConnection(self.mock_client, self.config)

        # Test single quote escaping
        escaped = connection._escape_sql_string("test's string")
        assert escaped == "test''s string"

        # Test multiple quotes
        escaped = connection._escape_sql_string("'quoted' 'string'")
        assert escaped == "''quoted'' ''string''"

        # Test no quotes
        escaped = connection._escape_sql_string("normal string")
        assert escaped == "normal string"
