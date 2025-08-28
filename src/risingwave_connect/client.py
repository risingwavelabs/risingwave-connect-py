"""Core RisingWave client for executing SQL and managing connections."""

from __future__ import annotations
import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional
from urllib.parse import quote, urlencode

import psycopg
from pydantic import BaseModel


logger = logging.getLogger(__name__)


class RisingWaveConfig(BaseModel):
    """Configuration for RisingWave connection."""

    host: str = "localhost"
    port: int = 4566
    user: str = "root"
    password: Optional[str] = None
    database: str = "dev"
    schema_name: str = "public"

    # SSL Configuration
    # disabled, preferred, required, verify-ca, verify-full
    ssl_mode: Optional[str] = None
    ssl_root_cert: Optional[str] = None
    connect_timeout: int = 30

    def dsn(self) -> str:
        """Build PostgreSQL connection string for RisingWave."""
        # Build base DSN
        if self.password:
            base_dsn = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            base_dsn = f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"

        # Add SSL and connection parameters
        params = []
        if self.ssl_mode:  # Only add if explicitly provided
            params.append(f"sslmode={self.ssl_mode}")
        if self.ssl_root_cert:
            params.append(f"sslrootcert={quote(self.ssl_root_cert)}")
        if self.connect_timeout != 30:  # Only add if not default
            params.append(f"connect_timeout={self.connect_timeout}")

        if params:
            base_dsn += "?" + "&".join(params)

        return base_dsn


class RisingWaveClient:
    """Client for connecting to and executing commands on RisingWave."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        *,
        host: str = "localhost",
        port: int = 4566,
        user: str = "root",
        username: Optional[str] = None,  # Alias for user
        password: Optional[str] = None,
        database: str = "dev",
        schema: str = "public",
        ssl_mode: Optional[str] = None,
        ssl_root_cert: Optional[str] = None,
        connect_timeout: int = 30,
    ):
        """Initialize RisingWave client.

        Args:
            dsn: PostgreSQL connection string, if provided overrides other params
            host: RisingWave host
            port: RisingWave port  
            user: Username (legacy parameter)
            username: Username (preferred parameter, alias for user)
            password: Password
            database: Database name
            schema: Default schema
            ssl_mode: SSL mode (disabled, preferred, required, verify-ca, verify-full)
            ssl_root_cert: Path to SSL root certificate
            connect_timeout: Connection timeout in seconds
        """
        # Handle username alias
        if username is not None:
            user = username

        if dsn:
            self.config = RisingWaveConfig()  # Will parse from DSN if needed
            self._dsn = dsn
        else:
            self.config = RisingWaveConfig(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                schema_name=schema,
                ssl_mode=ssl_mode,
                ssl_root_cert=ssl_root_cert,
                connect_timeout=connect_timeout,
            )
            self._dsn = self.config.dsn()

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        """Get a database connection context manager."""
        with psycopg.connect(self._dsn, autocommit=True) as conn:
            yield conn

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """Execute SQL statement without returning results.

        Args:
            sql: SQL statement to execute
            params: Optional parameters for the SQL statement
        """
        logger.info(f"Executing SQL: {sql}")
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def fetch_all(self, sql: str, params: Optional[tuple] = None) -> list[tuple]:
        """Execute SQL and fetch all results.

        Args:
            sql: SQL query to execute
            params: Optional parameters for the SQL query

        Returns:
            List of result tuples
        """
        logger.debug(f"Fetching SQL: {sql}")
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def fetch_one(self, sql: str, params: Optional[tuple] = None) -> Optional[tuple]:
        """Execute SQL and fetch one result.

        Args:
            sql: SQL query to execute  
            params: Optional parameters for the SQL query

        Returns:
            Single result tuple or None
        """
        logger.debug(f"Fetching one SQL: {sql}")
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table
            schema: Schema name, defaults to client's default schema

        Returns:
            True if table exists, False otherwise
        """
        schema = schema or self.config.schema_name
        result = self.fetch_one(
            """
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = %s AND table_schema = %s
            """,
            (table_name, schema)
        )
        return result is not None

    def source_exists(self, source_name: str, schema: Optional[str] = None) -> bool:
        """Check if a source exists.

        Args:
            source_name: Name of the source
            schema: Schema name, defaults to client's default schema

        Returns:
            True if source exists, False otherwise  
        """
        schema = schema or self.config.schema_name
        result = self.fetch_one(
            """
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = %s AND table_schema = %s AND table_type = 'SOURCE'
            """,
            (source_name, schema)
        )
        return result is not None

    def health_check(self) -> bool:
        """Check if RisingWave is healthy and responsive.

        Returns:
            True if RisingWave is healthy, False otherwise
        """
        try:
            result = self.fetch_one("SELECT 1")
            return result == (1,)
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    def get_version(self) -> str:
        """Get RisingWave version.

        Returns:
            RisingWave version string
        """
        try:
            result = self.fetch_one("SELECT version()")
            return result[0] if result else "Unknown"
        except Exception as e:
            logger.warning(f"Failed to get version: {e}")
            return "Unknown"

    def list_schemas(self) -> list[str]:
        """List all schemas.

        Returns:
            List of schema names
        """
        results = self.fetch_all(
            """
            SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name NOT LIKE 'pg_%' 
            AND schema_name NOT IN ('information_schema', 'rw_catalog')
            ORDER BY schema_name
            """
        )
        return [row[0] for row in results]
