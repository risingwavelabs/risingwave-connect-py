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

    def dsn(self) -> str:
        """Build PostgreSQL connection string for RisingWave."""
        if self.password:
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"


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
