"""PostgreSQL CDC pipeline implementation."""

from __future__ import annotations
import logging
from typing import Dict, Optional

from pydantic import BaseModel

from ..client import RisingWaveClient
from ..models import Source, Table
from ..sql_builders import build_create_source_postgres_cdc, build_create_table_from_source


logger = logging.getLogger(__name__)


class PostgresCDCConfig(BaseModel):
    """Configuration for PostgreSQL CDC pipeline."""
    
    # PostgreSQL connection
    postgres_host: str
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = ""
    postgres_database: str
    postgres_schema: str = "public"
    
    # CDC configuration
    slot_name: Optional[str] = None
    publication_name: str = "rw_publication"
    publication_create_enable: bool = True
    auto_schema_change: bool = False
    
    # SSL configuration
    ssl_mode: Optional[str] = None
    ssl_root_cert: Optional[str] = None
    
    # Advanced Debezium properties
    debezium_properties: Dict[str, str] = {}
    
    # Extra WITH clause properties
    extra_properties: Dict[str, str] = {}


class PostgresCDCPipeline:
    """Pipeline for creating PostgreSQL CDC sources and tables in RisingWave."""
    
    def __init__(
        self,
        client: RisingWaveClient,
        config: Optional[PostgresCDCConfig] = None,
        *,
        # Direct config parameters (alternative to config object)
        postgres_host: Optional[str] = None,
        postgres_port: int = 5432,
        postgres_user: str = "postgres", 
        postgres_password: str = "",
        postgres_database: Optional[str] = None,
        postgres_schema: str = "public",
        **kwargs
    ):
        """Initialize PostgreSQL CDC pipeline.
        
        Args:
            client: RisingWave client instance
            config: Optional PostgresCDCConfig object
            postgres_host: PostgreSQL host (if not using config)
            postgres_port: PostgreSQL port
            postgres_user: PostgreSQL username
            postgres_password: PostgreSQL password  
            postgres_database: PostgreSQL database name (if not using config)
            postgres_schema: PostgreSQL schema
            **kwargs: Additional config parameters
        """
        self.client = client
        
        if config:
            self.config = config
        else:
            if not postgres_host or not postgres_database:
                raise ValueError("postgres_host and postgres_database are required")
            
            self.config = PostgresCDCConfig(
                postgres_host=postgres_host,
                postgres_port=postgres_port,
                postgres_user=postgres_user,
                postgres_password=postgres_password,
                postgres_database=postgres_database,
                postgres_schema=postgres_schema,
                **kwargs
            )
    
    def create_source(
        self,
        name: str,
        *,
        schema: str = "public",
        slot_name: Optional[str] = None,
        publication_name: Optional[str] = None,
        publication_create_enable: Optional[bool] = None,
        auto_schema_change: Optional[bool] = None,
        ssl_mode: Optional[str] = None,
        ssl_root_cert: Optional[str] = None,
        debezium_properties: Optional[Dict[str, str]] = None,
        extra_properties: Optional[Dict[str, str]] = None,
        if_not_exists: bool = True,
    ) -> Source:
        """Create a PostgreSQL CDC source.
        
        Args:
            name: Source name
            schema: RisingWave schema to create source in
            slot_name: PostgreSQL replication slot name
            publication_name: PostgreSQL publication name
            publication_create_enable: Whether to auto-create publication
            auto_schema_change: Whether to enable automatic schema changes
            ssl_mode: SSL connection mode
            ssl_root_cert: SSL root certificate
            debezium_properties: Additional Debezium properties
            extra_properties: Additional WITH clause properties
            if_not_exists: Whether to use IF NOT EXISTS
            
        Returns:
            Source object representing the created source
        """
        # Use provided values or fall back to config defaults
        slot_name = slot_name or self.config.slot_name
        publication_name = publication_name or self.config.publication_name
        publication_create_enable = (
            publication_create_enable 
            if publication_create_enable is not None 
            else self.config.publication_create_enable
        )
        auto_schema_change = (
            auto_schema_change
            if auto_schema_change is not None
            else self.config.auto_schema_change
        )
        ssl_mode = ssl_mode or self.config.ssl_mode
        ssl_root_cert = ssl_root_cert or self.config.ssl_root_cert
        
        # Merge properties
        merged_debezium = {**self.config.debezium_properties}
        if debezium_properties:
            merged_debezium.update(debezium_properties)
            
        merged_extra = {**self.config.extra_properties}
        if extra_properties:
            merged_extra.update(extra_properties)
        
        # Check if source already exists
        if if_not_exists and self.client.source_exists(name, schema):
            logger.info(f"Source {schema}.{name} already exists, skipping creation")
            return Source(name=name, schema=schema, source_type="postgres-cdc")
        
        # Build and execute CREATE SOURCE SQL
        sql = build_create_source_postgres_cdc(
            f"{schema}.{name}" if schema != "public" else name,
            hostname=self.config.postgres_host,
            port=self.config.postgres_port,
            username=self.config.postgres_user,
            password=self.config.postgres_password,
            database=self.config.postgres_database,
            schema=self.config.postgres_schema,
            slot_name=slot_name,
            publication_name=publication_name,
            publication_create_enable=publication_create_enable,
            auto_schema_change=auto_schema_change,
            ssl_mode=ssl_mode,
            ssl_root_cert=ssl_root_cert,
            extra_with=merged_extra if merged_extra else None,
            debezium_overrides=merged_debezium if merged_debezium else None,
        )
        
        # Add IF NOT EXISTS if requested
        if if_not_exists:
            sql = sql.replace("CREATE SOURCE", "CREATE SOURCE IF NOT EXISTS")
        
        self.client.execute(sql)
        logger.info(f"Created PostgreSQL CDC source: {schema}.{name}")
        
        return Source(name=name, schema=schema, source_type="postgres-cdc")
    
    def create_table(
        self,
        name: str,
        source: Source,
        postgres_table: str,
        *,
        schema: str = "public",
        columns_ddl: Optional[list[str]] = None,
        snapshot: Optional[bool] = None,
        include_timestamp_as: Optional[str] = None,
        if_not_exists: bool = True,
    ) -> Table:
        """Create a table from a PostgreSQL CDC source.
        
        Args:
            name: Table name in RisingWave
            source: Source object to create table from
            postgres_table: Source PostgreSQL table in format 'schema.table'
            schema: RisingWave schema to create table in
            columns_ddl: Optional explicit column definitions, uses (*) if None
            snapshot: Whether to enable initial snapshot
            include_timestamp_as: Include timestamp metadata as this column name
            if_not_exists: Whether to use IF NOT EXISTS
            
        Returns:
            Table object representing the created table
        """
        # Check if table already exists
        if if_not_exists and self.client.table_exists(name, schema):
            logger.info(f"Table {schema}.{name} already exists, skipping creation")
            return Table(name=name, schema=schema, source=source)
        
        # Build and execute CREATE TABLE SQL
        sql = build_create_table_from_source(
            name,
            source.qualified_name,
            pg_table=postgres_table,
            schema=schema if schema != "public" else None,
            columns_ddl=columns_ddl,
            snapshot=snapshot,
            include_timestamp_as=include_timestamp_as,
        )
        
        # Add IF NOT EXISTS if requested
        if if_not_exists:
            sql = sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        
        self.client.execute(sql)
        logger.info(f"Created table: {schema}.{name} from {postgres_table}")
        
        return Table(name=name, schema=schema, source=source)
    
    def create_pipeline(
        self,
        source_name: str,
        tables: Dict[str, str],
        *,
        source_schema: str = "public",
        table_schema: str = "public",
        **source_kwargs
    ) -> tuple[Source, list[Table]]:
        """Create a complete pipeline with one source and multiple tables.
        
        Args:
            source_name: Name for the CDC source
            tables: Dict mapping RisingWave table names to PostgreSQL table names
            source_schema: Schema for the source
            table_schema: Schema for the tables  
            **source_kwargs: Additional arguments for create_source
            
        Returns:
            Tuple of (created_source, list_of_created_tables)
        """
        # Create the source
        source = self.create_source(source_name, schema=source_schema, **source_kwargs)
        
        # Create all tables
        created_tables = []
        for rw_table_name, pg_table_name in tables.items():
            table = self.create_table(
                rw_table_name,
                source,
                pg_table_name,
                schema=table_schema
            )
            created_tables.append(table)
        
        logger.info(f"Created pipeline with source {source.qualified_name} and {len(created_tables)} tables")
        return source, created_tables
