"""RisingWave Pipeline SDK CLI"""

from __future__ import annotations
import os
import logging
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table as RichTable
from rich.prompt import Confirm, Prompt
from rich import print as rprint

from .client import RisingWaveClient
from .pipeline_builder import PipelineBuilder
from .sources.postgresql import PostgreSQLConfig
from .discovery.base import TableSelector

# Set up logging and console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(
    help="RisingWave Pipeline SDK CLI - Create and manage streaming data pipelines."
)

postgres_app = typer.Typer(help="PostgreSQL CDC pipeline commands")
app.add_typer(postgres_app, name="postgres")


def env_default(name: str, default: str | None = None) -> str | None:
    """Get environment variable with RW_ prefix."""
    return os.environ.get(f"RW_{name}", default)


def pg_env_default(name: str, default: str | None = None) -> str | None:
    """Get environment variable with PG_ prefix."""
    return os.environ.get(f"PG_{name}", default)


@postgres_app.command()
def discover(
    # PostgreSQL connection
    pg_host: str = typer.Option(
        pg_env_default("HOST", "localhost"), help="PostgreSQL host; env PG_HOST"
    ),
    pg_port: int = typer.Option(
        int(pg_env_default("PORT", "5432")), help="PostgreSQL port; env PG_PORT"
    ),
    pg_user: str = typer.Option(
        pg_env_default("USER", "postgres"), help="PostgreSQL user; env PG_USER"
    ),
    pg_password: str = typer.Option(
        pg_env_default("PASSWORD", ""), help="PostgreSQL password; env PG_PASSWORD"
    ),
    pg_db: str = typer.Option(
        pg_env_default("DATABASE", "postgres"), help="PostgreSQL database; env PG_DATABASE"
    ),
    pg_schema: str = typer.Option(
        pg_env_default("SCHEMA", "public"), help="PostgreSQL schema; env PG_SCHEMA"
    ),
    ssl_mode: Optional[str] = typer.Option(
        None, help="SSL mode (disable, require, etc.)"),
    show_details: bool = typer.Option(
        False, help="Show detailed table information"),
):
    """Discover tables in PostgreSQL database."""

    try:
        # Create config
        config = PostgreSQLConfig(
            source_name="discovery_temp",  # Not used for discovery
            hostname=pg_host,
            port=pg_port,
            username=pg_user,
            password=pg_password,
            database=pg_db,
            schema_name=pg_schema,
            ssl_mode=ssl_mode,
        )

        # Create pipeline builder
        rw_client = RisingWaveClient()  # Dummy client for discovery only
        builder = PipelineBuilder(rw_client)

        # Discover tables
        console.print(f"\n🔍 Discovering tables in schema '{pg_schema}'...")
        tables = builder.discover_postgresql_tables(config, pg_schema)

        if not tables:
            console.print(f"❌ No tables found in schema '{pg_schema}'")
            return

        # Display tables
        console.print(f"✓ Found {len(tables)} tables in schema '{pg_schema}':")

        if show_details:
            # Detailed table with rich formatting
            rich_table = RichTable(title=f"Tables in {pg_schema}")
            rich_table.add_column("Table Name", style="cyan")
            rich_table.add_column("Type", style="green")
            rich_table.add_column("Row Count", justify="right", style="yellow")
            rich_table.add_column("Size (MB)", justify="right", style="blue")
            rich_table.add_column("Comment", style="dim")

            for table in tables:
                size_mb = f"{(table.size_bytes or 0) / 1024 / 1024:.1f}" if table.size_bytes else "N/A"
                row_count = str(table.row_count) if table.row_count else "N/A"
                rich_table.add_row(
                    table.table_name,
                    table.table_type,
                    row_count,
                    size_mb,
                    table.comment or ""
                )

            console.print(rich_table)
        else:
            # Simple list
            for i, table in enumerate(tables, 1):
                console.print(
                    f"  {i:2d}. {table.qualified_name} ({table.table_type})")

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@postgres_app.command()
def create_pipeline(
    # RisingWave connection
    rw_host: str = typer.Option(
        env_default("HOST", "localhost"), help="RisingWave host; env RW_HOST"
    ),
    rw_port: int = typer.Option(
        int(env_default("PORT", "4566")), help="RisingWave port; env RW_PORT"
    ),
    rw_user: str = typer.Option(
        env_default("USER", "root"), help="RisingWave user; env RW_USER"
    ),
    rw_password: Optional[str] = typer.Option(
        env_default("PASSWORD"), help="RisingWave password; env RW_PASSWORD"
    ),
    rw_db: str = typer.Option(
        env_default("DATABASE", "dev"), help="RisingWave database; env RW_DATABASE"
    ),
    # PostgreSQL connection
    pg_host: str = typer.Option(
        pg_env_default("HOST", "localhost"), help="PostgreSQL host; env PG_HOST"
    ),
    pg_port: int = typer.Option(
        int(pg_env_default("PORT", "5432")), help="PostgreSQL port; env PG_PORT"
    ),
    pg_user: str = typer.Option(
        pg_env_default("USER", "postgres"), help="PostgreSQL user; env PG_USER"
    ),
    pg_password: str = typer.Option(
        pg_env_default("PASSWORD", ""), help="PostgreSQL password; env PG_PASSWORD"
    ),
    pg_db: str = typer.Option(
        pg_env_default("DATABASE", "postgres"), help="PostgreSQL database; env PG_DATABASE"
    ),
    pg_schema: str = typer.Option(
        pg_env_default("SCHEMA", "public"), help="PostgreSQL schema; env PG_SCHEMA"
    ),
    # Source configuration
    source_name: str = typer.Option(
        env_default("SOURCE_NAME", "postgres_cdc_source"), help="CDC source name to create"
    ),
    slot_name: Optional[str] = typer.Option(
        None, help="PostgreSQL replication slot name"),
    publication_name: str = typer.Option(
        "rw_publication", help="PostgreSQL publication name"),
    auto_schema_change: bool = typer.Option(
        False, help="Enable automatic schema changes"),
    ssl_mode: Optional[str] = typer.Option(
        None, help="SSL mode (disable, require, etc.)"),
    # Backfill configuration
    backfill_rows_per_split: Optional[str] = typer.Option(
        "100000", help="Number of rows per backfill split"
    ),
    backfill_parallelism: Optional[str] = typer.Option(
        "8", help="Backfill parallelism level"
    ),
    # Table selection
    include_all: bool = typer.Option(
        False, help="Include all discovered tables"),
    include_tables: List[str] = typer.Option(
        [], help="Specific tables to include (repeatable)"),
    exclude_tables: List[str] = typer.Option(
        [], help="Tables to exclude (repeatable)"),
    interactive: bool = typer.Option(
        False, help="Interactive table selection"),
    # Control options
    dry_run: bool = typer.Option(False, help="Show SQL without executing"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
):
    """Create a PostgreSQL CDC pipeline with table discovery and selection."""

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Create clients
        rw_client = RisingWaveClient(
            host=rw_host,
            port=rw_port,
            user=rw_user,
            password=rw_password,
            database=rw_db,
        )

        # Create PostgreSQL config
        pg_config = PostgreSQLConfig(
            source_name=source_name,
            hostname=pg_host,
            port=pg_port,
            username=pg_user,
            password=pg_password,
            database=pg_db,
            schema_name=pg_schema,
            slot_name=slot_name,
            publication_name=publication_name,
            auto_schema_change=auto_schema_change,
            ssl_mode=ssl_mode,
            backfill_num_rows_per_split=backfill_rows_per_split,
            backfill_parallelism=backfill_parallelism,
        )

        # Create pipeline builder
        builder = PipelineBuilder(rw_client)

        # Discover tables first
        console.print(
            f"🔍 Discovering tables in PostgreSQL schema '{pg_schema}'...")
        available_tables = builder.discover_postgresql_tables(pg_config)

        if not available_tables:
            console.print(f"❌ No tables found in schema '{pg_schema}'")
            raise typer.Exit(1)

        console.print(f"✓ Found {len(available_tables)} tables")

        # Determine table selection
        selected_tables = []

        if interactive:
            # Interactive selection
            console.print("\n📋 Available tables:")
            for i, table in enumerate(available_tables, 1):
                console.print(
                    f"  {i:2d}. {table.qualified_name} ({table.table_type})")

            if Confirm.ask("\nSelect specific tables?", default=False):
                selected_indices = Prompt.ask(
                    "Enter table numbers (comma-separated, e.g., 1,3,5)"
                ).split(",")

                for idx_str in selected_indices:
                    try:
                        idx = int(idx_str.strip()) - 1
                        if 0 <= idx < len(available_tables):
                            selected_tables.append(available_tables[idx])
                    except ValueError:
                        console.print(f"❌ Invalid table number: {idx_str}")
            else:
                selected_tables = available_tables

        elif include_tables:
            # Use specific table list
            selector = TableSelector(specific_tables=include_tables)
            selected_tables = selector.select_tables(available_tables)

        elif include_all:
            # Include all tables, possibly excluding some
            selector = TableSelector(
                include_all=True, exclude_patterns=exclude_tables)
            selected_tables = selector.select_tables(available_tables)

        else:
            # Default: ask user
            if not dry_run:
                rprint(
                    "\\n[yellow]No table selection specified. Use --include-all, --include-tables, or --interactive[/yellow]")
                if Confirm.ask("Include all tables?", default=True):
                    selected_tables = available_tables
                else:
                    console.print("❌ No tables selected")
                    raise typer.Exit(1)
            else:
                selected_tables = available_tables  # For dry run, show all

        if not selected_tables:
            console.print("❌ No tables selected for CDC")
            raise typer.Exit(1)

        console.print(f"\\n✅ Selected {len(selected_tables)} tables for CDC:")
        for table in selected_tables:
            console.print(f"  - {table.qualified_name}")

        # Create table selector object
        table_selector = TableSelector(
            specific_tables=[t.qualified_name for t in selected_tables])

        # Create pipeline
        console.print(f"\\n🚀 Creating PostgreSQL CDC pipeline...")
        result = builder.create_postgresql_pipeline(
            pg_config,
            table_selector,
            dry_run=dry_run
        )

        if dry_run:
            console.print("\\n📝 Generated SQL:")
            console.print("=" * 60)
            for i, sql in enumerate(result["sql_statements"], 1):
                console.print(f"\\n-- Statement {i}")
                console.print(sql)
            console.print("=" * 60)
        else:
            console.print(f"\\n✅ Pipeline created successfully!")
            console.print(f"  - Source: {source_name}")
            console.print(f"  - Tables: {len(selected_tables)}")

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1)


@app.command()
def version():
    """Show version information."""
    from . import __version__
    console.print(f"RisingWave Pipeline SDK v{__version__}")


if __name__ == "__main__":
    app()
