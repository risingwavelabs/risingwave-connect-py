from __future__ import annotations
from typing import Dict, Iterable, Optional


def _quote(val: str) -> str:
    return val.replace("'", "''")


def build_create_source_postgres_cdc(
    source_name: str,
    *,
    hostname: str,
    port: int = 5432,
    username: str,
    password: str,
    database: str,
    schema: Optional[str] = None,
    slot_name: Optional[str] = None,
    publication_name: Optional[str] = None,
    publication_create_enable: Optional[bool] = None,
    auto_schema_change: Optional[bool] = None,
    ssl_mode: Optional[str] = None,
    ssl_root_cert: Optional[str] = None,
    extra_with: Optional[Dict[str, str]] = None,
    debezium_overrides: Optional[Dict[str, str]] = None,
) -> str:
    """Builds a CREATE SOURCE statement for postgres-cdc in RisingWave.

    See docs: https://docs.risingwave.com/ingestion/sources/postgresql/pg-cdc
    """
    with_items: list[str] = [
        "connector='postgres-cdc'",
        f"hostname='{_quote(hostname)}'",
        f"port='{int(port)}'",
        f"username='{_quote(username)}'",
        f"password='{_quote(password)}'",
        f"database.name='{_quote(database)}'",
    ]
    if schema:
        with_items.append(f"schema.name='{_quote(schema)}'")
    if slot_name:
        with_items.append(f"slot.name='{_quote(slot_name)}'")
    if publication_name:
        with_items.append(f"publication.name='{_quote(publication_name)}'")
    if publication_create_enable is not None:
        with_items.append(
            f"publication.create.enable='{'true' if publication_create_enable else 'false'}'"
        )
    if auto_schema_change is not None:
        with_items.append(f"auto.schema.change='{'true' if auto_schema_change else 'false'}'")
    if ssl_mode:
        with_items.append(f"ssl.mode='{_quote(ssl_mode)}'")
    if ssl_root_cert:
        with_items.append(f"ssl.root.cert='{_quote(ssl_root_cert)}'")

    if extra_with:
        for k, v in extra_with.items():
            with_items.append(f"{k}='{_quote(str(v))}'")

    if debezium_overrides:
        for k, v in debezium_overrides.items():
            with_items.append(f"debezium.{k}='{_quote(str(v))}'")

    with_clause = ",\n    ".join(with_items)
    return f"""
CREATE SOURCE {source_name} WITH (
    {with_clause}
);
""".strip()


def build_create_table_from_source(
    table_name: str,
    source_name: str,
    *,
    pg_table: str,
    schema: Optional[str] = None,
    columns_ddl: Optional[Iterable[str]] = None,
    snapshot: Optional[bool] = None,
    include_timestamp_as: Optional[str] = None,
) -> str:
    """Builds a CREATE TABLE ... FROM source ... statement.

    - If columns_ddl is None, we use wildcard mapping with (*) as per docs.
    - pg_table should be '<schema>.<table>' and will be quoted as a string literal.
    """
    include_clause = ""
    if include_timestamp_as:
        include_clause = f"\nINCLUDE timestamp AS {include_timestamp_as}"

    with_items: list[str] = []
    if snapshot is not None:
        with_items.append(f"snapshot='{'true' if snapshot else 'false'}'")
    with_clause = f"\nWITH ( {', '.join(with_items)} )" if with_items else ""

    cols = "*" if columns_ddl is None else ",\n    ".join(columns_ddl)

    schema_prefix = f"{schema}." if schema else ""

    return f"""
CREATE TABLE {schema_prefix}{table_name} (
    {cols}
){include_clause}{with_clause}
FROM {source_name} TABLE '{_quote(pg_table)}';
""".strip()
