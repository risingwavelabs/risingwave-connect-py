"""Base builder classes for sources and sinks."""

from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union

from ..client import RisingWaveClient
from ..discovery.base import TableSelector, TableInfo, TableColumnConfig

logger = logging.getLogger(__name__)


class BaseSourceBuilder(ABC):
    """Base class for source builders."""

    def __init__(self, rw_client: RisingWaveClient):
        self.rw_client = rw_client

    @abstractmethod
    def create_connection(self, config: Any, **kwargs) -> Dict[str, Any]:
        """Create a source connection."""
        pass

    @abstractmethod
    def discover_tables(self, config: Any, **kwargs) -> List[TableInfo]:
        """Discover available tables/collections."""
        pass

    @abstractmethod
    def get_schemas(self, config: Any) -> List[str]:
        """Get available schemas/databases."""
        pass

    def _validate_table_selector(
        self,
        table_selector: Optional[Union[TableSelector, List[str]]],
        dry_run: bool = False
    ) -> Optional[TableSelector]:
        """Convert and validate table selector."""
        if isinstance(table_selector, list):
            return TableSelector(specific_tables=table_selector)
        return table_selector

    def _create_placeholder_tables(
        self,
        table_names: List[str],
        default_schema: str = "public"
    ) -> List[TableInfo]:
        """Create placeholder tables for dry run mode."""
        placeholder_tables = []
        for table_name in table_names:
            if '.' in table_name:
                schema_name, table_name_only = table_name.split('.', 1)
            else:
                schema_name = default_schema
                table_name_only = table_name

            placeholder_table = TableInfo(
                schema_name=schema_name,
                table_name=table_name_only,
                table_type='BASE TABLE',
                row_count=None,
                size_bytes=None,
                comment="Dry run - table not validated"
            )
            placeholder_tables.append(placeholder_table)

        return placeholder_tables


class BaseSinkBuilder(ABC):
    """Base class for sink builders."""

    def __init__(self, rw_client: RisingWaveClient):
        self.rw_client = rw_client

    @abstractmethod
    def create_sink(self, config: Any, source_tables: List[str], **kwargs) -> Dict[str, Any]:
        """Create a sink."""
        pass
