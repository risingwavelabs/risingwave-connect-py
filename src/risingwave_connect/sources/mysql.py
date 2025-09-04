"""MySQL CDC source configuration and utilities."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MySQLConfig:
    """Configuration for MySQL CDC source connection."""

    # Required connection parameters
    hostname: str
    username: str
    password: str
    database: str
    port: int = 3306

    # Optional CDC parameters
    # Specific table for CDC (if not using discovery)
    table_name: Optional[str] = None
    schema_names: Optional[List[str]] = None  # Schemas to include in CDC
    server_id: Optional[int] = None  # MySQL server ID for replication

    # Schema change and transaction support
    auto_schema_change: bool = False  # Enable schema change replication
    transactional: Optional[bool] = None  # Enable transactions for CDC table

    # SSL configuration
    ssl_mode: str = "disabled"  # disabled, preferred, required, verify_ca, verify_identity
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None

    # Connection options
    connection_timeout: int = 30
    heartbeat_interval: int = 10000  # milliseconds

    # Additional MySQL-specific options
    charset: str = "utf8"
    timezone: str = "+00:00"

    # Debezium configuration parameters (with debezium. prefix)
    debezium_params: Optional[Dict[str, str]] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.hostname:
            raise ValueError("hostname is required")
        if not self.username:
            raise ValueError("username is required")
        if not self.password:
            raise ValueError("password is required")
        if not self.database:
            raise ValueError("database is required")

        if self.ssl_mode in ["verify_ca", "verify_identity"] and not self.ssl_ca:
            raise ValueError(
                f"ssl_ca is required for ssl_mode='{self.ssl_mode}'")

    def to_source_properties(self) -> Dict[str, Any]:
        """Convert to RisingWave source properties."""
        properties = {
            "connector": "mysql-cdc",
            "hostname": self.hostname,
            "port": str(self.port),
            "username": self.username,
            "password": self.password,
            "database.name": self.database,
        }

        # Add optional parameters
        if self.server_id:
            properties["server.id"] = str(self.server_id)

        if self.auto_schema_change:
            properties["auto.schema.change"] = "true"

        if self.transactional is not None:
            properties["transactional"] = "true" if self.transactional else "false"

        if self.ssl_mode != "disabled":
            properties["ssl.mode"] = self.ssl_mode

        if self.ssl_ca:
            properties["ssl.ca"] = self.ssl_ca

        if self.ssl_cert:
            properties["ssl.cert"] = self.ssl_cert

        if self.ssl_key:
            properties["ssl.key"] = self.ssl_key

        if self.connection_timeout != 30:
            properties["connect.timeout"] = str(self.connection_timeout)

        if self.heartbeat_interval != 10000:
            properties["heartbeat.interval"] = str(self.heartbeat_interval)

        if self.charset != "utf8":
            properties["charset"] = self.charset

        if self.timezone != "+00:00":
            properties["server.time.zone"] = self.timezone

        # Add Debezium parameters
        if self.debezium_params:
            for key, value in self.debezium_params.items():
                # Add debezium. prefix if not already present
                param_key = key if key.startswith(
                    "debezium.") else f"debezium.{key}"
                properties[param_key] = str(value)

        return properties

    def get_source_name(self) -> str:
        """Generate a source name based on configuration."""
        safe_hostname = self.hostname.replace(".", "_").replace("-", "_")
        safe_database = self.database.replace(".", "_").replace("-", "_")
        return f"mysql_cdc_{safe_hostname}_{safe_database}_source"

    def get_connection_url(self) -> str:
        """Get MySQL connection URL for testing."""
        return f"mysql://{self.username}:***@{self.hostname}:{self.port}/{self.database}"
