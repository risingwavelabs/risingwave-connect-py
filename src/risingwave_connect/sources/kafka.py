"""Kafka source configuration and utilities."""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    """Configuration for Kafka source connection."""

    # Required connection parameters
    bootstrap_servers: str  # Comma-separated list of Kafka brokers
    topic: str  # Kafka topic to consume from

    # Optional connection parameters
    consumer_group: Optional[str] = None  # Consumer group ID
    scan_startup_mode: str = "earliest"  # earliest, latest, timestamp
    scan_startup_timestamp_millis: Optional[int] = None  # For timestamp mode

    # Security parameters
    security_protocol: str = "PLAINTEXT"  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    sasl_mechanism: Optional[str] = None  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None

    # SSL parameters
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[str] = None

    # Message format parameters
    format_type: str = "PLAIN"  # PLAIN, UPSERT
    encode_type: str = "JSON"  # JSON, AVRO, PROTOBUF, BYTES, CSV
    # JSON, AVRO, PROTOBUF, BYTES for key
    key_encode_type: Optional[str] = None

    # Schema registry parameters (for AVRO/PROTOBUF)
    schema_registry_url: Optional[str] = None
    schema_registry_username: Optional[str] = None
    schema_registry_password: Optional[str] = None

    # AVRO specific parameters
    avro_message: Optional[str] = None  # message name for AVRO

    # Protobuf specific parameters
    protobuf_message: Optional[str] = None  # package.message_name for Protobuf
    # S3 location for schema descriptor
    protobuf_location: Optional[str] = None
    protobuf_access_key: Optional[str] = None  # S3 access key
    protobuf_secret_key: Optional[str] = None  # S3 secret key

    # CSV specific parameters
    csv_without_header: bool = True  # CSV without header
    csv_delimiter: str = ","  # CSV delimiter

    # Advanced parameters
    # Additional Kafka consumer properties
    properties: Optional[Dict[str, str]] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.scan_startup_mode == "timestamp" and self.scan_startup_timestamp_millis is None:
            raise ValueError(
                "scan_startup_timestamp_millis is required when scan_startup_mode is 'timestamp'")

        if self.encode_type == "AVRO" and not self.schema_registry_url:
            raise ValueError(
                f"schema_registry_url is required for {self.encode_type} format")
                
        if self.encode_type == "PROTOBUF" and not (self.schema_registry_url or self.protobuf_location):
            raise ValueError(
                "Either schema_registry_url or protobuf_location is required for PROTOBUF format")

        if self.security_protocol.startswith("SASL") and not self.sasl_mechanism:
            raise ValueError(
                "sasl_mechanism is required for SASL security protocols")

    def to_source_properties(self) -> Dict[str, Any]:
        """Convert to RisingWave source properties."""
        properties = {
            "connector": "kafka",
            "topic": self.topic,
            "properties.bootstrap.server": self.bootstrap_servers,
            "scan.startup.mode": self.scan_startup_mode,
        }

        # Add consumer group if specified
        if self.consumer_group:
            properties["consumer.group.id"] = self.consumer_group

        # Add timestamp for timestamp mode
        if self.scan_startup_mode == "timestamp" and self.scan_startup_timestamp_millis:
            properties["scan.startup.timestamp.millis"] = str(
                self.scan_startup_timestamp_millis)

        # Add security configuration
        if self.security_protocol != "PLAINTEXT":
            properties["properties.security.protocol"] = self.security_protocol

        if self.sasl_mechanism:
            properties["properties.sasl.mechanism"] = self.sasl_mechanism

        if self.sasl_username:
            properties["properties.sasl.username"] = self.sasl_username

        if self.sasl_password:
            properties["properties.sasl.password"] = self.sasl_password

        # Add SSL configuration
        if self.ssl_ca_location:
            properties["properties.ssl.ca.location"] = self.ssl_ca_location

        if self.ssl_certificate_location:
            properties["properties.ssl.certificate.location"] = self.ssl_certificate_location

        if self.ssl_key_location:
            properties["properties.ssl.key.location"] = self.ssl_key_location

        if self.ssl_key_password:
            properties["properties.ssl.key.password"] = self.ssl_key_password

        # Add schema registry configuration
        if self.schema_registry_url:
            properties["schema.registry"] = self.schema_registry_url

        if self.schema_registry_username:
            properties["schema.registry.username"] = self.schema_registry_username

        if self.schema_registry_password:
            properties["schema.registry.password"] = self.schema_registry_password

        # Add custom properties
        if self.properties:
            for key, value in self.properties.items():
                properties[f"properties.{key}"] = value

        return properties

    def get_source_name(self) -> str:
        """Generate a source name based on configuration."""
        # Extract topic name for source naming
        topic_name = self.topic.replace(".", "_").replace("-", "_")
        return f"kafka_{topic_name}_source"
