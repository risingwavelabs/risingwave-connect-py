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
    group_id_prefix: str = "rw-consumer"  # Custom prefix for consumer group ID
    scan_startup_mode: str = "earliest"  # earliest, latest, timestamp
    scan_startup_timestamp_millis: Optional[int] = None  # For timestamp mode

    # General Kafka properties
    client_id: Optional[str] = None  # Client ID for tracing and monitoring
    sync_call_timeout: str = "5s"  # Timeout for synchronous calls
    enable_auto_commit: bool = True  # Enable automatic offset commits
    enable_ssl_certificate_verification: bool = True  # SSL certificate verification
    fetch_max_bytes: Optional[int] = None  # Max data per fetch request
    fetch_queue_backoff_ms: Optional[int] = None  # Backoff time between fetch requests
    fetch_wait_max_ms: Optional[int] = None  # Max wait time for fetch requests
    message_max_bytes: Optional[int] = None  # Max message size
    queued_max_messages_kbytes: Optional[int] = None  # Max buffered messages size
    queued_min_messages: Optional[int] = None  # Min messages in local queue
    receive_message_max_bytes: Optional[int] = None  # Max receivable message size
    statistics_interval_ms: Optional[int] = None  # Statistics emission interval
    ssl_endpoint_identification_algorithm: Optional[str] = None  # SSL endpoint verification

    # Security parameters
    security_protocol: str = "PLAINTEXT"  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    sasl_mechanism: Optional[str] = None  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None

    # Kerberos parameters (for SASL/GSSAPI)
    sasl_kerberos_service_name: Optional[str] = None
    sasl_kerberos_keytab: Optional[str] = None
    sasl_kerberos_principal: Optional[str] = None
    sasl_kerberos_kinit_cmd: Optional[str] = None
    sasl_kerberos_min_time_before_relogin: Optional[int] = None

    # OAuth parameters (for SASL/OAUTHBEARER)
    sasl_oauthbearer_config: Optional[str] = None

    # SSL parameters
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[str] = None

    # AWS PrivateLink parameters
    privatelink_endpoint: Optional[str] = None  # VPC endpoint DNS name
    privatelink_targets: Optional[str] = None  # JSON array of port mappings

    # Message format parameters
    data_format: str = "PLAIN"  # PLAIN, UPSERT, DEBEZIUM, MAXWELL, CANAL
    data_encode: str = "JSON"  # JSON, AVRO, PROTOBUF, BYTES, CSV, PARQUET
    key_encode_type: Optional[str] = None  # JSON, AVRO, PROTOBUF, BYTES for key

    # Schema registry parameters (for AVRO/PROTOBUF)
    schema_registry_url: Optional[str] = None
    schema_registry_username: Optional[str] = None
    schema_registry_password: Optional[str] = None

    # Schema location parameters
    message: Optional[str] = None  # Message name for Protobuf
    location: Optional[str] = None  # Schema file location (http/https/S3)
    access_key: Optional[str] = None  # AWS access key for S3
    secret_key: Optional[str] = None  # AWS secret key for S3
    region: Optional[str] = None  # AWS region for S3
    arn: Optional[str] = None  # AWS role ARN
    external_id: Optional[str] = None  # AWS external ID

    # AWS Glue Schema Registry parameters
    aws_region: Optional[str] = None
    aws_credentials_access_key_id: Optional[str] = None
    aws_credentials_secret_access_key: Optional[str] = None
    aws_credentials_role_arn: Optional[str] = None
    aws_glue_schema_arn: Optional[str] = None

    # AVRO specific parameters
    map_handling_mode: str = "map"  # map or jsonb for Avro map types

    # CSV specific parameters
    csv_without_header: bool = True  # CSV without header
    csv_delimiter: str = ","  # CSV delimiter

    # Advanced parameters
    properties: Optional[Dict[str, str]] = None  # Additional Kafka consumer properties

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Validate data format options
        valid_formats = ["PLAIN", "UPSERT", "DEBEZIUM", "MAXWELL", "CANAL"]
        if self.data_format.upper() not in valid_formats:
            raise ValueError(f"data_format must be one of {valid_formats}, got '{self.data_format}'")

        # Validate data encode options
        valid_encodings = ["JSON", "AVRO", "PROTOBUF", "BYTES", "CSV", "PARQUET"]
        if self.data_encode.upper() not in valid_encodings:
            raise ValueError(f"data_encode must be one of {valid_encodings}, got '{self.data_encode}'")

        # Validate startup mode
        valid_startup_modes = ["earliest", "latest", "timestamp"]
        if self.scan_startup_mode not in valid_startup_modes:
            raise ValueError(f"scan_startup_mode must be one of {valid_startup_modes}, got '{self.scan_startup_mode}'")
            
        if self.scan_startup_mode == "timestamp" and self.scan_startup_timestamp_millis is None:
            raise ValueError(
                "scan_startup_timestamp_millis is required when scan_startup_mode is 'timestamp'")

        # Validate schema registry requirements for AVRO
        if self.data_encode == "AVRO" and not self.schema_registry_url and not self.aws_glue_schema_arn:
            raise ValueError(
                "schema_registry_url or AWS Glue Schema Registry is required for AVRO format")
                
        # Validate schema requirements for PROTOBUF
        if self.data_encode == "PROTOBUF" and not (self.schema_registry_url or self.location):
            raise ValueError(
                "Either schema_registry_url or location is required for PROTOBUF format")

        # Validate security protocol
        valid_security_protocols = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
        if self.security_protocol.upper() not in valid_security_protocols:
            raise ValueError(f"security_protocol must be one of {valid_security_protocols}, got '{self.security_protocol}'")

        # Validate SASL requirements
        if self.security_protocol.startswith("SASL") and not self.sasl_mechanism:
            raise ValueError(
                "sasl_mechanism is required for SASL security protocols")

        # Validate SASL mechanism
        if self.sasl_mechanism:
            valid_sasl_mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER"]
            if self.sasl_mechanism.upper() not in valid_sasl_mechanisms:
                raise ValueError(f"sasl_mechanism must be one of {valid_sasl_mechanisms}, got '{self.sasl_mechanism}'")

        # Validate SASL/GSSAPI (Kerberos) requirements
        if self.sasl_mechanism == "GSSAPI":
            required_kerberos_fields = [
                self.sasl_kerberos_service_name,
                self.sasl_kerberos_keytab,
                self.sasl_kerberos_principal,
                self.sasl_kerberos_kinit_cmd,
                self.sasl_kerberos_min_time_before_relogin
            ]
            if not all(required_kerberos_fields):
                raise ValueError(
                    "All Kerberos parameters are required for SASL/GSSAPI mechanism")

        # Validate OAuth requirements
        if self.sasl_mechanism == "OAUTHBEARER" and not self.sasl_oauthbearer_config:
            raise ValueError(
                "sasl_oauthbearer_config is required for OAUTHBEARER mechanism")

        # Validate AWS Glue Schema Registry requirements
        if self.aws_glue_schema_arn:
            if not self.aws_region:
                raise ValueError("aws_region is required when using AWS Glue Schema Registry")
            if not (self.aws_credentials_access_key_id and self.aws_credentials_secret_access_key) and not self.aws_credentials_role_arn:
                raise ValueError(
                    "Either AWS credentials (access_key_id and secret_access_key) or role ARN is required for AWS Glue Schema Registry")

        # Validate PrivateLink configuration
        if self.privatelink_endpoint and not self.privatelink_targets:
            raise ValueError(
                "privatelink_targets is required when privatelink_endpoint is specified")

        # Validate S3 location requirements
        if self.location and self.location.startswith("s3://"):
            if not (self.access_key and self.secret_key and self.region):
                raise ValueError(
                    "access_key, secret_key, and region are required for S3 schema locations")

        # Validate format-specific requirements
        if self.data_format.upper() == "UPSERT" and self.data_encode.upper() not in ["JSON", "AVRO", "PROTOBUF"]:
            raise ValueError("UPSERT format only supports JSON, AVRO, or PROTOBUF encoding")

        # Validate CDC format requirements
        cdc_formats = ["DEBEZIUM", "MAXWELL", "CANAL"]
        if self.data_format.upper() in cdc_formats and self.data_encode.upper() != "JSON":
            raise ValueError(f"{self.data_format} format only supports JSON encoding")

        # Validate map handling mode for AVRO
        if self.data_encode.upper() == "AVRO":
            valid_map_modes = ["map", "jsonb"]
            if self.map_handling_mode not in valid_map_modes:
                raise ValueError(f"map_handling_mode must be one of {valid_map_modes}, got '{self.map_handling_mode}'")

    def to_source_properties(self) -> Dict[str, Any]:
        """Convert to RisingWave source properties."""
        properties = {
            "connector": "kafka",
            "topic": self.topic,
            "properties.bootstrap.server": self.bootstrap_servers,  # Note: singular 'server' not 'servers'
            "scan.startup.mode": self.scan_startup_mode,
        }

        # Add group ID prefix
        if self.group_id_prefix != "rw-consumer":
            properties["group.id.prefix"] = self.group_id_prefix

        # Add timestamp for timestamp mode
        if self.scan_startup_mode == "timestamp" and self.scan_startup_timestamp_millis:
            properties["scan.startup.timestamp.millis"] = str(self.scan_startup_timestamp_millis)

        # Add general Kafka properties
        if self.client_id:
            properties["properties.client.id"] = self.client_id
        
        if self.sync_call_timeout != "5s":
            properties["properties.sync.call.timeout"] = self.sync_call_timeout
        
        if not self.enable_auto_commit:
            properties["properties.enable.auto.commit"] = "false"
        
        if not self.enable_ssl_certificate_verification:
            properties["properties.enable.ssl.certificate.verification"] = "false"
        
        if self.fetch_max_bytes:
            properties["properties.fetch.max.bytes"] = str(self.fetch_max_bytes)
        
        if self.fetch_queue_backoff_ms:
            properties["properties.fetch.queue.backoff.ms"] = str(self.fetch_queue_backoff_ms)
        
        if self.fetch_wait_max_ms:
            properties["properties.fetch.wait.max.ms"] = str(self.fetch_wait_max_ms)
        
        if self.message_max_bytes:
            properties["properties.message.max.bytes"] = str(self.message_max_bytes)
        
        if self.queued_max_messages_kbytes:
            properties["properties.queued.max.messages.kbytes"] = str(self.queued_max_messages_kbytes)
        
        if self.queued_min_messages:
            properties["properties.queued.min.messages"] = str(self.queued_min_messages)
        
        if self.receive_message_max_bytes:
            properties["properties.receive.message.max.bytes"] = str(self.receive_message_max_bytes)
        
        if self.statistics_interval_ms:
            properties["properties.statistics.interval.ms"] = str(self.statistics_interval_ms)
        
        if self.ssl_endpoint_identification_algorithm:
            properties["properties.ssl.endpoint.identification.algorithm"] = self.ssl_endpoint_identification_algorithm

        # Add security configuration
        if self.security_protocol != "PLAINTEXT":
            properties["properties.security.protocol"] = self.security_protocol

        if self.sasl_mechanism:
            properties["properties.sasl.mechanism"] = self.sasl_mechanism

        if self.sasl_username:
            properties["properties.sasl.username"] = self.sasl_username

        if self.sasl_password:
            properties["properties.sasl.password"] = self.sasl_password

        # Add Kerberos configuration
        if self.sasl_kerberos_service_name:
            properties["properties.sasl.kerberos.service.name"] = self.sasl_kerberos_service_name
        
        if self.sasl_kerberos_keytab:
            properties["properties.sasl.kerberos.keytab"] = self.sasl_kerberos_keytab
        
        if self.sasl_kerberos_principal:
            properties["properties.sasl.kerberos.principal"] = self.sasl_kerberos_principal
        
        if self.sasl_kerberos_kinit_cmd:
            properties["properties.sasl.kerberos.kinit.cmd"] = self.sasl_kerberos_kinit_cmd
        
        if self.sasl_kerberos_min_time_before_relogin:
            properties["properties.sasl.kerberos.min.time.before.relogin"] = str(self.sasl_kerberos_min_time_before_relogin)

        # Add OAuth configuration
        if self.sasl_oauthbearer_config:
            properties["properties.sasl.oauthbearer.config"] = self.sasl_oauthbearer_config

        # Add SSL configuration
        if self.ssl_ca_location:
            properties["properties.ssl.ca.location"] = self.ssl_ca_location

        if self.ssl_certificate_location:
            properties["properties.ssl.certificate.location"] = self.ssl_certificate_location

        if self.ssl_key_location:
            properties["properties.ssl.key.location"] = self.ssl_key_location

        if self.ssl_key_password:
            properties["properties.ssl.key.password"] = self.ssl_key_password

        # Add PrivateLink configuration
        if self.privatelink_endpoint:
            properties["privatelink.endpoint"] = self.privatelink_endpoint
        
        if self.privatelink_targets:
            properties["privatelink.targets"] = self.privatelink_targets

        # Add custom properties
        if self.properties:
            for key, value in self.properties.items():
                # Ensure proper prefixing for Kafka properties
                if not key.startswith("properties.") and key not in ["connector", "topic", "scan.startup.mode", "scan.startup.timestamp.millis", "group.id.prefix"]:
                    properties[f"properties.{key}"] = value
                else:
                    properties[key] = value

        return properties

        return properties

    def get_format_encode_properties(self) -> Dict[str, Any]:
        """Get format and encoding specific properties for the FORMAT/ENCODE clause."""
        format_properties = {}
        
        # AVRO specific parameters
        if self.data_encode.upper() == "AVRO":
            if self.message:
                format_properties["message"] = self.message
            
            # Schema registry configuration for AVRO
            if self.schema_registry_url:
                format_properties["schema.registry"] = self.schema_registry_url
            
            if self.schema_registry_username:
                format_properties["schema.registry.username"] = self.schema_registry_username
            
            if self.schema_registry_password:
                format_properties["schema.registry.password"] = self.schema_registry_password

            # AWS Glue Schema Registry configuration for AVRO
            if self.aws_region:
                format_properties["aws.region"] = self.aws_region
            
            if self.aws_credentials_access_key_id:
                format_properties["aws.credentials.access_key_id"] = self.aws_credentials_access_key_id
            
            if self.aws_credentials_secret_access_key:
                format_properties["aws.credentials.secret_access_key"] = self.aws_credentials_secret_access_key
            
            if self.aws_credentials_role_arn:
                format_properties["aws.credentials.role.arn"] = self.aws_credentials_role_arn
            
            if self.aws_glue_schema_arn:
                format_properties["aws.glue.schema_arn"] = self.aws_glue_schema_arn

            # AVRO map handling
            if self.map_handling_mode != "map":
                format_properties["map.handling.mode"] = self.map_handling_mode

        # PROTOBUF specific parameters
        elif self.data_encode.upper() == "PROTOBUF":
            if self.message:
                format_properties["message"] = self.message
            
            # Schema registry for PROTOBUF
            if self.schema_registry_url:
                format_properties["schema.registry"] = self.schema_registry_url
            
            if self.schema_registry_username:
                format_properties["schema.registry.username"] = self.schema_registry_username
            
            if self.schema_registry_password:
                format_properties["schema.registry.password"] = self.schema_registry_password
            
            # S3 location for PROTOBUF schema
            if self.location:
                format_properties["location"] = self.location
            
            if self.access_key:
                format_properties["access_key"] = self.access_key
            
            if self.secret_key:
                format_properties["secret_key"] = self.secret_key
            
            if self.region:
                format_properties["region"] = self.region
            
            if self.arn:
                format_properties["arn"] = self.arn
            
            if self.external_id:
                format_properties["external_id"] = self.external_id

        # CSV specific parameters
        elif self.data_encode.upper() == "CSV":
            format_properties["without_header"] = str(self.csv_without_header).lower()
            format_properties["delimiter"] = self.csv_delimiter

        # BYTES format - no additional parameters needed
        # JSON format - no additional parameters needed
        # PARQUET format - no additional parameters needed
        
        return format_properties

    def get_source_name(self) -> str:
        """Generate a source name based on configuration."""
        # Extract topic name for source naming
        topic_name = self.topic.replace(".", "_").replace("-", "_")
        return f"kafka_{topic_name}_source"

    @classmethod
    def create_basic_json_config(
        cls,
        bootstrap_servers: str,
        topic: str,
        **kwargs
    ) -> "KafkaConfig":
        """
        Create a basic Kafka configuration for JSON data.
        
        Example:
            config = KafkaConfig.create_basic_json_config(
                bootstrap_servers="localhost:9092",
                topic="user_activity"
            )
        """
        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            data_format="PLAIN",
            data_encode="JSON",
            **kwargs
        )

    @classmethod
    def create_secure_config(
        cls,
        bootstrap_servers: str,
        topic: str,
        security_protocol: str = "SASL_SSL",
        sasl_mechanism: str = "PLAIN",
        sasl_username: str = None,
        sasl_password: str = None,
        **kwargs
    ) -> "KafkaConfig":
        """
        Create a secure Kafka configuration with SASL authentication.
        
        Example:
            config = KafkaConfig.create_secure_config(
                bootstrap_servers="broker1:9092,broker2:9092",
                topic="user_activity",
                sasl_username="your-username",
                sasl_password="your-password"
            )
        """
        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            **kwargs
        )

    @classmethod
    def create_avro_config(
        cls,
        bootstrap_servers: str,
        topic: str,
        schema_registry_url: str,
        schema_registry_username: str = None,
        schema_registry_password: str = None,
        **kwargs
    ) -> "KafkaConfig":
        """
        Create a Kafka configuration for AVRO data with Schema Registry.
        
        Example:
            config = KafkaConfig.create_avro_config(
                bootstrap_servers="localhost:9092",
                topic="user_events",
                schema_registry_url="http://localhost:8081"
            )
        """
        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            data_format="PLAIN",
            data_encode="AVRO",
            schema_registry_url=schema_registry_url,
            schema_registry_username=schema_registry_username,
            schema_registry_password=schema_registry_password,
            **kwargs
        )
