#!/usr/bin/env python3
"""
Kafka Source Examples for RisingWave Connect
===========================================

This example demonstrates how to create Kafka source connections
with different configurations and formats using the improved API.
"""

import logging
from risingwave_connect.client import RisingWaveClient
from risingwave_connect.sources.kafka import KafkaConfig
from risingwave_connect.builders.kafka import KafkaBuilder

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    print("🔗 Kafka Source Examples for RisingWave")
    print("=" * 60)

    # Initialize RisingWave client (dry run mode for examples)
    client = RisingWaveClient(
        host="localhost", port=4566, user="root", password="")
    kafka_builder = KafkaBuilder(client)

    # Example 1: Basic JSON Kafka Source
    print("\n💼 Basic JSON Kafka Source Example")
    print("=" * 50)

    kafka_config = KafkaConfig.create_basic_json_config(
        bootstrap_servers="localhost:9092",
        topic="user_events",
        group_id_prefix="risingwave_consumers"
    )

    result = kafka_builder.create_connection(
        config=kafka_config,
        table_name="user_events",
        connection_type="table",  # Store data in RisingWave
        include_offset=True,
        include_partition=True,
        include_timestamp=True,
        dry_run=True
    )

    if result.get("success_summary", {}).get("overall_success"):
        print(
            f"✅ Created {result['success_summary']['total_tables']} Kafka table!")
        print(
            f"  📊 {result['table_info']['table_name']}: {result['table_info']['format']} format")

        print("\n🔍 Example queries:")
        print("  -- Check record counts")
        print("  SELECT count(*) FROM user_events;")
        print("\n  -- Query with Kafka metadata")
        print("  SELECT kafka_partition, kafka_offset, kafka_timestamp, data")
        print("  FROM user_events ORDER BY kafka_timestamp DESC LIMIT 100;")

    else:
        print(
            f"❌ Error: {result.get('success_summary', {}).get('error', 'Unknown error')}")

    # Example 2: AVRO Kafka Source with Schema Registry
    print("\n💼 AVRO Kafka Source with Schema Registry Example")
    print("=" * 60)

    avro_kafka_config = KafkaConfig.create_avro_config(
        bootstrap_servers="localhost:9092",
        topic="orders",
        schema_registry_url="http://localhost:8081",
        schema_registry_username="schema_user",
        schema_registry_password="schema_pass",
        message="message_name",
        group_id_prefix="risingwave_orders"
    )

    # Custom table schema for AVRO
    order_schema = {
        "order_id": "BIGINT",
        "customer_id": "BIGINT",
        "product_id": "BIGINT",
        "quantity": "INTEGER",
        "price": "DECIMAL(10,2)",
        "order_time": "TIMESTAMP"
    }

    result = kafka_builder.create_connection(
        config=avro_kafka_config,
        table_name="orders",
        table_schema=order_schema,
        connection_type="source",  # Data not stored in RisingWave
        include_headers=True,
        include_key=True,
        dry_run=True
    )

    if result.get("success_summary", {}).get("overall_success"):
        print(f"✅ Created AVRO Kafka table!")
        print(
            f"  📊 {result['table_info']['table_name']}: {result['table_info']['format']} format")

        print("\n🔍 Example queries:")
        print("  -- Top selling products")
        print("  SELECT product_id, SUM(quantity * price) as revenue")
        print("  FROM orders GROUP BY product_id ORDER BY revenue DESC LIMIT 10;")

    else:
        print(
            f"❌ Error: {result.get('success_summary', {}).get('error', 'Unknown error')}")

    # Example 3: Secure Kafka with SASL/SSL
    print("\n💼 Secure Kafka with SASL/SSL Example")
    print("=" * 50)

    secure_kafka_config = KafkaConfig.create_secure_config(
        bootstrap_servers="kafka-cluster.company.com:9093",
        topic="financial_transactions",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username="finance_user",
        sasl_password="secure_password",
        ssl_ca_location="/path/to/ca-cert.pem",
        group_id_prefix="risingwave_finance"
    )

    result = kafka_builder.create_connection(
        config=secure_kafka_config,
        table_name="financial_transactions",
        include_offset=True,
        include_timestamp=True,
        dry_run=True
    )

    if result.get("success_summary", {}).get("overall_success"):
        print(f"✅ Created secure Kafka table!")
        print(
            f"  📊 {result['table_info']['table_name']}: {result['table_info']['format']} format")
    else:
        print(
            f"❌ Error: {result.get('success_summary', {}).get('error', 'Unknown error')}")

    # Example 4: UPSERT Kafka Table with Key
    print("\n💼 UPSERT Kafka Table Example")
    print("=" * 50)

    upsert_kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic="user_profiles",
        data_format="UPSERT",
        data_encode="JSON",
        group_id_prefix="risingwave_profiles"
    )

    # Custom table schema for UPSERT with primary key
    profile_schema = {
        "user_id": "INT PRIMARY KEY",
        "name": "VARCHAR",
        "email": "VARCHAR",
        "updated_at": "TIMESTAMP"
    }

    result = kafka_builder.create_connection(
        config=upsert_kafka_config,
        table_name="user_profiles",
        table_schema=profile_schema,
        connection_type="table",
        include_key=True,
        include_timestamp=True,
        dry_run=True
    )

    if result.get("success_summary", {}).get("overall_success"):
        print(f"✅ Created UPSERT Kafka table!")
        print(
            f"  📊 {result['table_info']['table_name']}: {result['table_info']['format']} format")

        print("\n🔍 Example UPSERT queries:")
        print("  -- Latest user profiles")
        print("  SELECT * FROM user_profiles ORDER BY updated_at DESC;")
        print("\n  -- User profile history")
        print("  SELECT user_id, name, kafka_timestamp")
        print("  FROM user_profiles WHERE user_id = 123 ORDER BY kafka_timestamp;")

    else:
        print(
            f"❌ Error: {result.get('success_summary', {}).get('error', 'Unknown error')}")

    # Print all generated SQL
    print(f"\n🔍 Generated SQL Statements ({len(result['sql_statements'])}):")
    for i, sql in enumerate(result['sql_statements'], 1):
        print(f"-- Statement {i}")
        print(sql)
        print("-" * 80)


if __name__ == "__main__":
    main()
