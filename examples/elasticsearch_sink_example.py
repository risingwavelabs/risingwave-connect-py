"""
Elasticsearch Sink Examples for RisingWave Connect

This example demonstrates various Elasticsearch sink configurations matching
the official RisingWave documentation including the auto.schema.change parameter.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from risingwave_connect.sinks.elasticsearch import ElasticsearchConfig, ElasticsearchSink


def example_1_basic_elasticsearch_sink():
    """Example 1: Basic Elasticsearch sink with authentication."""
    print("=" * 70)
    print("Example 1: Basic Elasticsearch Sink with Authentication")
    print("=" * 70)
    
    config = ElasticsearchConfig(
        sink_name="basic_elasticsearch_sink",
        url="http://localhost:9200",
        index="src",
        username="elastic",
        password="changeme",
        primary_key="v",
        auto_schema_change=True
    )
    
    sink = ElasticsearchSink(config)
    sql = sink.create_sink_sql(source_name="src")
    
    print("Configuration:")
    print(f"  URL: {config.url}")
    print(f"  Index: {config.index}")
    print(f"  Username: {config.username}")
    print(f"  Primary Key: {config.primary_key}")
    print(f"  Auto Schema Change: {config.auto_schema_change}")
    print(f"  Sink Name: {config.sink_name}")
    
    print("\nGenerated SQL:")
    print(sql)
    
    return sql


def example_2_dynamic_index_elasticsearch():
    """Example 2: Elasticsearch sink with dynamic index based on column value."""
    print("\n" + "=" * 70)
    print("Example 2: Dynamic Index Elasticsearch Sink")
    print("=" * 70)
    
    config = ElasticsearchConfig.for_dynamic_index(
        url="http://localhost:9200",
        index_column="event_type",
        username="elastic",
        password="changeme",
        primary_key="id",
        auto_schema_change=True
    )
    
    sink = ElasticsearchSink(config)
    sql = sink.create_sink_sql(source_name="events_table")
    
    print("Configuration:")
    print(f"  URL: {config.url}")
    print(f"  Index Column: {config.index_column}")
    print(f"  Username: {config.username}")
    print(f"  Primary Key: {config.primary_key}")
    print(f"  Auto Schema Change: {config.auto_schema_change}")
    
    print("\nGenerated SQL:")
    print(sql)
    
    return sql


def example_3_high_throughput_elasticsearch():
    """Example 3: High-throughput Elasticsearch sink with performance tuning."""
    print("\n" + "=" * 70)
    print("Example 3: High-Throughput Elasticsearch Sink")
    print("=" * 70)
    
    config = ElasticsearchConfig.for_high_throughput(
        url="http://localhost:9200",
        index="high_volume_logs",
        username="elastic",
        password="changeme",
        batch_size_kb=8000,      # 8MB batches
        batch_num_messages=2000, # 2000 messages per batch
        concurrent_requests=8,   # 8 concurrent threads
        retry_on_conflict=3,     # 3 retry attempts
        primary_key="log_id",
        auto_schema_change=True
    )
    
    sink = ElasticsearchSink(config)
    sql = sink.create_sink_sql(source_name="logs_stream")
    
    print("Configuration:")
    print(f"  URL: {config.url}")
    print(f"  Index: {config.index}")
    print(f"  Batch Size KB: {config.batch_size_kb}")
    print(f"  Batch Num Messages: {config.batch_num_messages}")
    print(f"  Concurrent Requests: {config.concurrent_requests}")
    print(f"  Retry on Conflict: {config.retry_on_conflict}")
    print(f"  Auto Schema Change: {config.auto_schema_change}")
    
    print("\nGenerated SQL:")
    print(sql)
    
    return sql


def example_4_routing_and_custom_delimiter():
    """Example 4: Elasticsearch sink with routing and custom delimiter."""
    print("\n" + "=" * 70)
    print("Example 4: Elasticsearch with Routing and Custom Delimiter")
    print("=" * 70)
    
    config = ElasticsearchConfig(
        url="http://localhost:9200",
        index="user_activities",
        primary_key="user_id,activity_id",  # Composite primary key
        delimiter="|",                       # Custom delimiter for composite key
        routing_column="user_id",           # Route by user_id for better performance
        username="elastic",
        password="changeme",
        auto_schema_change=False,           # Strict schema mode
        batch_size_kb=1024,                # 1MB batches
        concurrent_requests=3
    )
    
    sink = ElasticsearchSink(config)
    sql = sink.create_sink_sql(source_name="user_activities")
    
    print("Configuration:")
    print(f"  URL: {config.url}")
    print(f"  Index: {config.index}")
    print(f"  Primary Key: {config.primary_key}")
    print(f"  Delimiter: {config.delimiter}")
    print(f"  Routing Column: {config.routing_column}")
    print(f"  Auto Schema Change: {config.auto_schema_change}")
    
    print("\nGenerated SQL:")
    print(sql)
    
    return sql


def example_5_unauthenticated_local_elasticsearch():
    """Example 5: Local Elasticsearch without authentication."""
    print("\n" + "=" * 70)
    print("Example 5: Local Elasticsearch (No Authentication)")
    print("=" * 70)
    
    config = ElasticsearchConfig(
        url="http://localhost:9200",
        index="development_logs",
        primary_key="timestamp",
        auto_schema_change=True
    )
    
    sink = ElasticsearchSink(config)
    sql = sink.create_sink_sql(source_name="dev_logs")
    
    print("Configuration:")
    print(f"  URL: {config.url}")
    print(f"  Index: {config.index}")
    print(f"  Primary Key: {config.primary_key}")
    print(f"  Authentication: None")
    print(f"  Auto Schema Change: {config.auto_schema_change}")
    
    print("\nGenerated SQL:")
    print(sql)
    
    return sql


def example_6_complex_select_query():
    """Example 6: Elasticsearch sink with complex SELECT query."""
    print("\n" + "=" * 70)
    print("Example 6: Elasticsearch Sink with Custom SELECT Query")
    print("=" * 70)
    
    config = ElasticsearchConfig(
        url="http://localhost:9200",
        index="analytics_summary",
        username="elastic",
        password="changeme",
        primary_key="summary_id",
        auto_schema_change=True,
        batch_size_kb=2048,
        retry_on_conflict=5
    )
    
    # Custom SELECT query for data transformation
    select_query = """
    SELECT 
        CONCAT(user_id, '_', DATE_TRUNC('hour', event_time)) as summary_id,
        user_id,
        DATE_TRUNC('hour', event_time) as hour_bucket,
        COUNT(*) as event_count,
        AVG(duration) as avg_duration,
        MAX(severity) as max_severity,
        CURRENT_TIMESTAMP as processed_at
    FROM user_events 
    WHERE event_time >= NOW() - INTERVAL '24 hours'
    GROUP BY user_id, DATE_TRUNC('hour', event_time)
    """
    
    sink = ElasticsearchSink(config)
    sql = sink.create_sink_sql(select_query=select_query)
    
    print("Configuration:")
    print(f"  URL: {config.url}")
    print(f"  Index: {config.index}")
    print(f"  Primary Key: {config.primary_key}")
    print(f"  Auto Schema Change: {config.auto_schema_change}")
    
    print("\nGenerated SQL:")
    print(sql)
    
    return sql


def example_7_validation_and_error_handling():
    """Example 7: Demonstrate validation and error handling."""
    print("\n" + "=" * 70)
    print("Example 7: Validation and Error Handling")
    print("=" * 70)
    
    print("Testing various validation scenarios...\n")
    
    # Test 1: Missing index and index_column
    try:
        config = ElasticsearchConfig(
            url="http://localhost:9200"
            # Missing both index and index_column
        )
        print("❌ Should have failed: Missing index/index_column")
    except ValueError as e:
        print(f"✅ Correctly caught error: {e}")
    
    # Test 2: Both index and index_column provided
    try:
        config = ElasticsearchConfig(
            url="http://localhost:9200",
            index="test_index",
            index_column="dynamic_index"  # Both provided - should fail
        )
        print("❌ Should have failed: Both index and index_column provided")
    except ValueError as e:
        print(f"✅ Correctly caught error: {e}")
    
    # Test 3: Incomplete authentication
    try:
        config = ElasticsearchConfig(
            url="http://localhost:9200",
            index="test_index",
            username="admin"  # Password missing
        )
        print("❌ Should have failed: Incomplete authentication")
    except ValueError as e:
        print(f"✅ Correctly caught error: {e}")
    
    # Test 4: Invalid URL format
    try:
        config = ElasticsearchConfig(
            url="invalid-url-format",  # Should start with http/https
            index="test_index"
        )
        print("❌ Should have failed: Invalid URL format")
    except ValueError as e:
        print(f"✅ Correctly caught error: {e}")
    
    # Test 5: Valid configuration
    try:
        config = ElasticsearchConfig(
            url="https://elasticsearch.example.com:9200",
            index="valid_index",
            username="user",
            password="pass",
            primary_key="id",
            auto_schema_change=True
        )
        sink = ElasticsearchSink(config)
        issues = sink.validate_config()
        if not issues:
            print("✅ Valid configuration passed all checks")
        else:
            print(f"❌ Unexpected validation issues: {issues}")
    except Exception as e:
        print(f"❌ Unexpected error with valid config: {e}")


def show_config_properties_format():
    """Show how configurations are converted to RisingWave WITH properties."""
    print("\n" + "=" * 70)
    print("Configuration Properties Format")
    print("=" * 70)
    
    config = ElasticsearchConfig(
        url="http://localhost:9200",
        index="my_index",
        username="elastic",
        password="changeme",
        primary_key="id",
        delimiter="|",
        routing_column="shard_key",
        retry_on_conflict=3,
        batch_size_kb=4096,
        batch_num_messages=1500,
        concurrent_requests=5,
        auto_schema_change=True
    )
    
    properties = config.to_with_properties()
    
    print("WITH properties generated:")
    for key, value in properties.items():
        print(f"   {key} = {value}")


def main():
    """Run all Elasticsearch sink examples."""
    print("🔍 RisingWave Elasticsearch Sink Examples")
    print("========================================")
    
    # Run all examples
    sqls = []
    sqls.append(example_1_basic_elasticsearch_sink())
    sqls.append(example_2_dynamic_index_elasticsearch())
    sqls.append(example_3_high_throughput_elasticsearch())
    sqls.append(example_4_routing_and_custom_delimiter())
    sqls.append(example_5_unauthenticated_local_elasticsearch())
    sqls.append(example_6_complex_select_query())
    
    # Validation examples
    example_7_validation_and_error_handling()
    
    # Show properties format
    show_config_properties_format()
    
    print("\n" + "=" * 70)
    print("🎉 All Examples Complete!")
    print("=" * 70)
    print(f"Generated {len(sqls)} SQL statements for different Elasticsearch configurations.")
    print("Each example demonstrates different aspects of the Elasticsearch sink:")
    print("  • Basic authentication and auto schema changes")
    print("  • Dynamic index selection based on column values")
    print("  • High-throughput configuration with performance tuning")
    print("  • Routing and composite primary keys")
    print("  • Local development setup")
    print("  • Custom SELECT queries for data transformation")
    print("  • Comprehensive validation and error handling")
    
    return sqls


if __name__ == "__main__":
    main()