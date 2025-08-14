#!/usr/bin/env python3
"""
Simple test script to verify the SDK structure and imports work locally.
Run with: PYTHONPATH=src python3 test_imports.py
"""

import sys
import os

# Add src to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from risingwave_pipeline_sdk import RisingWaveClient, PostgresCDCPipeline, PostgresCDCConfig
    from risingwave_pipeline_sdk.sql_builders import build_create_source_postgres_cdc
    
    print("✓ All imports successful")
    
    # Test SQL builder
    sql = build_create_source_postgres_cdc(
        'test_source',
        hostname='localhost',
        port=5432,
        username='postgres',
        password='secret',
        database='testdb'
    )
    print("✓ SQL builder working")
    print("Sample SQL:")
    print(sql[:200] + "...")
    
    # Test config
    config = PostgresCDCConfig(
        postgres_host='localhost',
        postgres_database='testdb',
        auto_schema_change=True
    )
    print("✓ Config model working")
    print(f"Config postgres_host: {config.postgres_host}")
    
    print("\n✓ All core SDK components are functional!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
