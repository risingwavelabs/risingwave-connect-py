"""
Example: Interactive Table Discovery and Selection

This example shows how to interactively discover and select tables
for PostgreSQL CDC connection creation.
"""

from risingwave_connect import (
    RisingWaveClient,
    ConnectBuilder,
    PostgreSQLConfig,
    TableSelector
)


def main():
    print("🔗 Connecting to databases...")

    # Connect to RisingWave
    rw_client = RisingWaveClient(
        host="localhost",
        port=4566,
        user="root",
        database="dev"
    )

    # Configure PostgreSQL connection
    pg_config = PostgreSQLConfig(
        source_name="interactive_cdc_source",
        hostname="localhost",
        port=5432,
        username="postgres",
        password="secret",
        database="mydb",
        schema_name="public",
        auto_schema_change=True,
        backfill_num_rows_per_split="100000",
        backfill_parallelism="8"
    )

    builder = ConnectBuilder(rw_client)

    # Step 1: Discover available schemas
    print("\n📂 Discovering schemas...")
    try:
        schemas = builder.get_postgresql_schemas(pg_config)
        print(f"Available schemas: {', '.join(schemas)}")
    except Exception as e:
        print(f"❌ Could not connect to PostgreSQL: {e}")
        return

    # Step 2: Discover tables in specified schema
    print(f"\n🔍 Discovering tables in schema '{pg_config.schema_name}'...")
    available_tables = builder.discover_postgresql_tables(pg_config)

    if not available_tables:
        print(f"❌ No tables found in schema '{pg_config.schema_name}'")
        return

    print(f"✅ Found {len(available_tables)} tables:")
    for i, table in enumerate(available_tables, 1):
        size_mb = (table.size_bytes or 0) / 1024 / 1024
        print(f"  {i:2d}. {table.qualified_name:30} ({table.table_type:10}) "
              f"{table.row_count or 0:>8} rows, {size_mb:>6.1f} MB")

    # Step 3: Interactive table selection
    print(f"\n📋 Table selection options:")
    print("1. Include all tables")
    print("2. Select specific tables")
    print("3. Include all except certain tables")
    print("4. Pattern-based selection")

    choice = input("\nEnter your choice (1-4): ").strip()

    table_selector = None

    if choice == "1":
        # Include all tables
        table_selector = TableSelector(include_all=True)
        print("✅ Selected: All tables")

    elif choice == "2":
        # Select specific tables
        print("\nEnter table numbers to include (comma-separated, e.g., 1,3,5):")
        selected_nums = input("Tables: ").strip().split(",")

        selected_table_names = []
        for num_str in selected_nums:
            try:
                idx = int(num_str.strip()) - 1
                if 0 <= idx < len(available_tables):
                    selected_table_names.append(
                        available_tables[idx].qualified_name)
            except ValueError:
                print(f"❌ Invalid number: {num_str}")

        if selected_table_names:
            table_selector = TableSelector(
                specific_tables=selected_table_names)
            print(f"✅ Selected: {', '.join(selected_table_names)}")
        else:
            print("❌ No valid tables selected")
            return

    elif choice == "3":
        # Include all except certain tables
        print("\nEnter table numbers to EXCLUDE (comma-separated):")
        exclude_nums = input("Exclude: ").strip().split(",")

        exclude_patterns = []
        for num_str in exclude_nums:
            try:
                idx = int(num_str.strip()) - 1
                if 0 <= idx < len(available_tables):
                    exclude_patterns.append(available_tables[idx].table_name)
            except ValueError:
                print(f"❌ Invalid number: {num_str}")

        table_selector = TableSelector(
            include_all=True, exclude_patterns=exclude_patterns)
        print(f"✅ Selected: All tables except {', '.join(exclude_patterns)}")

    elif choice == "4":
        # Pattern-based selection
        print("\nEnter include patterns (e.g., user_*, order_*, product_*):")
        include_input = input("Include patterns: ").strip()
        include_patterns = [p.strip()
                            for p in include_input.split(",") if p.strip()]

        print("Enter exclude patterns (optional):")
        exclude_input = input("Exclude patterns: ").strip()
        exclude_patterns = [p.strip()
                            for p in exclude_input.split(",") if p.strip()]

        if include_patterns:
            table_selector = TableSelector(
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns if exclude_patterns else None
            )
            print(
                f"✅ Selected: Include {include_patterns}, Exclude {exclude_patterns}")
        else:
            print("❌ No include patterns specified")
            return

    else:
        print("❌ Invalid choice")
        return

    # Step 4: Preview selection
    print(f"\n🔍 Previewing table selection...")
    result = builder.create_postgresql_pipeline(
        config=pg_config,
        table_selector=table_selector,
        dry_run=True
    )

    selected_tables = result['selected_tables']
    print(f"✅ {len(selected_tables)} tables will be included in CDC:")
    for table in selected_tables:
        print(f"  - {table.qualified_name}")

    # Step 5: Confirm and create connection
    confirm = input(
        f"\n❓ Create CDC connection with {len(selected_tables)} tables? (y/N): ").strip().lower()

    if confirm in ['y', 'yes']:
        print(f"\n🚀 Creating PostgreSQL CDC connection...")
        result = builder.create_postgresql_pipeline(
            config=pg_config,
            table_selector=table_selector,
            dry_run=False
        )

        print(f"✅ Connection created successfully!")
        print(f"  Source: {pg_config.source_name}")
        print(f"  Tables: {len(result['selected_tables'])}")
        print(
            f"  SQL statements executed: {len(result.get('executed_statements', []))}")

        # Show generated SQL
        print(f"\n📝 Generated SQL:")
        print("="*80)
        for sql in result['sql_statements']:
            print(f"{sql}\n")
        print("="*80)

    else:
        print("❌ Connection creation cancelled")


if __name__ == "__main__":
    main()
