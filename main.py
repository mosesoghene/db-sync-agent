from core.config import load_config
from core.connector import connect_mysql
from core.schema import ensure_change_log_table, setup_triggers
from scheduler.jobs import start_sync_scheduler_with_conflict_resolution, show_conflict_strategies
import uuid


def generate_database_node_id(sync_pair_name, db_type):
    """Generate a unique node ID for each database in the sync pair"""
    base_string = f"{sync_pair_name}_{db_type}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, base_string).hex


def initialize_sync_infrastructure_with_conflict_resolution():
    """Set up change_log tables and triggers with proper node IDs and conflict resolution."""
    config = load_config()
    main_node_id = config["node_id"]

    print(f"🔧 Initializing sync infrastructure with CONFLICT RESOLUTION")
    print(f"🆔 Sync Agent Node ID: {main_node_id}")

    # Show available conflict resolution strategies
    show_conflict_strategies()

    for pair in config["sync_pairs"]:
        name = pair["name"]
        tables = pair.get("tables", "all")
        resolution_strategy = pair.get("conflict_resolution", "timestamp_wins")

        print(f"\n📋 Setting up sync pair: {name}")
        print(f"🛡️ Conflict resolution strategy: {resolution_strategy}")

        # Generate unique node IDs for each database
        local_node_id = generate_database_node_id(name, "local")
        cloud_node_id = generate_database_node_id(name, "cloud")

        print(f"  🆔 Local DB Node ID: {local_node_id[:12]}...")
        print(f"  🆔 Cloud DB Node ID: {cloud_node_id[:12]}...")

        # Initialize local database
        try:
            local_conn = connect_mysql(pair["local"])
            local_db = pair["local"]["db"]

            print(f"  🔧 Setting up local DB: {local_db}")
            ensure_change_log_table(local_conn)
            setup_triggers(local_conn, local_db, tables, local_node_id)

            # Create conflict_log table
            with local_conn.cursor() as cur:
                cur.execute("""
                            CREATE TABLE IF NOT EXISTS conflict_log
                            (
                                id
                                BIGINT
                                AUTO_INCREMENT
                                PRIMARY
                                KEY,
                                change_id
                                BIGINT
                                NOT
                                NULL,
                                table_name
                                VARCHAR
                            (
                                255
                            ) NOT NULL,
                                record_pk VARCHAR
                            (
                                255
                            ) NOT NULL,
                                conflict_type VARCHAR
                            (
                                50
                            ) NOT NULL,
                                source_data JSON,
                                target_data JSON,
                                conflict_details JSON,
                                resolution VARCHAR
                            (
                                50
                            ) NOT NULL,
                                resolved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                INDEX idx_table_pk
                            (
                                table_name,
                                record_pk
                            ),
                                INDEX idx_resolved_at
                            (
                                resolved_at
                            ),
                                INDEX idx_resolution
                            (
                                resolution
                            )
                                )
                            """)

            local_conn.close()
            print(f"  ✅ Local DB setup complete")

        except Exception as e:
            print(f"  ❌ Local DB setup failed for {name}: {e}")
            import traceback
            traceback.print_exc()
            continue

        # Initialize cloud database
        try:
            cloud_conn = connect_mysql(pair["cloud"])
            cloud_db = pair["cloud"]["db"]

            print(f"  🔧 Setting up cloud DB: {cloud_db}")
            ensure_change_log_table(cloud_conn)
            setup_triggers(cloud_conn, cloud_db, tables, cloud_node_id)

            # Create conflict_log table
            with cloud_conn.cursor() as cur:
                cur.execute("""
                            CREATE TABLE IF NOT EXISTS conflict_log
                            (
                                id
                                BIGINT
                                AUTO_INCREMENT
                                PRIMARY
                                KEY,
                                change_id
                                BIGINT
                                NOT
                                NULL,
                                table_name
                                VARCHAR
                            (
                                255
                            ) NOT NULL,
                                record_pk VARCHAR
                            (
                                255
                            ) NOT NULL,
                                conflict_type VARCHAR
                            (
                                50
                            ) NOT NULL,
                                source_data JSON,
                                target_data JSON,
                                conflict_details JSON,
                                resolution VARCHAR
                            (
                                50
                            ) NOT NULL,
                                resolved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                INDEX idx_table_pk
                            (
                                table_name,
                                record_pk
                            ),
                                INDEX idx_resolved_at
                            (
                                resolved_at
                            ),
                                INDEX idx_resolution
                            (
                                resolution
                            )
                                )
                            """)

            cloud_conn.close()
            print(f"  ✅ Cloud DB setup complete")

        except Exception as e:
            print(f"  ❌ Cloud DB setup failed for {name}: {e}")
            import traceback
            traceback.print_exc()
            continue

        print(f"  🎯 Sync pair '{name}' initialized successfully with conflict resolution")


def test_connections():
    """Test database connections."""
    config = load_config()
    print("Sync Agent Node ID:", config["node_id"])

    for pair in config["sync_pairs"]:
        print(f"\n🔗 Testing pair: {pair['name']}")

        try:
            local_conn = connect_mysql(pair["local"])
            with local_conn.cursor() as cur:
                cur.execute("SELECT DATABASE()")
                print("  ✅ Local DB connected:", cur.fetchone()["DATABASE()"])
            local_conn.close()
        except Exception as e:
            print("  ❌ Local DB error:", e)

        try:
            cloud_conn = connect_mysql(pair["cloud"])
            with cloud_conn.cursor() as cur:
                cur.execute("SELECT DATABASE()")
                print("  ✅ Cloud DB connected:", cur.fetchone()["DATABASE()"])
            cloud_conn.close()
        except Exception as e:
            print("  ❌ Cloud DB error:", e)


def show_startup_info():
    """Show information about the conflict resolution system"""
    print("🛡️ CONFLICT RESOLUTION SYSTEM ACTIVE")
    print("=" * 60)
    print("This sync agent now includes advanced conflict resolution:")
    print("• Detects when both databases have changes to the same record")
    print("• Applies configurable resolution strategies")
    print("• Logs all conflicts for monitoring and analysis")
    print("• Supports timestamp-based, field-level, and manual resolution")
    print()
    print("📊 To monitor conflicts, run: python conflict_monitor.py")
    print("⚙️ To change strategies, edit the 'conflict_resolution' field in config.json")
    print("=" * 60)


if __name__ == "__main__":
    config = load_config()
    main_node_id = config["node_id"]

    print("=" * 70)
    print("🚀 MySQL Sync Agent with CONFLICT RESOLUTION")
    print("=" * 70)

    show_startup_info()

    # Step 1: Test connections
    print("\n🔍 Testing database connections...")
    test_connections()

    # Step 2: Initialize sync infrastructure with conflict resolution
    print("\n🏗️ Setting up sync infrastructure...")
    initialize_sync_infrastructure_with_conflict_resolution()

    # Step 3: Start the conflict-aware sync scheduler
    print("\n⏰ Starting conflict-aware sync scheduler...")
    start_sync_scheduler_with_conflict_resolution(config, main_node_id)

    # Keep the main thread alive
    try:
        interval = config.get('sync_interval_minutes', 10)
        print(f"\n✅ Conflict-aware sync agent is running!")
        print(f"⏰ Syncing every {interval} minutes with conflict resolution")
        print(f"🛡️ Monitor conflicts: python conflict_monitor.py")
        print("\nPress Ctrl+C to stop.\n")
        while True:
            pass
    except KeyboardInterrupt:
        print("\n🛑 Exiting sync agent")