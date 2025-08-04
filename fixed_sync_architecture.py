"""
Fixed sync architecture - each database gets its own unique node ID for triggers
"""

from core.config import load_config
from core.connector import connect_mysql
from core.schema import ensure_change_log_table, setup_triggers
from core.scheduler.jobs import start_sync_scheduler
import uuid


def generate_database_node_id(sync_pair_name, db_type):
    """Generate a unique node ID for each database in the sync pair"""
    # Create a deterministic but unique ID for each database
    base_string = f"{sync_pair_name}_{db_type}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, base_string).hex


def initialize_sync_infrastructure_fixed():
    """Set up change_log tables and triggers with proper node IDs."""
    config = load_config()
    main_node_id = config["node_id"]  # This is the sync agent's ID

    print(f"🔧 Initializing sync infrastructure")
    print(f"🆔 Sync Agent Node ID: {main_node_id}")

    for pair in config["sync_pairs"]:
        name = pair["name"]
        tables = pair.get("tables", "all")

        print(f"\n📋 Setting up sync pair: {name}")

        # Generate unique node IDs for each database
        local_node_id = generate_database_node_id(name, "local")
        cloud_node_id = generate_database_node_id(name, "cloud")

        print(f"  🆔 Local DB Node ID: {local_node_id}")
        print(f"  🆔 Cloud DB Node ID: {cloud_node_id}")

        # Initialize local database with its own node ID
        try:
            local_conn = connect_mysql(pair["local"])
            local_db = pair["local"]["db"]

            print(f"  🔧 Setting up local DB: {local_db}")
            ensure_change_log_table(local_conn)
            setup_triggers(local_conn, local_db, tables, local_node_id)  # Use local node ID
            local_conn.close()
            print(f"  ✅ Local DB setup complete")

        except Exception as e:
            print(f"  ❌ Local DB setup failed for {name}: {e}")
            continue

        # Initialize cloud database with its own node ID
        try:
            cloud_conn = connect_mysql(pair["cloud"])
            cloud_db = pair["cloud"]["db"]

            print(f"  🔧 Setting up cloud DB: {cloud_db}")
            ensure_change_log_table(cloud_conn)
            setup_triggers(cloud_conn, cloud_db, tables, cloud_node_id)  # Use cloud node ID
            cloud_conn.close()
            print(f"  ✅ Cloud DB setup complete")

        except Exception as e:
            print(f"  ❌ Cloud DB setup failed for {name}: {e}")
            continue

        print(f"  🎯 Sync pair '{name}' initialized successfully")

        # Store the database node IDs in the config for the sync engine
        pair["_local_node_id"] = local_node_id
        pair["_cloud_node_id"] = cloud_node_id


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


if __name__ == "__main__":
    config = load_config()
    main_node_id = config["node_id"]

    print("=" * 60)
    print("🚀 Starting MySQL Sync Agent (FIXED)")
    print("=" * 60)

    # Step 1: Test connections
    print("\n🔍 Testing database connections...")
    test_connections()

    # Step 2: Initialize sync infrastructure with proper node IDs
    print("\n🏗️ Setting up sync infrastructure with unique node IDs...")
    initialize_sync_infrastructure_fixed()

    # Step 3: Start the sync scheduler
    print("\n⏰ Starting sync scheduler...")
    start_sync_scheduler(config, main_node_id)

    # Keep the main thread alive
    try:
        print(f"\n✅ Sync agent is running! Syncing every {config.get('sync_interval_minutes', 10)} minutes")
        print("Press Ctrl+C to stop.\n")
        while True:
            pass
    except KeyboardInterrupt:
        print("\n🛑 Exiting sync agent")
