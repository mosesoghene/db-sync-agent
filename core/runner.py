from core.connector import connect_mysql
from core.schema import ensure_change_log_table
from core.sync_engine import sync_changes
from core.scheduler.jobs import start_sync_scheduler
from core.config import load_config
import logging

logger = logging.getLogger("sync_gui")

scheduler = None

def start_sync():
    global scheduler
    config = load_config()
    node_id = config["node_id"]

    if scheduler:
        print("⚠️ Scheduler already running")
        return

    scheduler = start_sync_scheduler(config, node_id)

def stop_sync():
    global scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
        scheduler = None
        print("🛑 Scheduler stopped")


def run_one_time_sync(config, node_id):
    pairs = config.get("sync_pairs", [])

    for pair in pairs:
        local_cfg = pair["local"]
        cloud_cfg = pair["cloud"]
        sync_config = config.get('sync', {})
        tables_config = sync_config.get('tables', {})

        local_conn = connect_mysql(local_cfg)
        cloud_conn = connect_mysql(cloud_cfg)

        # Dynamically resolve tables
        with local_conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            all_tables = [list(row.values())[0] for row in cur.fetchall()]
            available_tables = [t for t in all_tables if t != "change_log"]

        # Get tables and their sync directions
        sync_tables = {}
        for table in available_tables:
            table_config = tables_config.get(table, {})
            if table_config:  # Only include tables that have sync configuration
                direction = table_config.get('direction', 'bidirectional')
                if direction != 'no_sync':  # Skip tables set to no_sync
                    sync_tables[table] = direction

        if not sync_tables:
            logger.info("No tables configured for synchronization")
            continue

        # Ensure change log tables exist for selected tables
        ensure_change_log_table(local_conn, local_cfg["db"], list(sync_tables.keys()))
        ensure_change_log_table(cloud_conn, cloud_cfg["db"], list(sync_tables.keys()))

        # Sync based on direction
        for table, direction in sync_tables.items():
            if direction in ['bidirectional', 'cloud_to_local']:
                # Sync changes from cloud to local
                sync_changes(cloud_conn, local_conn, node_id, [table])

            if direction in ['bidirectional', 'local_to_cloud']:
                # Sync changes from local to cloud
                sync_changes(local_conn, cloud_conn, node_id, [table])

        local_conn.close()
        cloud_conn.close()