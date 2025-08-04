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
        tables_spec = pair.get("tables", "all")

        local_conn = connect_mysql(local_cfg)
        cloud_conn = connect_mysql(cloud_cfg)

        # Dynamically resolve tables
        with local_conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            all_tables = [list(row.values())[0] for row in cur.fetchall()]
            tables = [t for t in all_tables if t != "change_log"] if tables_spec == "all" else tables_spec

        ensure_change_log_table(local_conn, local_cfg["db"], tables)
        ensure_change_log_table(cloud_conn, cloud_cfg["db"], tables)

        # Actual sync
        sync_changes(local_conn, cloud_conn, node_id, tables)
        sync_changes(cloud_conn, local_conn, node_id, tables)

        local_conn.close()
        cloud_conn.close()