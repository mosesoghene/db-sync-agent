from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

from core.schema import ensure_change_log_table
from core.sync_engine import sync_changes
from core.connector import connect_mysql

import logging

logger = logging.getLogger("sync_gui")


def start_sync_scheduler(config, node_id):
    scheduler = BackgroundScheduler()

    def run_sync_job():
        try:
            pairs = config.get("sync_pairs", [])
            if not pairs:
                logger.warning("⚠ No sync pairs configured.")
                return

            for pair in pairs:
                local_cfg = pair.get("local")
                cloud_cfg = pair.get("cloud")
                tables_spec = pair.get("tables", "all")

                if not (local_cfg and cloud_cfg):
                    logger.warning("⚠️ Skipping sync pair due to missing local/cloud config.")
                    continue

                local_conn = connect_mysql(local_cfg)
                cloud_conn = connect_mysql(cloud_cfg)

                # Dynamically get table list
                with local_conn.cursor() as cur:
                    cur.execute("SHOW TABLES")
                    all_tables = [list(row.values())[0] for row in cur.fetchall()]
                    tables = [t for t in all_tables if t != "change_log"] if tables_spec == "all" else tables_spec

                # Ensure change_log tables and triggers exist
                ensure_change_log_table(local_conn, local_cfg["db"], tables)
                ensure_change_log_table(cloud_conn, cloud_cfg["db"], tables)

                # Run sync in both directions
                sync_changes(local_conn, cloud_conn, node_id, tables)
                sync_changes(cloud_conn, local_conn, node_id, tables)

                local_conn.close()
                cloud_conn.close()

        except Exception as e:
            logger.exception(f"❌ Sync job failed: {e}")

    # Log scheduler-level warnings
    def scheduler_listener(event):
        if event.exception:
            logger.error("🚨 Scheduled job raised an exception.")
        elif event.code == EVENT_JOB_MISSED:
            logger.warning("⚠️ Scheduled job missed its run window.")

    scheduler.add_listener(scheduler_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)

    # Run once immediately
    run_sync_job()

    # Schedule future runs
    interval = config.get("sync_interval_minutes", 10)
    scheduler.add_job(
        run_sync_job,
        trigger="interval",
        minutes=interval,
        misfire_grace_time=60,  # allow job to run up to 1 minute late
    )

    logger.info(f"⏰ Sync job scheduled every {interval} minutes")
    scheduler.start()
