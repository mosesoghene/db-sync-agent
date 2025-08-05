from apscheduler.schedulers.background import BackgroundScheduler
from core.sync_engine import sync_changes_with_conflict_resolution, sync_changes
from core.connector import connect_mysql
import logging

logger = logging.getLogger(__name__)


def start_sync_scheduler(config, node_id):
    """Start the sync scheduler with directional sync support"""
    scheduler = BackgroundScheduler()

    def run_sync_job():
        logger.info("Starting scheduled sync job")
        try:
            for pair in config["sync_pairs"]:
                name = pair["name"]
                local_cfg = pair["local"]
                cloud_cfg = pair["cloud"]
                sync_config = config.get('sync', {})
                tables_config = sync_config.get('tables', {})

                local_conn = connect_mysql(local_cfg)
                cloud_conn = connect_mysql(cloud_cfg)

                # Get tables and their sync directions
                sync_tables = {}
                for table, table_config in tables_config.items():
                    if table_config:  # Only include tables that have sync configuration
                        direction = table_config.get('direction', 'bidirectional')
                        if direction != 'no_sync':  # Skip tables set to no_sync
                            sync_tables[table] = direction

                if not sync_tables:
                    logger.info(f"No tables configured for synchronization in pair {name}")
                    continue

                # Sync based on direction
                for table, direction in sync_tables.items():
                    try:
                        if direction in ['bidirectional', 'cloud_to_local']:
                            # Sync changes from cloud to local
                            logger.info(f"Syncing {table} from cloud to local")
                            sync_changes(cloud_conn, local_conn, node_id, [table])

                        if direction in ['bidirectional', 'local_to_cloud']:
                            # Sync changes from local to cloud
                            logger.info(f"Syncing {table} from local to cloud")
                            sync_changes(local_conn, cloud_conn, node_id, [table])
                    except Exception as e:
                        logger.error(f"Error syncing table {table}: {str(e)}")

                local_conn.close()
                cloud_conn.close()

        except Exception as e:
            logger.error(f"Sync job error: {str(e)}")

    # Get sync interval from config (default to 5 minutes)
    sync_interval = config.get('sync', {}).get('interval', 300) // 60  # Convert seconds to minutes

    # Add the job to run at the configured interval
    scheduler.add_job(run_sync_job, "interval", minutes=sync_interval, id="sync_job")
    scheduler.start()
    logger.info(f"Sync scheduler started with {sync_interval} minute interval")
    return scheduler


def start_sync_scheduler_with_conflict_resolution(config, node_id):
    scheduler = BackgroundScheduler()

    def run_sync_job():
        print("\n" + "=" * 60)
        print("🔄 Starting scheduled sync job with CONFLICT RESOLUTION")
        print("=" * 60)

        for pair in config["sync_pairs"]:
            name = pair["name"]
            tables = pair.get("tables", "all")

            # Get conflict resolution strategy from config (default: timestamp_wins)
            resolution_strategy = pair.get("conflict_resolution", "timestamp_wins")

            print(f"\n📋 Processing sync pair: {name}")
            print(f"🛡️ Conflict resolution strategy: {resolution_strategy}")

            local_conn = None
            cloud_conn = None

            try:
                # Connect to both databases
                local_conn = connect_mysql(pair["local"])
                cloud_conn = connect_mysql(pair["cloud"])

                print(f"🔗 Connected to local: {pair['local']['db']}")
                print(f"🔗 Connected to cloud: {pair['cloud']['db']}")

                # Bi-directional sync with conflict resolution
                sync_changes_with_conflict_resolution(
                    local_conn, cloud_conn, name, tables, resolution_strategy
                )
                sync_changes_with_conflict_resolution(
                    cloud_conn, local_conn, name, tables, resolution_strategy
                )

            except Exception as e:
                print(f"❌ Sync job failed: {e}")
                import traceback

                traceback.print_exc()

            finally:
                # Always close connections
                if local_conn:
                    local_conn.close()
                if cloud_conn:
                    cloud_conn.close()

        print(f"\n✅ Sync job completed for all pairs")

    # Run once at startup
    print("🚀 Running initial sync with conflict resolution...")
    run_sync_job()

    # Schedule repeated runs
    interval = config.get("sync_interval_minutes", 10)
    scheduler.add_job(run_sync_job, "interval", minutes=interval)

    print(f"\n⏰ Scheduled to sync every {interval} minutes")
    scheduler.start()

    return scheduler


# Show available conflict resolution strategies
def show_conflict_strategies():
    strategies = {
        "timestamp_wins": "Most recent change wins (requires updated_at column)",
        "source_wins": "Source database always wins",
        "target_wins": "Target database always wins",
        "merge_fields": "Merge non-conflicting fields only",
        "manual": "Log conflicts for manual resolution (safest)",
    }

    print("\n🛡️ Available Conflict Resolution Strategies:")
    print("=" * 50)
    for strategy, description in strategies.items():
        print(f"  {strategy:15} - {description}")
    print("=" * 50)