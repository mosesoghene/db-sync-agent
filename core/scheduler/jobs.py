from apscheduler.schedulers.background import BackgroundScheduler
from core.sync_engine import sync_changes_with_conflict_resolution
from core.connector import connect_mysql


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
        'timestamp_wins': 'Most recent change wins (requires updated_at column)',
        'source_wins': 'Source database always wins',
        'target_wins': 'Target database always wins',
        'merge_fields': 'Merge non-conflicting fields only',
        'manual': 'Log conflicts for manual resolution (safest)'
    }

    print("\n🛡️ Available Conflict Resolution Strategies:")
    print("=" * 50)
    for strategy, description in strategies.items():
        print(f"  {strategy:15} - {description}")
    print("=" * 50)