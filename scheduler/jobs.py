from apscheduler.schedulers.background import BackgroundScheduler
from core.sync_engine import sync_changes
from core.connector import connect_mysql

def start_sync_scheduler(config, node_id):
    scheduler = BackgroundScheduler()

    def run_sync_job():
        for pair in config["sync_pairs"]:
            name = pair["name"]
            tables = pair.get("tables", "all")
            print(f"\n🔄 Running sync for: {name}")

            try:
                local = connect_mysql(pair["local"])
                cloud = connect_mysql(pair["cloud"])

                # local → cloud
                sync_changes(local, cloud, node_id, tables)

                # cloud → local
                sync_changes(cloud, local, node_id, tables)

            except Exception as e:
                print(f"❌ Sync error on {name}: {e}")

    # Run once at startup
    run_sync_job()

    # Schedule repeated runs
    interval = config.get("sync_interval_minutes", 10)
    scheduler.add_job(run_sync_job, "interval", minutes=interval)

    print(f"⏰ Scheduled to sync every {interval} minutes")
    scheduler.start()
