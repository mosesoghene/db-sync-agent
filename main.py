from core.config import load_config
from core.connector import connect_mysql
from scheduler.jobs import start_sync_scheduler


def test_connections():
    config = load_config()
    print("Node ID:", config["node_id"])

    for pair in config["sync_pairs"]:
        print(f"\n🔗 Testing pair: {pair['name']}")

        try:
            local_conn = connect_mysql(pair["local"])
            with local_conn.cursor() as cur:
                cur.execute("SELECT DATABASE()")
                print("  ✅ Local DB connected:", cur.fetchone()["DATABASE()"])
        except Exception as e:
            print("  ❌ Local DB error:", e)

        try:
            cloud_conn = connect_mysql(pair["cloud"])
            with cloud_conn.cursor() as cur:
                cur.execute("SELECT DATABASE()")
                print("  ✅ Cloud DB connected:", cur.fetchone()["DATABASE()"])
        except Exception as e:
            print("  ❌ Cloud DB error:", e)

if __name__ == "__main__":
    config = load_config()
    node_id = config["node_id"]

    start_sync_scheduler(config, node_id)

    # Keep the main thread alive
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("🛑 Exiting sync agent")

