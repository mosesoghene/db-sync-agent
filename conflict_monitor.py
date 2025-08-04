"""
Conflict Resolution Monitor - View and manage sync conflicts
"""

from core.config import load_config
from core.connector import connect_mysql
import json


def show_conflict_summary():
    """Show a summary of all conflicts across sync pairs"""
    config = load_config()

    print("\n🛡️ CONFLICT RESOLUTION SUMMARY")
    print("=" * 60)

    total_conflicts = 0

    for pair in config["sync_pairs"]:
        print(f"\n📋 Sync Pair: {pair['name']}")

        for db_type in ["local", "cloud"]:
            try:
                conn = connect_mysql(pair[db_type])
                db_name = pair[db_type]["db"]

                with conn.cursor() as cur:
                    # Check if conflict_log table exists
                    cur.execute("SHOW TABLES LIKE 'conflict_log'")
                    if not cur.fetchone():
                        print(f"  📭 No conflict log in {db_type} database")
                        continue

                    # Get conflict summary
                    cur.execute("""
                                SELECT conflict_type,
                                       resolution,
                                       COUNT(*) as count,
                            MAX(resolved_at) as latest_conflict
                                FROM conflict_log
                                GROUP BY conflict_type, resolution
                                ORDER BY count DESC
                                """)

                    conflicts = cur.fetchall()

                    if conflicts:
                        print(f"  🔥 {db_type.upper()} Database Conflicts:")
                        for conflict in conflicts:
                            print(
                                f"    {conflict['conflict_type']} ({conflict['resolution']}): {conflict['count']} conflicts")
                            print(f"      Latest: {conflict['latest_conflict']}")
                            total_conflicts += conflict['count']
                    else:
                        print(f"  ✅ No conflicts in {db_type} database")

                conn.close()

            except Exception as e:
                print(f"  ❌ Error checking {db_type}: {e}")

    print(f"\n📊 TOTAL CONFLICTS ACROSS ALL DATABASES: {total_conflicts}")


def show_recent_conflicts(limit=10):
    """Show recent conflicts in detail"""
    config = load_config()

    print(f"\n🔥 RECENT CONFLICTS (Last {limit})")
    print("=" * 80)

    all_conflicts = []

    for pair in config["sync_pairs"]:
        for db_type in ["local", "cloud"]:
            try:
                conn = connect_mysql(pair[db_type])

                with conn.cursor() as cur:
                    cur.execute("SHOW TABLES LIKE 'conflict_log'")
                    if not cur.fetchone():
                        continue

                    cur.execute("""
                                SELECT table_name,
                                       record_pk,
                                       conflict_type,
                                       resolution,
                                       source_data,
                                       target_data,
                                       conflict_details,
                                       resolved_at
                                FROM conflict_log
                                ORDER BY resolved_at DESC
                                    LIMIT %s
                                """, (limit,))

                    conflicts = cur.fetchall()
                    for conflict in conflicts:
                        conflict['database'] = f"{pair['name']}.{db_type}"
                        all_conflicts.append(conflict)

                conn.close()

            except Exception as e:
                print(f"❌ Error getting conflicts from {pair['name']}.{db_type}: {e}")

    # Sort all conflicts by time
    all_conflicts.sort(key=lambda x: x['resolved_at'], reverse=True)

    for i, conflict in enumerate(all_conflicts[:limit], 1):
        print(f"\n{i}. {conflict['database']} - {conflict['table_name']} [PK: {conflict['record_pk']}]")
        print(f"   Type: {conflict['conflict_type']} | Resolution: {conflict['resolution']}")
        print(f"   Time: {conflict['resolved_at']}")

        # Show conflict details if available
        if conflict['conflict_details']:
            try:
                details = json.loads(conflict['conflict_details']) if isinstance(conflict['conflict_details'], str) else \
                conflict['conflict_details']
                if details.get('type') == 'field_conflict':
                    print(f"   Field conflicts:")
                    for field_conflict in details.get('conflicts', []):
                        print(
                            f"     - {field_conflict['field']}: source='{field_conflict['source_value']}' vs target='{field_conflict['target_value']}'")
                elif details.get('type') == 'timestamp_conflict':
                    print(f"   Timestamp conflict: source={details['source_time']} vs target={details['target_time']}")
            except:
                pass


def show_manual_resolution_queue():
    """Show conflicts that need manual resolution"""
    config = load_config()

    print("\n📝 MANUAL RESOLUTION QUEUE")
    print("=" * 60)

    manual_conflicts = []

    for pair in config["sync_pairs"]:
        for db_type in ["local", "cloud"]:
            try:
                conn = connect_mysql(pair[db_type])

                with conn.cursor() as cur:
                    cur.execute("SHOW TABLES LIKE 'conflict_log'")
                    if not cur.fetchone():
                        continue

                    cur.execute("""
                                SELECT id,
                                       table_name,
                                       record_pk,
                                       conflict_type,
                                       source_data,
                                       target_data,
                                       conflict_details,
                                       resolved_at
                                FROM conflict_log
                                WHERE resolution = 'manual'
                                ORDER BY resolved_at DESC
                                """)

                    conflicts = cur.fetchall()
                    for conflict in conflicts:
                        conflict['database'] = f"{pair['name']}.{db_type}"
                        manual_conflicts.append(conflict)

                conn.close()

            except Exception as e:
                print(f"❌ Error getting manual conflicts: {e}")

    if not manual_conflicts:
        print("✅ No conflicts pending manual resolution")
        return

    for i, conflict in enumerate(manual_conflicts, 1):
        print(f"\n{i}. [ID: {conflict['id']}] {conflict['database']} - {conflict['table_name']}")
        print(f"   Record PK: {conflict['record_pk']}")
        print(f"   Conflict Type: {conflict['conflict_type']}")
        print(f"   Time: {conflict['resolved_at']}")

        # Show the conflicting data
        try:
            source_data = json.loads(conflict['source_data']) if conflict['source_data'] else {}
            target_data = json.loads(conflict['target_data']) if conflict['target_data'] else {}

            print(f"   Source Data: {source_data}")
            print(f"   Target Data: {target_data}")
        except:
            pass


def clear_old_conflicts(days_old=30):
    """Clear conflict logs older than specified days"""
    config = load_config()

    print(f"\n🧹 CLEARING CONFLICTS OLDER THAN {days_old} DAYS")
    print("=" * 60)

    total_cleared = 0

    for pair in config["sync_pairs"]:
        for db_type in ["local", "cloud"]:
            try:
                conn = connect_mysql(pair[db_type])

                with conn.cursor() as cur:
                    cur.execute("SHOW TABLES LIKE 'conflict_log'")
                    if not cur.fetchone():
                        continue

                    cur.execute("""
                                DELETE
                                FROM conflict_log
                                WHERE resolved_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                                """, (days_old,))

                    cleared = cur.rowcount
                    if cleared > 0:
                        print(f"  🗑️ Cleared {cleared} old conflicts from {pair['name']}.{db_type}")
                        total_cleared += cleared

                conn.close()

            except Exception as e:
                print(f"❌ Error clearing conflicts from {pair['name']}.{db_type}: {e}")

    print(f"\n✅ Total conflicts cleared: {total_cleared}")


if __name__ == "__main__":
    print("🛡️ CONFLICT RESOLUTION MONITOR")
    print("=" * 60)
    print("1. Show conflict summary")
    print("2. Show recent conflicts")
    print("3. Show manual resolution queue")
    print("4. Clear old conflicts")
    print("5. All reports")

    choice = input("\nEnter choice (1-5): ").strip()

    if choice == '1':
        show_conflict_summary()
    elif choice == '2':
        limit = input("Number of recent conflicts to show (default 10): ").strip()
        limit = int(limit) if limit.isdigit() else 10
        show_recent_conflicts(limit)
    elif choice == '3':
        show_manual_resolution_queue()
    elif choice == '4':
        days = input("Clear conflicts older than how many days? (default 30): ").strip()
        days = int(days) if days.isdigit() else 30
        clear_old_conflicts(days)
    elif choice == '5':
        show_conflict_summary()
        show_recent_conflicts(10)
        show_manual_resolution_queue()
    else:
        print("Invalid choice")

    print("\n" + "=" * 60)
    print("✅ Monitor complete!")