"""
Debug script to identify and fix the trigger issue.
Run this to see what's wrong with your triggers and fix them.
"""

from core.config import load_config
from core.connector import connect_mysql

def debug_table_structure():
    """Debug the table structure and triggers to identify the issue."""
    config = load_config()

    for pair in config["sync_pairs"]:
        print(f"\n=== Debugging sync pair: {pair['name']} ===")

        for db_type in ["local", "cloud"]:
            print(f"\n--- {db_type.upper()} DATABASE ---")

            try:
                conn = connect_mysql(pair[db_type])
                db_name = pair[db_type]["db"]

                with conn.cursor() as cur:
                    # Show all tables
                    cur.execute("SHOW TABLES")
                    tables = [list(row.values())[0] for row in cur.fetchall()]
                    print(f"Tables: {tables}")

                    # For each table (except change_log), show structure and triggers
                    for table in tables:
                        if table == "change_log":
                            continue

                        print(f"\n📋 Table: {table}")

                        # Show table columns
                        cur.execute(f"DESCRIBE `{table}`")
                        columns = cur.fetchall()
                        print("Columns:")
                        for col in columns:
                            key_info = f" [{col['Key']}]" if col['Key'] else ""
                            print(f"  - {col['Field']}: {col['Type']}{key_info}")

                        # Show existing triggers
                        cur.execute(f"""
                            SELECT TRIGGER_NAME, EVENT_MANIPULATION, ACTION_STATEMENT
                            FROM INFORMATION_SCHEMA.TRIGGERS
                            WHERE TRIGGER_SCHEMA = %s AND EVENT_OBJECT_TABLE = %s
                        """, (db_name, table))

                        triggers = cur.fetchall()
                        if triggers:
                            print("Existing triggers:")
                            for trig in triggers:
                                print(f"  - {trig['TRIGGER_NAME']} ({trig['EVENT_MANIPULATION']})")
                                # Print first 200 chars of trigger code
                                action = trig['ACTION_STATEMENT'][:200] + "..." if len(trig['ACTION_STATEMENT']) > 200 else trig['ACTION_STATEMENT']
                                print(f"    Action: {action}")
                        else:
                            print("No triggers found")

                conn.close()

            except Exception as e:
                print(f"❌ Error debugging {db_type}: {e}")


def drop_all_triggers():
    """Drop all existing triggers to start fresh."""
    config = load_config()

    for pair in config["sync_pairs"]:
        print(f"\n🧹 Cleaning triggers for: {pair['name']}")

        for db_type in ["local", "cloud"]:
            try:
                conn = connect_mysql(pair[db_type])
                db_name = pair[db_type]["db"]

                with conn.cursor() as cur:
                    # Get all triggers
                    cur.execute("""
                        SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE
                        FROM INFORMATION_SCHEMA.TRIGGERS
                        WHERE TRIGGER_SCHEMA = %s
                        AND TRIGGER_NAME LIKE 'trg_%%'
                    """, (db_name,))

                    triggers = cur.fetchall()

                    if not triggers:
                        print(f"  📭 No triggers found in {db_type}")
                    else:
                        for trig in triggers:
                            drop_sql = f"DROP TRIGGER IF EXISTS `{trig['TRIGGER_NAME']}`"
                            cur.execute(drop_sql)
                            print(f"  ❌ Dropped: {trig['TRIGGER_NAME']} on {trig['EVENT_OBJECT_TABLE']}")
                        print(f"  ✅ Dropped {len(triggers)} triggers from {db_type}")

                conn.close()

            except Exception as e:
                print(f"  ❌ Error cleaning {db_type}: {e}")
                import traceback
                traceback.print_exc()


if __name__ == "__main__":
    print("🔍 DEBUGGING TABLE STRUCTURE AND TRIGGERS")
    print("=" * 60)

    # First, let's see what we're working with
    debug_table_structure()

    print("\n" + "=" * 60)
    choice = input("\nDo you want to drop all existing triggers and recreate them? (y/n): ")

    if choice.lower() == 'y':
        drop_all_triggers()

        print("\n🔧 Now run your main.py again to recreate triggers properly!")
        print("The triggers should now match your actual table structure.")
    else:
        print("\n💡 Manual fix needed:")
        print("1. Check the trigger code above for any references to 'row_id'")
        print("2. Drop problematic triggers manually")
        print("3. Run main.py to recreate them")