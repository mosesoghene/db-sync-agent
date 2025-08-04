"""
Sync Diagnostic Tool - Run this to debug what's happening with your sync
"""

import json
from core.config import load_config
from core.connector import connect_mysql


def diagnose_sync_issues():
    """Comprehensive sync diagnosis"""
    config = load_config()
    node_id = config["node_id"]

    print(f"🔍 SYNC DIAGNOSTIC REPORT")
    print(f"Node ID: {node_id}")
    print("=" * 60)

    for pair in config["sync_pairs"]:
        print(f"\n📋 Analyzing sync pair: {pair['name']}")

        try:
            local_conn = connect_mysql(pair["local"])
            cloud_conn = connect_mysql(pair["cloud"])

            # Check change_log in both databases
            for db_type, conn in [("LOCAL", local_conn), ("CLOUD", cloud_conn)]:
                print(f"\n--- {db_type} DATABASE ---")

                with conn.cursor() as cur:
                    # 1. Check if change_log table exists
                    cur.execute("SHOW TABLES LIKE 'change_log'")
                    if not cur.fetchone():
                        print("❌ change_log table doesn't exist!")
                        continue

                    # 2. Show all changes in change_log
                    cur.execute("""
                                SELECT id,
                                       table_name,
                                       operation,
                                       row_pk,
                                       source_node,
                                       applied_nodes,
                                       created_at
                                FROM change_log
                                ORDER BY created_at DESC LIMIT 10
                                """)
                    changes = cur.fetchall()

                    if not changes:
                        print("📭 No changes found in change_log")
                    else:
                        print(f"📊 Found {len(changes)} recent changes:")
                        for change in changes:
                            applied = change.get('applied_nodes', '[]')
                            print(f"  ID {change['id']}: {change['operation']} on {change['table_name']}")
                            print(f"    PK: {change['row_pk']}, Source: {change['source_node']}")
                            print(f"    Applied to: {applied}")
                            print(f"    Created: {change['created_at']}")

                    # 3. Test the unapplied changes query
                    print(f"\n🔍 Testing unapplied changes query for node: {node_id}")

                    # Try different query approaches
                    queries = [
                        # Original query
                        ("Original Query", """
                                           SELECT COUNT(*) as count
                                           FROM change_log
                                           WHERE NOT JSON_CONTAINS(applied_nodes
                                               , %s)
                                             AND source_node != %s
                                           """),
                        # Alternative query 1
                        ("Alternative 1", """
                                          SELECT COUNT(*) as count
                                          FROM change_log
                                          WHERE applied_nodes NOT LIKE %s
                                            AND source_node != %s
                                          """),
                        # Alternative query 2
                        ("Alternative 2", """
                                          SELECT COUNT(*) as count
                                          FROM change_log
                                          WHERE JSON_SEARCH(applied_nodes
                                              , 'one'
                                              , %s) IS NULL
                                            AND source_node != %s
                                          """)
                    ]

                    for name, query in queries:
                        try:
                            if "JSON_CONTAINS" in query:
                                cur.execute(query, (json.dumps(node_id), node_id))
                            elif "NOT LIKE" in query:
                                cur.execute(query, (f'%"{node_id}"%', node_id))
                            else:
                                cur.execute(query, (node_id, node_id))

                            result = cur.fetchone()
                            print(f"    {name}: {result['count']} unapplied changes")
                        except Exception as e:
                            print(f"    {name}: ERROR - {e}")

            local_conn.close()
            cloud_conn.close()

        except Exception as e:
            print(f"❌ Error analyzing {pair['name']}: {e}")
            import traceback
            traceback.print_exc()


def test_manual_sync():
    """Test syncing manually with detailed logging"""
    config = load_config()
    node_id = config["node_id"]

    print(f"\n🧪 MANUAL SYNC TEST")
    print("=" * 60)

    for pair in config["sync_pairs"]:
        print(f"\n📋 Manual sync test for: {pair['name']}")

        try:
            local_conn = connect_mysql(pair["local"])
            cloud_conn = connect_mysql(pair["cloud"])

            # Test local → cloud sync
            print(f"\n📤 Testing LOCAL → CLOUD sync")

            with local_conn.cursor() as cur:
                # Get all changes from local that haven't been applied to this node
                cur.execute("""
                            SELECT *
                            FROM change_log
                            WHERE source_node != %s
                            ORDER BY created_at ASC
                                LIMIT 5
                            """, (node_id,))

                changes = cur.fetchall()
                print(f"Found {len(changes)} changes to potentially sync")

                for change in changes:
                    print(f"\n  🔄 Processing change ID {change['id']}:")
                    print(f"    Table: {change['table_name']}")
                    print(f"    Operation: {change['operation']}")
                    print(f"    PK: {change['row_pk']}")
                    print(f"    Source: {change['source_node']}")
                    print(f"    Applied to: {change.get('applied_nodes', '[]')}")

                    # Check if already applied
                    applied_nodes = change.get('applied_nodes') or '[]'
                    try:
                        applied_list = json.loads(applied_nodes) if isinstance(applied_nodes, str) else applied_nodes
                        if node_id in applied_list:
                            print(f"    ✅ Already applied to this node")
                            continue
                    except:
                        print(f"    ⚠️ Could not parse applied_nodes: {applied_nodes}")

                    # Try to apply the change
                    try:
                        row_data = json.loads(change['row_data'] or '{}')
                        print(f"    📊 Row data: {list(row_data.keys()) if row_data else 'None'}")

                        # Simulate applying to cloud (just test the query)
                        if change['operation'] in ['INSERT', 'UPDATE'] and row_data:
                            cols = ", ".join(f"`{k}`" for k in row_data.keys())
                            print(f"    🔧 Would insert/update columns: {cols}")

                        # Mark as applied (test)
                        cur.execute("""
                                    UPDATE change_log
                                    SET applied_nodes = JSON_ARRAY_APPEND(applied_nodes, '$', %s)
                                    WHERE id = %s
                                    """, (node_id, change['id']))

                        print(f"    ✅ Marked as applied to node {node_id}")

                    except Exception as e:
                        print(f"    ❌ Error processing: {e}")

            local_conn.close()
            cloud_conn.close()

        except Exception as e:
            print(f"❌ Manual sync test failed: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    print("Choose diagnostic option:")
    print("1. Full diagnostic report")
    print("2. Manual sync test")
    print("3. Both")

    choice = input("Enter choice (1/2/3): ").strip()

    if choice in ['1', '3']:
        diagnose_sync_issues()

    if choice in ['2', '3']:
        test_manual_sync()

    print("\n" + "=" * 60)
    print("✅ Diagnostic complete!")