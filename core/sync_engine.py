import json

from core.schema import get_primary_key_column, get_table_list
import logging

logger = logging.getLogger("sync_gui")

def fetch_unapplied_changes(conn, node_id, table_name=None, limit=100):
    with conn.cursor() as cur:
        if table_name:
            sql = """
                SELECT * FROM change_log
                WHERE table_name = %s
                  AND FIND_IN_SET(%s, applied_nodes) = 0
                ORDER BY change_time ASC
                LIMIT %s
            """
            cur.execute(sql, (table_name, node_id, limit))
        else:
            sql = """
                SELECT * FROM change_log
                WHERE FIND_IN_SET(%s, applied_nodes) = 0
                ORDER BY change_time ASC
                LIMIT %s
            """
            cur.execute(sql, (node_id, limit))

        return cur.fetchall()


def apply_change(target_conn, change):
    op = change["operation"]
    table = change["table_name"]
    pk = change["row_pk"]
    row_data = json.loads(change["row_data"] or "{}")

    with target_conn.cursor() as cur:
        if op == "INSERT" or op == "UPDATE":
            cols = ", ".join(f"`{k}`" for k in row_data.keys())
            placeholders = ", ".join(["%s"] * len(row_data))
            updates = ", ".join(f"`{k}`=VALUES(`{k}`)" for k in row_data.keys())

            sql = f"""
            INSERT INTO `{table}` ({cols}) VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {updates}
            """
            cur.execute(sql, list(row_data.values()))

        elif op == "DELETE":
            pk_col = get_primary_key_column(target_conn, target_conn.db.decode(), table)
            sql = f"DELETE FROM `{table}` WHERE `{pk_col}` = %s"
            cur.execute(sql, (pk,))


def mark_change_as_applied(conn, change_id, node_id):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE change_log
            SET applied_nodes = JSON_ARRAY_APPEND(COALESCE(applied_nodes, JSON_ARRAY()), '$', %s)
            WHERE id = %s
        """, (node_id, change_id))

    conn.commit()


def sync_changes(source_conn, target_conn, node_id, tables="all"):
    tables_to_sync = get_table_list(source_conn, source_conn.db.decode(), tables)

    for table in tables_to_sync:
        changes = fetch_unapplied_changes(source_conn, node_id, table)

        for change in changes:
            if change["source_node"] == node_id:
                continue  # skip own changes

            try:
                apply_change(target_conn, change)
                mark_change_as_applied(source_conn, change["id"], node_id)
                logger.exception(f"✅ Synced {change['operation']} on {table} [id={change['id']}]")
            except Exception as e:
                logger.exception(f"❌ Failed to apply change {change['id']}: {e}")
