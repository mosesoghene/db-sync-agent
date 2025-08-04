import json
import uuid
from datetime import datetime

from core.schema import get_primary_key_column, get_table_list


def generate_database_node_id(sync_pair_name, db_type):
    """Generate a unique node ID for each database in the sync pair"""
    base_string = f"{sync_pair_name}_{db_type}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, base_string).hex


def get_record_last_modified(conn, table_name, pk_col, pk_value):
    """Get the last modified timestamp for a record if it has updated_at column"""
    with conn.cursor() as cur:
        # Check if the table has an updated_at or modified_at column
        cur.execute("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = %s
                      AND COLUMN_NAME IN ('updated_at', 'modified_at', 'last_modified') LIMIT 1
                    """, (table_name,))

        timestamp_col = cur.fetchone()
        if not timestamp_col:
            return None

        timestamp_col_name = timestamp_col['COLUMN_NAME']

        # Get the record's timestamp
        cur.execute(f"""
            SELECT `{timestamp_col_name}` 
            FROM `{table_name}` 
            WHERE `{pk_col}` = %s
        """, (pk_value,))

        result = cur.fetchone()
        return result[timestamp_col_name] if result else None


def detect_conflict(source_change, target_conn, table_name, pk_col, pk_value):
    """Detect if applying a change would cause a conflict"""
    with target_conn.cursor() as cur:
        # Check if record exists in target
        cur.execute(f"SELECT * FROM `{table_name}` WHERE `{pk_col}` = %s", (pk_value,))
        target_record = cur.fetchone()

        if not target_record:
            return False, None  # No conflict if record doesn't exist

        # Check if target record was modified after the source change
        target_timestamp = get_record_last_modified(target_conn, table_name, pk_col, pk_value)
        source_timestamp = source_change.get('created_at')

        if target_timestamp and source_timestamp:
            if target_timestamp > source_timestamp:
                return True, {
                    'type': 'timestamp_conflict',
                    'source_time': source_timestamp,
                    'target_time': target_timestamp,
                    'target_record': target_record
                }

        # Check for field-level conflicts by comparing data
        source_data = json.loads(source_change.get('row_data', '{}'))

        conflicts = []
        for field, source_value in source_data.items():
            if field in target_record:
                target_value = target_record[field]
                if str(source_value) != str(target_value):
                    conflicts.append({
                        'field': field,
                        'source_value': source_value,
                        'target_value': target_value
                    })

        if conflicts:
            return True, {
                'type': 'field_conflict',
                'conflicts': conflicts,
                'target_record': target_record
            }

        return False, None


def log_conflict(conn, source_change, conflict_info, resolution):
    """Log a conflict to the conflict_log table"""
    with conn.cursor() as cur:
        # Create conflict_log table if it doesn't exist
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS conflict_log
                    (
                        id
                        BIGINT
                        AUTO_INCREMENT
                        PRIMARY
                        KEY,
                        change_id
                        BIGINT
                        NOT
                        NULL,
                        table_name
                        VARCHAR
                    (
                        255
                    ) NOT NULL,
                        record_pk VARCHAR
                    (
                        255
                    ) NOT NULL,
                        conflict_type VARCHAR
                    (
                        50
                    ) NOT NULL,
                        source_data JSON,
                        target_data JSON,
                        conflict_details JSON,
                        resolution VARCHAR
                    (
                        50
                    ) NOT NULL,
                        resolved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_table_pk
                    (
                        table_name,
                        record_pk
                    ),
                        INDEX idx_resolved_at
                    (
                        resolved_at
                    )
                        )
                    """)

        # Insert conflict log
        cur.execute("""
                    INSERT INTO conflict_log
                    (change_id, table_name, record_pk, conflict_type, source_data, target_data, conflict_details,
                     resolution)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        source_change['id'],
                        source_change['table_name'],
                        source_change['row_pk'],
                        conflict_info['type'],
                        source_change.get('row_data'),
                        json.dumps(conflict_info.get('target_record', {})),
                        json.dumps(conflict_info),
                        resolution
                    ))


def resolve_conflict(source_change, target_conn, conflict_info, resolution_strategy='timestamp_wins'):
    """
    Resolve a conflict based on the specified strategy

    Strategies:
    - 'timestamp_wins': Most recent change wins (default)
    - 'source_wins': Source always wins
    - 'target_wins': Target always wins
    - 'merge_fields': Attempt to merge non-conflicting fields
    - 'manual': Log for manual resolution (don't apply)
    """

    if resolution_strategy == 'manual':
        print(f"    📝 Conflict logged for manual resolution")
        log_conflict(target_conn, source_change, conflict_info, 'manual')
        return False

    elif resolution_strategy == 'target_wins':
        print(f"    🎯 Target wins - skipping source change")
        log_conflict(target_conn, source_change, conflict_info, 'target_wins')
        return False

    elif resolution_strategy == 'source_wins':
        print(f"    📤 Source wins - applying source change")
        log_conflict(target_conn, source_change, conflict_info, 'source_wins')
        return True

    elif resolution_strategy == 'timestamp_wins':
        if conflict_info['type'] == 'timestamp_conflict':
            if conflict_info['source_time'] > conflict_info['target_time']:
                print(f"    ⏰ Source is newer - applying change")
                log_conflict(target_conn, source_change, conflict_info, 'timestamp_wins_source')
                return True
            else:
                print(f"    ⏰ Target is newer - skipping change")
                log_conflict(target_conn, source_change, conflict_info, 'timestamp_wins_target')
                return False
        else:
            # Fall back to source wins for non-timestamp conflicts
            print(f"    📤 No timestamp info - source wins")
            log_conflict(target_conn, source_change, conflict_info, 'timestamp_wins_source')
            return True

    elif resolution_strategy == 'merge_fields':
        return resolve_merge_fields(source_change, target_conn, conflict_info)

    # Default: source wins
    log_conflict(target_conn, source_change, conflict_info, 'source_wins')
    return True


def resolve_merge_fields(source_change, target_conn, conflict_info):
    """Attempt to merge fields, only updating non-conflicting ones"""
    source_data = json.loads(source_change.get('row_data', '{}'))
    target_record = conflict_info['target_record']
    table_name = source_change['table_name']
    pk_col = None

    # Get primary key column
    with target_conn.cursor() as cur:
        pk_col = get_primary_key_column(target_conn, target_conn.db.decode(), table_name)
        if not pk_col:
            print(f"    ❌ No primary key found for merge")
            return False

    # Identify non-conflicting fields
    conflicting_fields = {c['field'] for c in conflict_info.get('conflicts', [])}
    safe_fields = {k: v for k, v in source_data.items()
                   if k not in conflicting_fields and k != pk_col}

    if not safe_fields:
        print(f"    ⚠️ No safe fields to merge - skipping")
        log_conflict(target_conn, source_change, conflict_info, 'merge_no_safe_fields')
        return False

    # Update only safe fields
    with target_conn.cursor() as cur:
        set_clause = ", ".join(f"`{k}` = %s" for k in safe_fields.keys())
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE `{pk_col}` = %s"
        values = list(safe_fields.values()) + [source_change['row_pk']]

        cur.execute(sql, values)
        print(f"    🔀 Merged {len(safe_fields)} non-conflicting fields")

    log_conflict(target_conn, source_change, conflict_info, 'merge_fields')
    return True


def apply_change_with_conflict_detection(target_conn, change, resolution_strategy='timestamp_wins'):
    """Apply a single change to the target database with conflict detection."""
    op = change["operation"]
    table = change["table_name"]
    pk_value = change["row_pk"]
    row_data = json.loads(change["row_data"] or "{}")

    print(f"    🔧 Applying {op} to {table} [pk={pk_value}]")

    if op == "DELETE":
        # Deletes are simpler - just delete if exists
        pk_col = get_primary_key_column(target_conn, target_conn.db.decode(), table)
        if not pk_col:
            print(f"    ⚠️ No primary key found for {table}, can't delete")
            return False

        with target_conn.cursor() as cur:
            sql = f"DELETE FROM `{table}` WHERE `{pk_col}` = %s"
            cur.execute(sql, (pk_value,))
            print(f"    ✅ DELETE applied successfully")
        return True

    elif op in ["INSERT", "UPDATE"]:
        if not row_data:
            print(f"    ⚠️ No row data for {op} on {table}, skipping")
            return False

        # Get primary key column
        pk_col = get_primary_key_column(target_conn, target_conn.db.decode(), table)
        if not pk_col:
            print(f"    ⚠️ No primary key found for {table}")
            return False

        # Detect conflicts
        has_conflict, conflict_info = detect_conflict(change, target_conn, table, pk_col, pk_value)

        if has_conflict:
            print(f"    ⚠️ CONFLICT DETECTED: {conflict_info['type']}")

            # Show conflict details
            if conflict_info['type'] == 'field_conflict':
                for conflict in conflict_info.get('conflicts', []):
                    print(
                        f"      🔥 {conflict['field']}: source={conflict['source_value']}, target={conflict['target_value']}")
            elif conflict_info['type'] == 'timestamp_conflict':
                print(f"      ⏰ Source: {conflict_info['source_time']}, Target: {conflict_info['target_time']}")

            # Resolve the conflict
            should_apply = resolve_conflict(change, target_conn, conflict_info, resolution_strategy)

            if not should_apply:
                return False

        # Apply the change
        with target_conn.cursor() as cur:
            cols = ", ".join(f"`{k}`" for k in row_data.keys())
            placeholders = ", ".join(["%s"] * len(row_data))
            updates = ", ".join(f"`{k}`=VALUES(`{k}`)" for k in row_data.keys())

            sql = f"""
            INSERT INTO `{table}` ({cols}) VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {updates}
            """

            cur.execute(sql, list(row_data.values()))
            action = "CONFLICT RESOLVED + APPLIED" if has_conflict else "APPLIED"
            print(f"    ✅ {op} {action} successfully")

        return True

    return False


def fetch_unapplied_changes(conn, target_node_id, table_name=None, limit=100):
    """Fetch changes that haven't been applied to the target node yet."""
    with conn.cursor() as cur:
        base_sql = """
                   SELECT * \
                   FROM change_log
                   WHERE (applied_nodes IS NULL OR JSON_SEARCH(applied_nodes, 'one', %s) IS NULL) \
                   """

        args = [target_node_id]

        if table_name:
            base_sql += " AND table_name = %s"
            args.append(table_name)

        base_sql += " ORDER BY created_at ASC LIMIT %s"
        args.append(limit)

        cur.execute(base_sql, args)
        results = cur.fetchall()

        print(f"    📋 Found {len(results)} unapplied changes")
        return results


def mark_change_as_applied(conn, change_id, target_node_id):
    """Mark a change as applied to the target node."""
    with conn.cursor() as cur:
        cur.execute("""
                    UPDATE change_log
                    SET applied_nodes = JSON_ARRAY_APPEND(
                            COALESCE(applied_nodes, JSON_ARRAY()), '$', %s
                                        )
                    WHERE id = %s
                    """, (target_node_id, change_id))

        return cur.rowcount > 0


def sync_changes_with_conflict_resolution(source_conn, target_conn, sync_pair_name, tables="all",
                                          resolution_strategy='timestamp_wins'):
    """
    Sync changes from source to target database with conflict resolution.

    Resolution strategies:
    - 'timestamp_wins': Most recent change wins (requires updated_at column)
    - 'source_wins': Source database always wins
    - 'target_wins': Target database always wins
    - 'merge_fields': Merge non-conflicting fields only
    - 'manual': Log conflicts for manual resolution
    """
    try:
        source_db = source_conn.db.decode()
        target_db = target_conn.db.decode()

        # Generate the node IDs for this sync pair
        local_node_id = generate_database_node_id(sync_pair_name, "local")
        cloud_node_id = generate_database_node_id(sync_pair_name, "cloud")

        # Determine sync direction
        if "local" in source_db.lower() or "127.0.0.1" in source_db:
            target_node_id = cloud_node_id
            direction = "LOCAL → CLOUD"
        else:
            target_node_id = local_node_id
            direction = "CLOUD → LOCAL"

        print(f"\n📊 {direction} (Strategy: {resolution_strategy})")
        print(f"🔄 {source_db} → {target_db}")

        # Get tables to sync
        if tables == "all":
            tables_to_sync = get_table_list(source_conn, source_db, tables)
        else:
            tables_to_sync = tables if isinstance(tables, list) else [tables]

        total_synced = 0
        total_conflicts = 0

        # Sync each table
        for table in tables_to_sync:
            print(f"\n  📋 Processing table: {table}")
            changes = fetch_unapplied_changes(source_conn, target_node_id, table)

            if not changes:
                print(f"  📭 No unapplied changes for table: {table}")
                continue

            table_conflicts = 0
            table_synced = 0

            for change in changes:
                try:
                    print(f"\n    🔄 Processing change ID {change['id']}")

                    # Apply change with conflict detection
                    if apply_change_with_conflict_detection(target_conn, change, resolution_strategy):
                        # Mark as applied
                        if mark_change_as_applied(source_conn, change["id"], target_node_id):
                            table_synced += 1
                            total_synced += 1

                except Exception as e:
                    print(f"    ❌ Error processing change {change['id']}: {e}")
                    import traceback
                    traceback.print_exc()

            print(f"  🎯 Table {table}: {table_synced} synced, {table_conflicts} conflicts")

        print(f"\n  📊 SYNC SUMMARY: {total_synced} changes synced, {total_conflicts} conflicts handled")

    except Exception as e:
        print(f"❌ Sync error: {e}")
        import traceback
        traceback.print_exc()