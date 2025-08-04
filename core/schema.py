from pymysql.connections import Connection
from pymysql.cursors import DictCursor

CHANGE_LOG_TABLE = "change_log"

CREATE_CHANGE_LOG_SQL = f"""
CREATE TABLE IF NOT EXISTS `{CHANGE_LOG_TABLE}` (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation ENUM('INSERT', 'UPDATE', 'DELETE') NOT NULL,
    row_pk VARCHAR(255) NOT NULL,
    row_data JSON NULL,
    source_node VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    applied_nodes JSON DEFAULT (JSON_ARRAY()),
    INDEX idx_applied_source (source_node, created_at),
    INDEX idx_table_created (table_name, created_at)
);
"""


def ensure_change_log_table(conn: Connection):
    """Create change_log table and ensure required fields exist."""
    with conn.cursor() as cur:
        # Create table if it doesn't exist
        cur.execute(CREATE_CHANGE_LOG_SQL)
        print("    ✅ change_log table ensured")

        # Check if `applied_nodes` column exists (for legacy compatibility)
        cur.execute("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'change_log'
                      AND COLUMN_NAME = 'applied_nodes'
                    """)

        if not cur.fetchone():
            print("    🔧 Adding missing `applied_nodes` column...")
            cur.execute("""
                        ALTER TABLE change_log
                            ADD COLUMN applied_nodes JSON DEFAULT (JSON_ARRAY())
                        """)
            print("    ✅ applied_nodes column added")


def get_table_list(conn: Connection, db_name: str, tables_spec):
    """Get list of tables to sync, excluding change_log."""
    with conn.cursor() as cur:
        if tables_spec == "all":
            cur.execute("SHOW TABLES")
            result = cur.fetchall()
            # Extract table names from result and exclude change_log
            all_tables = [list(row.values())[0] for row in result]
            return [t for t in all_tables if t != "change_log"]
        elif isinstance(tables_spec, list):
            return [t for t in tables_spec if t != "change_log"]
        else:
            raise ValueError("Invalid `tables` field. Use 'all' or list of table names.")


def get_primary_key_column(conn: Connection, db_name: str, table_name: str):
    """Get the primary key column name for a table."""
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_NAME = %s
                      AND COLUMN_KEY = 'PRI'
                    ORDER BY ORDINAL_POSITION LIMIT 1
                    """, (db_name, table_name))

        row = cur.fetchone()
        return row['COLUMN_NAME'] if row else None


def get_table_columns(conn: Connection, db_name: str, table_name: str):
    """Get all column names for a table."""
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """, (db_name, table_name))
        return [row["COLUMN_NAME"] for row in cur.fetchall()]


def setup_triggers(conn: Connection, db_name: str, tables, node_id):
    """Create change_log triggers on specified tables."""
    table_list = get_table_list(conn, db_name, tables)

    if not table_list:
        print(f"    ⚠️ No tables found to setup triggers for")
        return

    print(f"    📋 Setting up triggers for {len(table_list)} tables...")

    for table in table_list:
        pk = get_primary_key_column(conn, db_name, table)
        if not pk:
            print(f"    ⚠️ Skipping table `{table}` (no primary key)")
            continue

        columns = get_table_columns(conn, db_name, table)
        if not columns:
            print(f"    ⚠️ Skipping table `{table}` (no columns found)")
            continue

        # Build JSON_OBJECT expressions for row data
        new_row_pairs = []
        old_row_pairs = []

        for col in columns:
            new_row_pairs.append(f"'{col}', NEW.`{col}`")
            old_row_pairs.append(f"'{col}', OLD.`{col}`")

        new_row_expr = ", ".join(new_row_pairs)
        old_row_expr = ", ".join(old_row_pairs)

        # Create triggers for INSERT, UPDATE, DELETE
        triggers = [
            ("INSERT", f"NEW.`{pk}`", f"JSON_OBJECT({new_row_expr})"),
            ("UPDATE", f"NEW.`{pk}`", f"JSON_OBJECT({new_row_expr})"),
            ("DELETE", f"OLD.`{pk}`", f"JSON_OBJECT({old_row_expr})")
        ]

        for op, pk_expr, row_data_expr in triggers:
            trig_name = f"trg_{table}_{op.lower()}"

            # Drop existing trigger
            drop_sql = f"DROP TRIGGER IF EXISTS `{trig_name}`"

            # Create new trigger
            create_sql = f"""
            CREATE TRIGGER `{trig_name}` 
            AFTER {op} ON `{table}`
            FOR EACH ROW
            INSERT INTO change_log (table_name, operation, row_pk, row_data, source_node)
            VALUES (
                '{table}',
                '{op}',
                {pk_expr},
                {row_data_expr},
                '{node_id}'
            )
            """

            with conn.cursor() as cur:
                cur.execute(drop_sql)
                cur.execute(create_sql)

        print(f"    ✅ Triggers created for `{table}` (PK: {pk})")

    print(f"    🎯 All triggers setup complete for {len(table_list)} tables")