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
    applied_nodes JSON DEFAULT JSON_ARRAY()
);
"""

def ensure_change_log_table(conn, db_name, tables):
    with conn.cursor() as cur:
        # Create the change_log table if it doesn't exist
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{db_name}`.`change_log` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255),
                operation ENUM('INSERT', 'UPDATE', 'DELETE'),
                row_id VARCHAR(255),
                change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                applied_nodes TEXT DEFAULT NULL
            );
        """)

        # Add missing columns if needed (backward-compatible upgrade)
        cur.execute(f"SHOW COLUMNS FROM `{db_name}`.`change_log` LIKE 'applied_nodes';")
        if not cur.fetchone():
            cur.execute(f"""
                ALTER TABLE `{db_name}`.`change_log`
                ADD COLUMN applied_nodes TEXT DEFAULT NULL;
            """)

        cur.execute(f"SHOW COLUMNS FROM `{db_name}`.`change_log` LIKE 'change_time';")
        if not cur.fetchone():
            cur.execute(f"""
                ALTER TABLE `{db_name}`.`change_log`
                ADD COLUMN change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
            """)

        for table in tables:
            if not table or not isinstance(table, str):
                print(f"⚠️ Skipping invalid table entry: {table}")
                continue

            try:
                # Check for primary key
                cur.execute(f"SHOW KEYS FROM `{db_name}`.`{table}` WHERE Key_name = 'PRIMARY'")
                pk_result = cur.fetchone()
                if not pk_result:
                    print(f"⚠️ Skipping '{table}': no primary key found")
                    continue

                pk_column = pk_result['Column_name']

                for op in ['INSERT', 'UPDATE', 'DELETE']:
                    trigger_name = f"trg_{table}_{op.lower()}_log"
                    cur.execute(f"DROP TRIGGER IF EXISTS `{trigger_name}`")

                    if op == 'INSERT':
                        cur.execute(f"""
                            CREATE TRIGGER `{trigger_name}`
                            AFTER INSERT ON `{db_name}`.`{table}`
                            FOR EACH ROW
                            INSERT INTO `{db_name}`.`change_log` (table_name, operation, row_id)
                            VALUES (%s, 'INSERT', NEW.`{pk_column}`);
                        """, (table,))
                    elif op == 'UPDATE':
                        cur.execute(f"""
                            CREATE TRIGGER `{trigger_name}`
                            AFTER UPDATE ON `{db_name}`.`{table}`
                            FOR EACH ROW
                            INSERT INTO `{db_name}`.`change_log` (table_name, operation, row_id)
                            VALUES (%s, 'UPDATE', NEW.`{pk_column}`);
                        """, (table,))
                    elif op == 'DELETE':
                        cur.execute(f"""
                            CREATE TRIGGER `{trigger_name}`
                            AFTER DELETE ON `{db_name}`.`{table}`
                            FOR EACH ROW
                            INSERT INTO `{db_name}`.`change_log` (table_name, operation, row_id)
                            VALUES (%s, 'DELETE', OLD.`{pk_column}`);
                        """, (table,))
            except Exception as e:
                print(f"❌ Error creating triggers for '{table}': {e}")

        conn.commit()



def get_table_list(conn: Connection, db_name: str, tables_spec):
    with conn.cursor() as cur:
        if tables_spec == "all":
            cur.execute("SHOW TABLES")
            result = cur.fetchall()
            return [list(row.values())[0] for row in result if list(row.values())[0] != "change_log"]
        elif isinstance(tables_spec, list):
            return [t for t in tables_spec if t != "change_log"]
        else:
            raise ValueError("Invalid `tables` field. Use 'all' or list of table names.")


def get_primary_key_column(conn: Connection, db_name: str, table_name: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_KEY='PRI'
        """, (db_name, table_name))
        row = cur.fetchone()
        if row:
            return row['COLUMN_NAME']
        else:
            return None


def generate_trigger_sql(table_name, pk_col, op, node_id):
    trig_name = f"trg_{table_name}_{op.lower()}"

    if op in ("INSERT", "UPDATE"):
        row_expr = ", ".join([f"'{col}', NEW.`{col}`" for col in ["*"]])  # we'll replace "*" dynamically
        row_data_expr = f"JSON_OBJECT({row_expr})"
        pk_expr = f"NEW.`{pk_col}`"
    else:  # DELETE
        row_expr = ", ".join([f"'{col}', OLD.`{col}`" for col in ["*"]])
        row_data_expr = f"JSON_OBJECT({row_expr})"
        pk_expr = f"OLD.`{pk_col}`"

    # Note: we'll dynamically replace '*' with actual column names later
    sql = f"""
    DROP TRIGGER IF EXISTS `{trig_name}`;
    CREATE TRIGGER `{trig_name}` AFTER {op} ON `{table_name}`
    FOR EACH ROW
    INSERT INTO change_log (table_name, operation, row_pk, row_data, source_node)
    VALUES (
        '{table_name}',
        '{op}',
        {pk_expr},
        {row_data_expr},
        '{node_id}'
    );
    """
    return sql


def get_table_columns(conn: Connection, db_name: str, table_name: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """, (db_name, table_name))
        return [row["COLUMN_NAME"] for row in cur.fetchall()]


def setup_triggers(conn: Connection, db_name: str, tables, node_id):
    """Create change_log triggers on specified tables."""
    table_list = get_table_list(conn, db_name, tables)

    for table in table_list:
        pk = get_primary_key_column(conn, db_name, table)
        if not pk:
            print(f"⚠️ Skipping table `{table}` (no primary key)")
            continue

        columns = get_table_columns(conn, db_name, table)
        if not columns:
            continue

        row_expr = ", ".join([f"'{col}', NEW.`{col}`" for col in columns])
        row_expr_delete = ", ".join([f"'{col}', OLD.`{col}`" for col in columns])

        for op in ["INSERT", "UPDATE", "DELETE"]:
            trig_name = f"trg_{table}_{op.lower()}"
            row_data_expr = f"JSON_OBJECT({row_expr})" if op != "DELETE" else f"JSON_OBJECT({row_expr_delete})"
            pk_expr = f"NEW.`{pk}`" if op != "DELETE" else f"OLD.`{pk}`"

            sql = f"""
            DROP TRIGGER IF EXISTS `{trig_name}`;
            CREATE TRIGGER `{trig_name}` AFTER {op} ON `{table}`
            FOR EACH ROW
            INSERT INTO change_log (table_name, operation, row_pk, row_data, source_node)
            VALUES (
                '{table}',
                '{op}',
                {pk_expr},
                {row_data_expr},
                '{node_id}'
            );
            """
            with conn.cursor() as cur:
                cur.execute(sql)

        print(f"✅ Triggers created for `{table}`")

