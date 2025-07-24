import unittest
from unittest.mock import MagicMock, call
from core.schema import ensure_change_log_table, setup_triggers

class TestSchema(unittest.TestCase):

    def test_ensure_change_log_table_adds_column_if_missing(self):
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

        # Simulate applied_nodes column NOT existing
        mock_cursor.fetchone.return_value = None

        ensure_change_log_table(mock_conn)

        # Validate expected SQLs were executed
        executed_sqls = [args[0] for args, _ in mock_cursor.execute.call_args_list]

        assert any("CREATE TABLE IF NOT EXISTS `change_log`" in sql for sql in executed_sqls)
        assert any("SELECT COLUMN_NAME" in sql for sql in executed_sqls)
        assert any("ALTER TABLE change_log" in sql for sql in executed_sqls)

    def test_setup_triggers_skips_tables_with_no_pk(self):
        # Setup mock connection
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

        # Patch get_table_list, get_primary_key_column, get_table_columns
        from core import schema
        schema.get_table_list = lambda conn, db, tables: ["products"]
        schema.get_primary_key_column = lambda conn, db, table: None  # Simulate missing PK
        schema.get_table_columns = lambda conn, db, table: ["id", "name"]

        setup_triggers(mock_conn, "test_db", ["products"], "node-123")

        # If no primary key, no trigger SQL should be executed
        mock_cursor.execute.assert_not_called()
