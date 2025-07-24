import unittest
from unittest.mock import MagicMock

from core.sync_engine import fetch_unapplied_changes, mark_change_as_applied

class TestSyncEngine(unittest.TestCase):

    def test_fetch_unapplied_changes_returns_results(self):
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

        mock_cursor.fetchall.return_value = [
            {
                "id": 1,
                "table_name": "users",
                "operation": "INSERT",
                "row_pk": "5",
                "row_data": '{"id": 5, "name": "Alice"}',
                "source_node": "edge-02",
            }
        ]

        changes = fetch_unapplied_changes(mock_conn, "edge-01", table_name="users", limit=10)

        mock_cursor.execute.assert_called_once()
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]["id"], 1)

    def test_mark_change_as_applied_executes_update(self):
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

        mark_change_as_applied(mock_conn, 42, "edge-01")

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("UPDATE change_log", sql)
        self.assertIn("JSON_ARRAY_APPEND", sql)
