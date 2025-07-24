import unittest
from unittest.mock import patch, MagicMock
from core.connector import connect_mysql

class TestConnectMySQL(unittest.TestCase):

    @patch("core.connector.pymysql.connect")
    def test_connect_mysql_success(self, mock_connect):
        # Arrange
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        db_config = {
            "host": "localhost",
            "user": "testuser",
            "password": "testpass",
            "db": "testdb"
        }

        # Act
        conn = connect_mysql(db_config)

        # Assert
        args, kwargs = mock_connect.call_args

        self.assertEqual(kwargs["host"], "localhost")
        self.assertEqual(kwargs["port"], 3306)
        self.assertEqual(kwargs["user"], "testuser")
        self.assertEqual(kwargs["password"], "testpass")
        self.assertEqual(kwargs["database"], "testdb")
        self.assertEqual(kwargs["autocommit"], True)
        self.assertIn("cursorclass", kwargs)
        self.assertEqual(conn, mock_conn)

    @patch("core.connector.pymysql.connect", side_effect=Exception("Connection failed"))
    def test_connect_mysql_failure(self, mock_connect):
        db_config = {
            "host": "localhost",
            "user": "baduser",
            "password": "wrongpass",
            "db": "missingdb"
        }

        with self.assertRaises(Exception) as context:
            connect_mysql(db_config)

        self.assertIn("Connection failed", str(context.exception))
