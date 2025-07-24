import unittest
from unittest.mock import patch, mock_open
import json
import uuid

from core.config import load_config

class TestLoadConfig(unittest.TestCase):

    def setUp(self):
        self.base_config = {
            "sync_interval_minutes": 10,
            "sync_pairs": [
                {
                    "name": "test-pair",
                    "local": {
                        "host": "localhost",
                        "user": "user",
                        "password": "pass",
                        "db": "localdb"
                    },
                    "cloud": {
                        "host": "cloudhost",
                        "user": "clouduser",
                        "password": "cloudpass",
                        "db": "clouddb"
                    },
                    "tables": ["customers"]
                }
            ]
        }

    @patch("core.config.os.path.exists", return_value=True)
    @patch("core.config.open", new_callable=mock_open)
    @patch("core.config.uuid.uuid4", return_value=uuid.UUID("12345678-1234-5678-1234-567812345678"))
    @patch("core.config.save_config")  # mock save so it doesn't write
    def test_adds_node_id_if_missing(self, mock_save, mock_uuid, mock_open_fn, mock_exists):
        config_without_node_id = self.base_config.copy()
        mock_open_fn.return_value.read.return_value = json.dumps(config_without_node_id)

        config = load_config()

        self.assertIn("node_id", config)
        self.assertEqual(config["node_id"], "12345678123456781234567812345678")
        mock_save.assert_called_once()

    @patch("core.config.os.path.exists", return_value=True)
    @patch("core.config.open", new_callable=mock_open, read_data=json.dumps({
        "node_id": "abc-123",
        "sync_interval_minutes": 5,
        "sync_pairs": []
    }))
    def test_loads_existing_valid_config(self, mock_open_fn, mock_exists):
        config = load_config()
        self.assertEqual(config["node_id"], "abc-123")
        self.assertEqual(config["sync_interval_minutes"], 5)
        self.assertIsInstance(config["sync_pairs"], list)

    @patch("core.config.os.path.exists", return_value=True)
    @patch("core.config.open", new_callable=mock_open, read_data="{ invalid json }")
    def test_invalid_json_raises_exception(self, mock_open_fn, mock_exists):
        with self.assertRaises(json.JSONDecodeError):
            load_config()
