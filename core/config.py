import json
import os
import uuid

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config.json")

def load_config():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    if not config.get("node_id"):
        config["node_id"] = uuid.uuid4().hex
        save_config(config)

    if "sync_pairs" not in config or not isinstance(config["sync_pairs"], list):
        raise ValueError("config.json must contain a list of sync_pairs")

    for pair in config["sync_pairs"]:
        if not all(k in pair for k in ("local", "cloud", "name")):
            raise ValueError("Each sync_pair must include 'name', 'local', and 'cloud'")
        if not all(k in pair["local"] for k in ("host", "user", "password", "db")):
            raise ValueError(f"Missing DB info in local config for sync pair: {pair.get('name')}")
        if not all(k in pair["cloud"] for k in ("host", "user", "password", "db")):
            raise ValueError(f"Missing DB info in cloud config for sync pair: {pair.get('name')}")

    return config

def save_config(config):
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
