import json
import os
import uuid

DEFAULT_CONFIG = {
    "node_id": str(uuid.uuid4()),
    "sync_pairs": [{
        "name": "default",
        "local": {
            "host": "localhost",
            "port": 3306,
            "user": "",
            "password": "",
            "db": ""
        },
        "cloud": {
            "host": "localhost",
            "port": 3306,
            "user": "",
            "password": "",
            "db": ""
        }
    }],
    "sync": {
        "enabled": True,
        "interval": 300,
        "tables": "all"  # Changed from {} to "all"
    },
    "advanced": {
        "batch_size": 1000,
        "retry_attempts": 3,
        "log_level": "INFO"
    }
}

def get_config_path():
    """Get the path to config.json in the project root"""
    return os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config.json'))

def load_gui_config():
    """Load configuration from config.json in project root, create with defaults if missing"""
    config_path = get_config_path()
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                try:
                    config = json.load(f)
                    # Ensure sync.tables is either "all" or a list of tables
                    if config.get('sync', {}).get('tables', {}) == {}:
                        config['sync']['tables'] = "all"
                    return config
                except json.JSONDecodeError:  # Invalid JSON
                    config = DEFAULT_CONFIG.copy()
                    save_gui_config(config)
                    return config
        else:  # File doesn't exist
            config = DEFAULT_CONFIG.copy()
            save_gui_config(config)
            return config
    except Exception as e:
        print(f"Error loading config from {config_path}: {e}")
        return DEFAULT_CONFIG.copy()

def save_gui_config(config):
    """Save configuration to config.json in project root"""
    config_path = get_config_path()
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
    except Exception as e:
        print(f"Error saving config to {config_path}: {e}")

def convert_config_for_core(gui_config):
    """Convert GUI config format to core config format"""
    return gui_config  # No conversion needed since we're using the same format
