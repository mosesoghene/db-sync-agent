# ⚙️ Configuration Reference

This document explains the structure and options in `config.json`.

---

## 🔑 Top-Level Fields

| Field                 | Description |
|----------------------|-------------|
| `node_id`            | Unique ID for this sync node. Auto-generated on first run if missing. |
| `sync_interval_minutes` | How often to sync in minutes (e.g., `10`) |
| `sync_pairs`         | List of database pairings to sync |

---

## 🔄 sync_pairs Format

```json
{
  "name": "inventory-db",
  "local": {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "password",
    "db": "local_inventory"
  },
  "cloud": {
    "host": "cloud-host",
    "port": 3306,
    "user": "admin",
    "password": "cloudpass",
    "db": "cloud_inventory"
  },
  "tables": "all"
}
```

### Fields

| Field     | Description |
|-----------|-------------|
| `name`    | A unique label for the sync pair |
| `local`   | Connection details for the local MySQL DB |
| `cloud`   | Connection details for the cloud MySQL DB |
| `tables`  | `"all"` or list of specific table names to sync |

---

## Example Full Config

```json
{
  "node_id": "auto-generated-or-fixed-id",
  "sync_interval_minutes": 10,
  "sync_pairs": [
    {
      "name": "customers-sync",
      "local": { ... },
      "cloud": { ... },
      "tables": ["customers", "orders"]
    },
    {
      "name": "sales-db",
      "local": { ... },
      "cloud": { ... },
      "tables": "all"
    }
  ]
}
```