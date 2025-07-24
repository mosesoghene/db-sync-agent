# Sync Agent: Process Flow Overview

This document explains how the sync system initializes, tracks changes, and synchronizes data between local and cloud MySQL databases.

---

## 🛠️ 1. Initialization per Sync Pair

For each `sync_pair` in the config:

### On both `local` and `cloud` databases:

- Connect to the database using credentials.
- Ensure the `change_log` table exists:
  - If missing, create it.
  - If present but missing the `applied_nodes` column, add it.
- Check the `tables` configuration:
  - If set to `"all"` → sync all tables in the database, excluding `change_log`.
  - If a list → only sync the specified tables.

### For each tracked table:

- Identify its primary key column.
- If no primary key exists, the table is skipped.
- Automatically generate the following triggers:
  - `AFTER INSERT`
  - `AFTER UPDATE`
  - `AFTER DELETE`

Each trigger inserts a corresponding row into the `change_log` table when a change occurs.

---

## 🔄 2. Sync Execution

Sync jobs are run either:
- Immediately on startup
- Periodically via a background scheduler (e.g. every 10 minutes)

### For each direction (`local → cloud` and `cloud → local`):

- Fetch up to N changes (default: 100) from the `change_log` table:
  - Only changes where `applied_nodes` does **not** contain this node’s ID
  - Skip changes where `source_node` is this node (to avoid syncing its own changes)
- Apply each change to the target DB:
  - `INSERT` / `UPDATE`: Upsert the row using `ON DUPLICATE KEY UPDATE`
  - `DELETE`: Delete the row using the primary key
- After applying, update the original `change_log` row:
  - Append this node’s ID to the `applied_nodes` field to mark it as processed

---

## ✅ Key Behaviors

| Feature | Purpose |
|--------|---------|
| `change_log` | Central audit log for all changes made to tracked tables |
| Triggers | Ensure every insert, update, or delete is recorded automatically |
| `applied_nodes` | Prevents the same change from being applied multiple times to the same node |
| `source_node` | Avoids syncing changes back to the originator node |
| Bi-directional sync | All changes from local are pushed to cloud, and vice versa |
| Primary key dependency | Only tables with a defined primary key are eligible for sync |
