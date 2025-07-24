# 🧪 Real Sync Simulation Guide

This guide walks you through manually verifying that sync between local and cloud databases is working correctly.

---

## ✅ 1. Create a Test Table

In both your **local** and **cloud** databases:

```sql
CREATE TABLE IF NOT EXISTS customers (
  id INT PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(100)
);
```

In your `config.json`, include:

```json
"tables": ["customers"]
```

---

## 🔧 2. Run the Sync Agent Once

```bash
python main.py
```

✅ This will:
- Create the `change_log` table (if missing)
- Generate triggers for `customers` (and other tables)
- Print confirmation messages

---

## ✍️ 3. Insert a Row in Local DB

```sql
INSERT INTO customers (id, name, email)
VALUES (101, 'Ada Lovelace', 'ada@analyticalengine.com');
```

Check the local DB's `change_log` table:

```sql
SELECT * FROM change_log;
```

Expected:
- 1 row
- operation = 'INSERT'
- source_node = your `node_id`
- applied_nodes = empty array

---

## 🔄 4. Run the Sync Agent Again

```bash
python main.py
```

Expected output:

```text
✅ Synced INSERT on customers [id=X]
```

The change will be applied to the cloud DB.

---

## ✅ 5. Verify in Cloud DB

```sql
SELECT * FROM customers WHERE id = 101;
```

You should see Ada’s row now in the cloud DB.

Also check that `applied_nodes` now includes your `node_id` in local’s `change_log`.

---

## 🧼 Notes

- `INSERT`, `UPDATE`, `DELETE` are all handled.
- Loops are avoided by checking `source_node`.
- Duplicate syncs are prevented using `applied_nodes`.
