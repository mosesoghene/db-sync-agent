# Offline-Cloud MySQL Sync Agent

A Python-based synchronization system that syncs MySQL database tables between local and cloud environments — even when offline. Tracks data changes with triggers, resolves conflicts using a node ID and sync log, and supports many-to-many DB mapping.

## 🔧 Features

- ✅ Multiple local/cloud DBs per sync pair
- 🔁 Bi-directional sync (local ↔ cloud)
- ⏱️ Periodic auto-sync with scheduling
- 🧠 Intelligent change tracking using triggers
- 💡 Designed for GUI extension (PySide/Qt)
- ⚙️ Configurable via a single `config.json`

## 🚀 Getting Started

```bash
uv venv
uv pip install -r requirements.txt
python main.py
```

Make sure to edit `config.json` to define your sync pairs.

## 📂 Project Structure

```
core/
  ├── config.py
  ├── connector.py
  ├── schema.py
  └── sync_engine.py
scheduler/
  └── jobs.py
main.py
docs/
  ├── sync_process_flow.md
  ├── simulation_guide.md
  └── config_reference.md
```

## 📚 Documentation

- [📄 Sync Process Flow](docs/sync_process_flow.md)
- [🧪 Real Sync Simulation Guide](docs/simulation_guide.md)
- [⚙️ Configuration Format](docs/config_reference.md)
