import os
import logging
from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QLabel, QMessageBox
from PySide6.QtGui import QIcon

from core.connector import connect_mysql
from core.schema import ensure_change_log_table
from gui.components.sync_controls import SyncControls
from gui.components.log_viewer import LogViewer
from gui.components.system_tray import SystemTrayIcon
from gui.components.table_selector import TableSelector

from core.runner import start_sync, stop_sync, run_one_time_sync
from core.config import load_config, PROJECT_ROOT
from gui.utils.logger import log_buffer

from PySide6.QtWidgets import QPushButton, QHBoxLayout
from gui.views.config_editor import ConfigEditor


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.config = load_config()
        self.setWindowTitle("DB Sync Agent")
        self.resize(800, 600)

        # Logger
        self.logger = logging.getLogger("sync_gui")

        # Icon
        icon_path = os.path.join(PROJECT_ROOT, "assets/icon.ico")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        # Central widget + layout
        central = QWidget()
        layout = QVBoxLayout(central)
        self.setCentralWidget(central)

        # Status label
        self.status_label = QLabel("Status: Idle")
        layout.addWidget(self.status_label)

        # Config button
        config_btn = QPushButton("Edit Config")
        config_btn.clicked.connect(self.open_config_editor)

        btn_row = QHBoxLayout()
        btn_row.addWidget(config_btn)
        layout.addLayout(btn_row)

        # Sync control buttons
        self.sync_controls = SyncControls(
            on_start=self.handle_start,
            on_stop=self.handle_stop,
            on_sync_now=self.handle_sync_now
        )
        layout.addWidget(self.sync_controls)

        # Table selector
        self.table_selector = TableSelector()
        layout.addWidget(self.table_selector)
        self.load_tables()

        # Log viewer
        self.log_viewer = LogViewer(log_buffer)
        layout.addWidget(self.log_viewer)

        # System tray
        self.tray = SystemTrayIcon(window=self, icon_path=icon_path)
        self.tray.setVisible(True)
        self.tray.show()

        # Logger
        self.logger = logging.getLogger("sync_gui")

    def load_tables(self):
        from core.connector import connect_mysql
        from core.schema import get_table_list

        try:
            config = load_config()
            all_tables_set = set()
            selected_tables = config.get("tables", [])

            for pair in config.get("sync_pairs", []):
                local_cfg = pair.get("local", {})
                if not all(k in local_cfg for k in ("host", "user", "password", "db")):
                    self.logger.warning(f"⚠️ Incomplete local config in pair: {pair.get('name')}")
                    continue

                try:
                    conn = connect_mysql(local_cfg)
                    tables = get_table_list(conn, local_cfg["db"], "all")
                    all_tables_set.update(tables)
                    conn.close()
                except Exception as e:
                    self.logger.warning(f"❌ Could not load tables for pair '{pair.get('name')}': {e}")

            all_tables = sorted(all_tables_set)
            config["all_tables"] = all_tables  # store for reuse if needed
            self.table_selector.set_tables(all_tables, selected=selected_tables)
            self.logger.info(f"📋 Loaded {len(all_tables)} tables into selector.")

        except Exception as e:
            self.logger.error(f"⚠️ Failed to load tables: {e}")

    def get_selected_tables(self):
        return self.table_selector.get_selected_tables()

    def handle_start(self):
        self.sync_controls.set_syncing_state()
        self.status_label.setText("Status: Running...")
        self.logger.info("🔄 Sync started (GUI trigger)")

        try:
            for pair in self.config.get("sync_pairs", []):
                local_cfg = pair["local"]
                cloud_cfg = pair["cloud"]
                tables = pair["tables"]

                local_conn = connect_mysql(local_cfg)
                cloud_conn = connect_mysql(cloud_cfg)

                if tables == "all":
                    # Fetch all tables except change_log
                    with local_conn.cursor() as cur:
                        cur.execute("SHOW TABLES")
                        tables = [list(row.values())[0] for row in cur.fetchall() if
                                  list(row.values())[0] != "change_log"]
                        self.logger.info(f"📋 Loaded {len(tables)} tables for pair '{pair['name']}' from local DB.")

                # Ensure change_log exists in both DBs before sync
                ensure_change_log_table(local_conn, local_cfg["db"], tables)
                ensure_change_log_table(cloud_conn, cloud_cfg["db"], tables)

                local_conn.close()
                cloud_conn.close()

            start_sync()

        except Exception as e:
            self.logger.exception("❌ Error while starting sync")
            QMessageBox.critical(self, "Sync Start Failed", str(e))

    def handle_stop(self):
        stop_sync()
        self.sync_controls.set_idle_state()
        self.status_label.setText("Status: Stopped")
        self.logger.info("⏹️ Sync stopped (GUI trigger)")

    def handle_sync_now(self):
        self.sync_controls.set_manual_syncing_state()
        self.status_label.setText("Status: Syncing...")
        self.logger.info("⚡ Manual sync started (GUI trigger)")

        try:
            run_one_time_sync(self.config, self.config["node_id"])
            self.logger.info("✅ Manual sync completed")
        except Exception as e:
            self.logger.error(f"❌ Manual sync failed: {e}")
        finally:
            self.sync_controls.set_idle_state()
            self.status_label.setText("Status: Idle")

    def closeEvent(self, event):
        event.ignore()
        self.hide()
        self.tray.showMessage(
            "DB Sync Agent",
            "App minimized to tray. Right-click icon for options.",
            self.tray.icon(),
            4000
        )

    def open_config_editor(self):
        editor = ConfigEditor(self)
        if editor.exec():
            self.logger.info("🛠 Config updated from GUI.")
            self.load_tables()  # Reload selector if tables were affected