from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QFormLayout,
    QLineEdit, QSpinBox, QPushButton, QCheckBox,
    QLabel, QMessageBox, QHBoxLayout
)
from PySide6.QtCore import Qt, Signal
from core.connector import connect_mysql
from core.schema import ensure_change_log_table
import logging


class SyncPairForm(QWidget):
    remove_requested = Signal(QWidget)  # Emits self

    def __init__(self, name="New Pair", existing_data=None):
        super().__init__()
        self.logger = logging.getLogger("sync_gui")
        self.name = name
        self.tables = []
        self.table_checkboxes = {}
        self.selected_tables = []

        # Layout setup
        main_layout = QVBoxLayout(self)
        self.group = QGroupBox(name)
        form_layout = QFormLayout(self.group)

        # Sync pair name
        self.name_field = QLineEdit(existing_data.get("name", "") if existing_data else name)
        form_layout.addRow("Sync Pair Name:", self.name_field)

        # Local DB fields
        self.local_box = QGroupBox("Local DB")
        self.local = self._db_fields(existing_data.get("local") if existing_data else {})
        local_layout = QFormLayout(self.local_box)
        for k, widget in self.local.items():
            local_layout.addRow(f"{k.capitalize()}:", widget)
        form_layout.addRow(self.local_box)

        # Cloud DB fields
        self.cloud_box = QGroupBox("Cloud DB")
        self.cloud = self._db_fields(existing_data.get("cloud") if existing_data else {})
        cloud_layout = QFormLayout(self.cloud_box)
        for k, widget in self.cloud.items():
            cloud_layout.addRow(f"{k.capitalize()}:", widget)
        form_layout.addRow(self.cloud_box)

        # Load / Remove buttons
        btn_row = QHBoxLayout()
        self.load_btn = QPushButton("Load Tables")
        self.load_btn.clicked.connect(self.load_tables)
        btn_row.addWidget(self.load_btn)

        self.delete_btn = QPushButton("🗑 Remove")
        self.delete_btn.clicked.connect(lambda: self.remove_requested.emit(self))
        btn_row.addWidget(self.delete_btn)

        form_layout.addRow(btn_row)

        # Select All checkbox
        self.select_all_cb = QCheckBox("Select All Tables")
        self.select_all_cb.stateChanged.connect(self.toggle_all_tables)
        self.select_all_cb.setEnabled(False)
        form_layout.addRow(self.select_all_cb)

        # Placeholder label
        self.tables_label = QLabel("Tables will be shown here after loading.")
        self.tables_label.setWordWrap(True)
        form_layout.addRow(self.tables_label)

        main_layout.addWidget(self.group)

        # Restore table selection state
        if existing_data and existing_data.get("tables") == "all":
            self.select_all_cb.setCheckState(Qt.Checked)
        elif existing_data and isinstance(existing_data.get("tables"), list):
            self.selected_tables = existing_data["tables"]

    def _db_fields(self, data):
        fields = {
            "host": QLineEdit(data.get("host", "localhost")),
            "port": QSpinBox(),
            "user": QLineEdit(data.get("user", "")),
            "password": QLineEdit(data.get("password", "")),
            "db": QLineEdit(data.get("db", ""))
        }
        fields["port"].setMaximum(65535)
        fields["port"].setValue(data.get("port", 3306))
        fields["password"].setEchoMode(QLineEdit.Password)
        return fields

    def get_db_config(self, side):
        fields = self.local if side == "local" else self.cloud
        return {
            "host": fields["host"].text(),
            "port": fields["port"].value(),
            "user": fields["user"].text(),
            "password": fields["password"].text(),
            "db": fields["db"].text()
        }

    def load_tables(self):
        try:
            db_cfg = self.get_db_config("local")
            conn = connect_mysql(db_cfg)
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                result = cur.fetchall()
                self.tables = sorted(
                    [list(row.values())[0] for row in result if list(row.values())[0] != "change_log"]
                )

            if not self.tables:
                self.tables_label.setText("No tables found.")
                return

            ensure_change_log_table(conn, db_cfg["db"], self.tables)
            conn.close()

            self.tables_label.setText("")
            self.table_checkboxes.clear()

            for t in self.tables:
                cb = QCheckBox(t)
                cb.setChecked(t in self.selected_tables)
                cb.stateChanged.connect(self.sync_select_all_state)
                self.table_checkboxes[t] = cb
                self.group.layout().addRow("", cb)

            self.select_all_cb.setEnabled(True)
            self.sync_select_all_state()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load tables:\n{e}")
            self.logger.exception(f"Error loading tables: {e}")

    def toggle_all_tables(self, state):
        checked = state == Qt.Checked
        for cb in self.table_checkboxes.values():
            cb.setChecked(checked)

    def sync_select_all_state(self):
        all_checked = all(cb.isChecked() for cb in self.table_checkboxes.values())
        any_checked = any(cb.isChecked() for cb in self.table_checkboxes.values())

        if all_checked:
            self.select_all_cb.setCheckState(Qt.Checked)
        elif any_checked:
            self.select_all_cb.setCheckState(Qt.PartiallyChecked)
        else:
            self.select_all_cb.setCheckState(Qt.Unchecked)

    def get_data(self):
        name = self.name_field.text().strip()
        local = self.get_db_config("local")
        cloud = self.get_db_config("cloud")

        if self.select_all_cb.checkState() == Qt.Checked:
            tables = "all"
        else:
            tables = [t for t, cb in self.table_checkboxes.items() if cb.isChecked()]

        return {
            "name": name,
            "local": local,
            "cloud": cloud,
            "tables": tables
        }
