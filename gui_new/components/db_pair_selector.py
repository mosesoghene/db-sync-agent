from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QScrollArea,
                              QCheckBox, QPushButton, QComboBox, QLabel, QGroupBox)
from PySide6.QtCore import Signal
from core.connector import connect_mysql
import logging

class SyncDirection:
    NO_SYNC = "no_sync"
    LOCAL_TO_CLOUD = "local_to_cloud"
    CLOUD_TO_LOCAL = "cloud_to_local"
    BIDIRECTIONAL = "bidirectional"

class TableRow(QWidget):
    directionChanged = Signal(str, str)  # table_name, direction

    def __init__(self, table_name, parent=None):
        super().__init__(parent)
        self.table_name = table_name
        self.setup_ui()

    def setup_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Checkbox for table selection
        self.checkbox = QCheckBox(self.table_name)
        layout.addWidget(self.checkbox)

        # Combo box for sync direction
        self.direction_combo = QComboBox()
        self.direction_combo.addItems([
            "No Sync",
            "Bidirectional",
            "Local to Cloud",
            "Cloud to Local"
        ])
        self.direction_combo.setEnabled(False)
        layout.addWidget(self.direction_combo)

        # Connect checkbox state change to enable/disable direction combo
        self.checkbox.stateChanged.connect(self.on_checkbox_changed)
        self.direction_combo.currentTextChanged.connect(self.on_direction_changed)

    def on_checkbox_changed(self, state):
        self.direction_combo.setEnabled(state == 2)  # 2 is Checked
        if state == 2:
            self.direction_combo.setCurrentText("Bidirectional")
        else:
            self.direction_combo.setCurrentText("No Sync")

    def on_direction_changed(self, text):
        if self.checkbox.isChecked():
            direction = {
                "No Sync": SyncDirection.NO_SYNC,
                "Bidirectional": SyncDirection.BIDIRECTIONAL,
                "Local to Cloud": SyncDirection.LOCAL_TO_CLOUD,
                "Cloud to Local": SyncDirection.CLOUD_TO_LOCAL
            }[text]
            self.directionChanged.emit(self.table_name, direction)

    def get_sync_config(self):
        if not self.checkbox.isChecked() or self.direction_combo.currentText() == "No Sync":
            return None

        direction = {
            "Bidirectional": SyncDirection.BIDIRECTIONAL,
            "Local to Cloud": SyncDirection.LOCAL_TO_CLOUD,
            "Cloud to Local": SyncDirection.CLOUD_TO_LOCAL
        }[self.direction_combo.currentText()]

        return {
            "table": self.table_name,
            "direction": direction
        }

class DBPairSelector(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        from ..utils.detailed_logger import logger
        self.logger = logger
        self.table_rows = {}
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Create scroll area for tables
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(scroll_content)

        # Add header
        header = QHBoxLayout()
        header.addWidget(QLabel("Table Name"))
        header.addWidget(QLabel("Sync Direction"))
        self.scroll_layout.addLayout(header)

        # Add control buttons section
        controls_layout = QVBoxLayout()

        # Add "Select All" controls in horizontal layout
        select_all_group = QGroupBox("Bulk Selection Options")
        select_all_layout = QHBoxLayout()

        # Bidirectional select all
        self.select_all_bi = QPushButton("Select All\n(Bidirectional)")
        self.select_all_bi.clicked.connect(lambda: self.on_select_all("Bidirectional"))
        select_all_layout.addWidget(self.select_all_bi)

        # Local to Cloud select all
        self.select_all_ltc = QPushButton("Select All\n(Local to Cloud)")
        self.select_all_ltc.clicked.connect(lambda: self.on_select_all("Local to Cloud"))
        select_all_layout.addWidget(self.select_all_ltc)

        # Cloud to Local select all
        self.select_all_ctl = QPushButton("Select All\n(Cloud to Local)")
        self.select_all_ctl.clicked.connect(lambda: self.on_select_all("Cloud to Local"))
        select_all_layout.addWidget(self.select_all_ctl)

        # Add clear selection button
        self.clear_all = QPushButton("Clear All\nSelections")
        self.clear_all.clicked.connect(self.clear_all_selections)
        select_all_layout.addWidget(self.clear_all)

        select_all_group.setLayout(select_all_layout)
        controls_layout.addWidget(select_all_group)

        # Add action buttons in horizontal layout
        action_buttons = QHBoxLayout()

        # Add refresh button
        refresh_btn = QPushButton("Refresh Tables")
        refresh_btn.clicked.connect(self.refresh_tables)
        action_buttons.addWidget(refresh_btn)

        controls_layout.addLayout(action_buttons)
        self.scroll_layout.addLayout(controls_layout)

        scroll.setWidget(scroll_content)
        layout.addWidget(scroll)

    def clear_all_selections(self):
        """Clear all table selections"""
        for row in self.table_rows.values():
            row.checkbox.setChecked(False)
            row.direction_combo.setCurrentText("No Sync")

    def load_tables(self, config):
        try:
            # Get the first sync pair's local database config
            if not config.get('sync_pairs'):
                self.logger.log_sync_operation(
                    'Load Tables',
                    {'status': 'failed', 'error': 'No sync pairs configured'}
                )
                return

            source_config = config['sync_pairs'][0].get('local', {})
            if not source_config or not source_config.get('db'):
                self.logger.log_sync_operation(
                    'Load Tables',
                    {'status': 'failed', 'error': 'No database configured'}
                )
                return

            # Clear existing rows
            for row in self.table_rows.values():
                row.deleteLater()
            self.table_rows.clear()

            try:
                # Prepare connection config
                db_config = source_config.copy()
                db_config['database'] = db_config['db']

                # Log connection attempt
                self.logger.log_sync_operation(
                    'Database Connection',
                    {
                        'database': db_config['db'],
                        'status': 'attempting'
                    }
                )

                # Connect to source database
                conn = connect_mysql(db_config)
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                tables = []
                results = cursor.fetchall()

                # Extract table names from results
                field_name = f"Tables_in_{source_config['db']}"
                for row in results:
                    if isinstance(row, dict):
                        tables.append(row[field_name])
                    else:
                        tables.append(row[0])

                cursor.close()
                conn.close()

                # Log successful connection and table fetch
                self.logger.log_sync_operation(
                    'Load Tables',
                    {
                        'database': db_config['db'],
                        'table_count': len(tables),
                        'tables': tables,
                        'status': 'success'
                    }
                )

            except Exception as db_error:
                self.logger.log_sync_operation(
                    'Database Connection',
                    {
                        'database': db_config['db'],
                        'status': 'failed',
                        'error': str(db_error)
                    }
                )
                raise

            # Create rows for tables
            sync_config = config.get('sync', {}).get('tables', {})
            for table in sorted(tables):
                if table != 'change_log':  # Skip change_log table
                    row = TableRow(table)
                    self.table_rows[table] = row
                    self.scroll_layout.addWidget(row)

                    # Set initial state if table in config
                    if table in sync_config:
                        row.checkbox.setChecked(True)
                        table_config = sync_config[table]
                        direction = table_config.get('direction', SyncDirection.BIDIRECTIONAL)
                        direction_text = {
                            SyncDirection.BIDIRECTIONAL: "Bidirectional",
                            SyncDirection.LOCAL_TO_CLOUD: "Local to Cloud",
                            SyncDirection.CLOUD_TO_LOCAL: "Cloud to Local"
                        }[direction]
                        row.direction_combo.setCurrentText(direction_text)

                        # Log table sync configuration
                        self.logger.log_sync_operation(
                            'Table Configuration',
                            {
                                'table': table,
                                'direction': direction,
                                'status': 'configured',
                                'from_config': True
                            }
                        )

        except Exception as e:
            self.logger.log_sync_operation(
                'Load Tables',
                {
                    'status': 'failed',
                    'error': str(e)
                }
            )
            raise

    def on_select_all(self, direction):
        for row in self.table_rows.values():
            row.checkbox.setChecked(True)
            row.direction_combo.setCurrentText(direction)

    def get_sync_config(self):
        """Get the current sync configuration for all selected tables"""
        config = {}
        for table, row in self.table_rows.items():
            sync_config = row.get_sync_config()
            if sync_config:
                config[table] = sync_config
        return config

    def refresh_tables(self):
        # Find the MainWindow instance
        parent = self.parent()
        while parent and not hasattr(parent, 'get_current_config'):
            parent = parent.parent()

        if parent and hasattr(parent, 'get_current_config'):
            config = parent.get_current_config()
            self.load_tables(config)
        else:
            self.logger.error("Could not find MainWindow with configuration")
