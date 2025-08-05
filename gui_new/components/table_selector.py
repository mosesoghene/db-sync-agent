from PySide6.QtWidgets import (QWidget, QVBoxLayout, QScrollArea,
                               QCheckBox, QPushButton, QHBoxLayout, QComboBox, QLabel)
from core.connector import connect_mysql
import logging

class TableSelectorWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger(__name__)
        self.tables = []
        self.table_widgets = {}  # Store both checkbox and direction selector
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Create scroll area for tables
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(scroll_content)

        # Add header with select all
        header_layout = QHBoxLayout()
        self.select_all_cb = QCheckBox("Select All Tables")
        self.select_all_cb.clicked.connect(self.on_select_all)
        header_layout.addWidget(self.select_all_cb)

        # Add default direction selector
        header_layout.addStretch()
        header_layout.addWidget(QLabel("Default Direction:"))
        self.default_direction = QComboBox()
        self.default_direction.addItems(["bidirectional", "local_to_cloud", "cloud_to_local"])
        header_layout.addWidget(self.default_direction)

        self.scroll_layout.addLayout(header_layout)

        scroll.setWidget(scroll_content)
        layout.addWidget(scroll)

        # Add refresh button
        refresh_btn = QPushButton("Refresh Tables")
        refresh_btn.clicked.connect(self.refresh_tables)
        layout.addWidget(refresh_btn)

    def create_table_row(self, table_name, is_checked=False, direction="bidirectional"):
        """Create a row with checkbox and direction selector for a table"""
        row = QWidget()
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)

        # Checkbox for enabling/disabling the table
        checkbox = QCheckBox(table_name)
        checkbox.setChecked(is_checked)
        layout.addWidget(checkbox)

        # Direction selector
        direction_selector = QComboBox()
        direction_selector.addItems(["bidirectional", "local_to_cloud", "cloud_to_local"])
        direction_selector.setCurrentText(direction)
        direction_selector.setEnabled(is_checked)
        layout.addWidget(direction_selector)

        # Connect checkbox to enable/disable direction selector
        checkbox.toggled.connect(direction_selector.setEnabled)

        return row, checkbox, direction_selector

    def load_tables(self, config):
        """Load tables for a specific sync pair"""
        try:
            if not config.get('local'):  # Now expecting a sync pair config
                return

            # Clear existing table widgets
            for widgets in self.table_widgets.values():
                widgets['row'].deleteLater()
            self.table_widgets.clear()

            # Connect to local database
            conn = connect_mysql(config['local'])
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                self.tables = [list(row.values())[0] for row in cur.fetchall()]

            # Get tables configuration from the sync pair
            tables_config = config.get('tables_config', {})
            enabled_tables = {t['name']: t for t in tables_config.get('enabled_tables', [])}
            default_direction = tables_config.get('default_direction', 'bidirectional')

            # Set default direction in UI
            self.default_direction.setCurrentText(default_direction)

            # Create rows for tables
            for table in sorted(self.tables):
                if table != 'change_log':  # Skip change_log table
                    table_config = enabled_tables.get(table, {})
                    is_enabled = table in enabled_tables
                    direction = table_config.get('direction', default_direction)

                    row, checkbox, direction_selector = self.create_table_row(
                        table, is_enabled, direction
                    )

                    self.table_widgets[table] = {
                        'row': row,
                        'checkbox': checkbox,
                        'direction': direction_selector
                    }
                    self.scroll_layout.addWidget(row)

            # Update select all checkbox state
            self.update_select_all_state()

        except Exception as e:
            self.logger.error(f"Failed to load tables: {str(e)}")

    def get_table_config(self):
        """Get the current table configuration"""
        enabled_tables = []
        for table, widgets in self.table_widgets.items():
            if widgets['checkbox'].isChecked():
                enabled_tables.append({
                    'name': table,
                    'direction': widgets['direction'].currentText()
                })

        return {
            'enabled_tables': enabled_tables,
            'default_direction': self.default_direction.currentText()
        }

    def on_select_all(self, checked):
        for widgets in self.table_widgets.values():
            widgets['checkbox'].setChecked(checked)

    def update_select_all_state(self):
        if not self.table_widgets:
            self.select_all_cb.setChecked(False)
            return
        all_checked = all(w['checkbox'].isChecked() for w in self.table_widgets.values())
        self.select_all_cb.setChecked(all_checked)

    def refresh_tables(self):
        """Refresh tables for the current sync pair"""
        # Find the ConfigEditor instance
        parent = self.parent()
        while parent and not hasattr(parent, 'get_current_pair_config'):
            parent = parent.parent()

        if parent and hasattr(parent, 'get_current_pair_config'):
            config = parent.get_current_pair_config()
            self.load_tables(config)
        else:
            self.logger.error("Could not find ConfigEditor with pair configuration")
