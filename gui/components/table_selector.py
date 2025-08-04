import logging

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QCheckBox, QGroupBox,
    QScrollArea, QFrame, QSizePolicy, QPushButton
)
from PySide6.QtCore import Qt
from core.config import load_config, save_config




class TableSelector(QWidget):
    def __init__(self, title="Select Tables"):
        super().__init__()

        self.tables = []
        self.checkboxes = {}

        self.logger = logging.getLogger("sync_gui")

        layout = QVBoxLayout(self)

        # Group Box
        group_box = QGroupBox(title)
        group_layout = QVBoxLayout(group_box)

        # Select All checkbox
        self.select_all_checkbox = QCheckBox("Select All")
        self.select_all_checkbox.stateChanged.connect(self.toggle_all)
        group_layout.addWidget(self.select_all_checkbox)

        # Scroll area with table checkboxes
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)

        scroll_widget = QFrame()
        self.checkboxes_layout = QVBoxLayout(scroll_widget)
        scroll_widget.setLayout(self.checkboxes_layout)
        self.scroll_area.setWidget(scroll_widget)

        group_layout.addWidget(self.scroll_area)

        # Save Button
        self.save_button = QPushButton("💾 Save Selection")
        self.save_button.clicked.connect(self.save_selection_to_config)
        group_layout.addWidget(self.save_button)

        layout.addWidget(group_box)

    def set_tables(self, tables, selected=None):
        self.tables = tables
        self.checkboxes.clear()

        selected = selected or []

        # Clear layout
        while self.checkboxes_layout.count():
            child = self.checkboxes_layout.takeAt(0)
            widget = child.widget()
            if widget:
                widget.deleteLater()

        # Add checkboxes
        for table in tables:
            cb = QCheckBox(table)
            cb.setChecked(table in selected)
            cb.stateChanged.connect(self.sync_select_all_state)
            self.checkboxes_layout.addWidget(cb)
            self.checkboxes[table] = cb

        self.sync_select_all_state()

    def get_selected_tables(self):
        return [table for table, cb in self.checkboxes.items() if cb.isChecked()]

    def set_selected_tables(self, tables_to_select):
        for table, cb in self.checkboxes.items():
            cb.setChecked(table in tables_to_select)

    def toggle_all(self, state):
        checked = state == Qt.Checked
        for cb in self.checkboxes.values():
            cb.setChecked(checked)

    def sync_select_all_state(self):
        all_checked = all(cb.isChecked() for cb in self.checkboxes.values())
        any_checked = any(cb.isChecked() for cb in self.checkboxes.values())

        if all_checked:
            self.select_all_checkbox.setCheckState(Qt.Checked)
        elif any_checked:
            self.select_all_checkbox.setCheckState(Qt.PartiallyChecked)
        else:
            self.select_all_checkbox.setCheckState(Qt.Unchecked)

    def save_selection_to_config(self):
        try:
            config = load_config()
            config["tables"] = self.get_selected_tables()
            save_config(config)
        except Exception as e:
            self.logger.exception(f"⚠️ Failed to save selected tables: {e}")
