from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QSpinBox,
    QLabel, QMessageBox, QWidget, QScrollArea
)

from core.config import load_config, save_config
from gui.components.sync_pair_form import SyncPairForm


class ConfigEditor(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Configuration")
        self.resize(700, 600)

        self.config = load_config()
        self.sync_pairs = self.config.get("sync_pairs", [])

        self.layout = QVBoxLayout(self)

        # Scroll area for sync pair forms
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)

        self.form_container = QWidget()
        self.form_layout = QVBoxLayout(self.form_container)
        self.scroll_area.setWidget(self.form_container)
        self.layout.addWidget(self.scroll_area)

        self.sync_pair_forms = []
        for pair in self.sync_pairs:
            self.add_sync_pair_form(pair)

        # Add new sync pair button
        add_btn = QPushButton("➕ Add Sync Pair")
        add_btn.clicked.connect(lambda: self.add_sync_pair_form())
        self.layout.addWidget(add_btn)

        # Sync interval
        self.sync_interval = QSpinBox()
        self.sync_interval.setMinimum(1)
        self.sync_interval.setValue(self.config.get("sync_interval_minutes", 10))

        self.layout.addWidget(QLabel("Sync Interval (minutes):"))
        self.layout.addWidget(self.sync_interval)

        # Save/cancel buttons
        btn_row = QHBoxLayout()
        save_btn = QPushButton("💾 Save")
        cancel_btn = QPushButton("Cancel")
        save_btn.clicked.connect(self.save)
        cancel_btn.clicked.connect(self.reject)

        btn_row.addWidget(save_btn)
        btn_row.addWidget(cancel_btn)
        self.layout.addLayout(btn_row)

    def add_sync_pair_form(self, existing_data=None):
        form = SyncPairForm(existing_data=existing_data or {})
        self.sync_pair_forms.append(form)
        self.form_layout.addWidget(form)

    def save(self):
        sync_pairs = []
        for form in self.sync_pair_forms:
            data = form.get_data()
            if not data["name"]:
                QMessageBox.warning(self, "Invalid", "Each sync pair must have a name.")
                return
            sync_pairs.append(data)

        self.config["sync_pairs"] = sync_pairs
        self.config["sync_interval_minutes"] = self.sync_interval.value()

        try:
            save_config(self.config)
            QMessageBox.information(self, "Success", "Configuration saved successfully.")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save configuration: {e}")

    def add_sync_pair_form(self, existing_data=None):
        form = SyncPairForm(existing_data=existing_data or {})
        form.remove_requested.connect(self.remove_sync_pair_form)
        self.sync_pair_forms.append(form)
        self.form_layout.addWidget(form)

    def remove_sync_pair_form(self, form_widget):
        self.form_layout.removeWidget(form_widget)
        form_widget.setParent(None)
        self.sync_pair_forms.remove(form_widget)
