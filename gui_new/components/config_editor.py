from PySide6.QtWidgets import (QDialog, QVBoxLayout, QTabWidget,
                               QWidget, QFormLayout, QLineEdit, QSpinBox,
                               QDialogButtonBox, QTextEdit, QCheckBox, QPushButton,
                               QMessageBox, QHBoxLayout, QApplication, QGroupBox, QLabel)
from PySide6.QtCore import Signal
from ..utils.config_manager import load_gui_config, save_gui_config
from core.connector import connect_mysql
from .db_pairs_manager import DBPairsManager
import logging

class ConfigEditor(QDialog):
    configSaved = Signal(dict)  # Signal emitted when config is saved

    def __init__(self, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger(__name__)
        self.setWindowTitle("DB Sync Agent Settings")
        self.setMinimumSize(800, 600)  # Back to original height
        # Get screen size to ensure dialog fits
        screen = QApplication.primaryScreen().geometry()
        self.setMaximumHeight(int(screen.height() * 0.8))  # 80% of screen height
        self.setup_ui()
        self.load_config()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Create tab widget
        self.tab_widget = QTabWidget()

        # Database pairs tab (replaces old Database Settings tab)
        self.db_tab = QWidget()
        self.setup_db_tab()
        self.tab_widget.addTab(self.db_tab, "Database Pairs")

        # Sync settings tab
        self.sync_tab = QWidget()
        self.setup_sync_tab()
        self.tab_widget.addTab(self.sync_tab, "Sync Settings")

        # Advanced settings tab
        self.advanced_tab = QWidget()
        self.setup_advanced_tab()
        self.tab_widget.addTab(self.advanced_tab, "Advanced Settings")

        layout.addWidget(self.tab_widget)

        # Add OK/Cancel buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        self.button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok |
            QDialogButtonBox.StandardButton.Cancel
        )
        self.button_box.accepted.connect(self.save_config)
        self.button_box.rejected.connect(self.reject)
        button_layout.addWidget(self.button_box)

        layout.addLayout(button_layout)

    def setup_db_tab(self):
        layout = QVBoxLayout(self.db_tab)

        # Use the new DBPairsManager
        self.db_pairs_manager = DBPairsManager()
        layout.addWidget(self.db_pairs_manager)

    def setup_sync_tab(self):
        layout = QVBoxLayout(self.sync_tab)

        # Add sync interval settings
        form_layout = QFormLayout()
        self.sync_interval = QSpinBox()
        self.sync_interval.setMinimum(1)
        self.sync_interval.setMaximum(1440)
        self.sync_enabled = QCheckBox("Enable Scheduled Sync")
        form_layout.addRow("Sync Interval (minutes):", self.sync_interval)
        form_layout.addRow(self.sync_enabled)
        layout.addLayout(form_layout)

    def setup_advanced_tab(self):
        layout = QVBoxLayout(self.advanced_tab)

        # Sync settings group
        settings_group = QGroupBox("Sync Settings")
        form_layout = QFormLayout()

        self.batch_size = QSpinBox()
        self.batch_size.setMinimum(100)
        self.batch_size.setMaximum(10000)
        self.retry_attempts = QSpinBox()
        self.retry_attempts.setMaximum(10)
        self.log_level = QLineEdit()

        form_layout.addRow("Batch Size:", self.batch_size)
        form_layout.addRow("Retry Attempts:", self.retry_attempts)
        form_layout.addRow("Log Level:", self.log_level)
        settings_group.setLayout(form_layout)
        layout.addWidget(settings_group)

        # Log Management group
        log_group = QGroupBox("Log Management")
        log_layout = QVBoxLayout()

        # Export button with info label
        export_layout = QHBoxLayout()
        export_btn = QPushButton("Export Detailed Logs")
        export_btn.clicked.connect(self.export_logs)
        export_layout.addWidget(export_btn)
        export_info = QLabel("Export logs to Documents/DB Sync Agent/exports")
        export_layout.addWidget(export_info)
        export_layout.addStretch()
        log_layout.addLayout(export_layout)

        # Log backup settings
        backup_layout = QHBoxLayout()
        self.auto_backup = QCheckBox("Auto-backup logs when entries exceed:")
        self.backup_limit = QSpinBox()
        self.backup_limit.setMinimum(100)
        self.backup_limit.setMaximum(10000)
        self.backup_limit.setValue(500)
        backup_layout.addWidget(self.auto_backup)
        backup_layout.addWidget(self.backup_limit)
        backup_layout.addStretch()
        log_layout.addLayout(backup_layout)

        log_group.setLayout(log_layout)
        layout.addWidget(log_group)
        layout.addStretch()

    def load_config(self):
        try:
            config = load_gui_config()
            if not config:
                return

            # Load database pairs
            sync_pairs = config.get('sync_pairs', [])
            self.db_pairs_manager.set_all_configs(sync_pairs)

            # Load sync settings
            sync = config.get('sync', {})
            self.sync_interval.setValue(int(sync.get('interval', 300)))
            self.sync_enabled.setChecked(sync.get('enabled', True))

            # Load advanced settings
            advanced = config.get('advanced', {})
            self.batch_size.setValue(int(advanced.get('batch_size', 1000)))
            self.retry_attempts.setValue(int(advanced.get('retry_attempts', 3)))
            self.log_level.setText(advanced.get('log_level', 'INFO'))

        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")

    def save_config(self):
        try:
            # Load existing config to preserve node_id
            current_config = load_gui_config() or {}
            node_id = current_config.get('node_id', 'ca1a235588f64aeb858b302050586031')

            # Get configurations from all database pairs
            sync_pairs = self.db_pairs_manager.get_all_configs()

            # Create new config
            config = {
                'node_id': node_id,
                'sync_pairs': sync_pairs,
                'sync': {
                    'interval': self.sync_interval.value(),
                    'enabled': self.sync_enabled.isChecked()
                },
                'advanced': {
                    'batch_size': self.batch_size.value(),
                    'retry_attempts': self.retry_attempts.value(),
                    'log_level': self.log_level.text()
                }
            }

            save_gui_config(config)
            self.logger.info("Configuration saved successfully")
            self.configSaved.emit(config)
            self.accept()

        except Exception as e:
            self.logger.error(f"Failed to save configuration: {str(e)}")
            raise

    def export_logs(self):
        """Export detailed logs to Documents folder"""
        from ..utils.detailed_logger import logger
        try:
            export_path = logger.export_detailed_logs()
            if export_path:
                QMessageBox.information(
                    self,
                    "Export Successful",
                    f"Logs exported to:\n{export_path}"
                )
            else:
                QMessageBox.warning(
                    self,
                    "Export Failed",
                    "No logs found to export."
                )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Export Error",
                f"Failed to export logs: {str(e)}"
            )
