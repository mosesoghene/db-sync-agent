from PySide6.QtWidgets import QWidget, QVBoxLayout, QPushButton, QLabel, QHBoxLayout
from PySide6.QtCore import Signal
import logging

class SyncControlPanel(QWidget):
    start_sync = Signal()
    stop_sync = Signal()
    manual_sync = Signal()

    def __init__(self):
        super().__init__()
        from ..utils.detailed_logger import logger
        self.logger = logger
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Status label
        self.status_label = QLabel("Status: Stopped")
        layout.addWidget(self.status_label)

        # Control buttons in horizontal layout
        button_layout = QHBoxLayout()

        self.start_button = QPushButton("Start Sync")
        self.stop_button = QPushButton("Stop Sync")
        self.manual_sync_button = QPushButton("Run Manual Sync")

        self.stop_button.setEnabled(False)

        button_layout.addWidget(self.start_button)
        button_layout.addWidget(self.stop_button)
        button_layout.addWidget(self.manual_sync_button)

        layout.addLayout(button_layout)

        # Connect signals
        self.start_button.clicked.connect(self.on_start)
        self.stop_button.clicked.connect(self.on_stop)
        self.manual_sync_button.clicked.connect(self.on_manual_sync)

    def on_start(self):
        self.logger.log_sync_operation(
            'Start Sync',
            {
                'action': 'start_scheduler',
                'status': 'attempting'
            }
        )
        self.start_sync.emit()
        self.status_label.setText("Status: Running")
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.manual_sync_button.setEnabled(False)

    def on_stop(self):
        self.logger.log_sync_operation(
            'Stop Sync',
            {
                'action': 'stop_scheduler',
                'status': 'attempting'
            }
        )
        self.stop_sync.emit()
        self.status_label.setText("Status: Stopped")
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.manual_sync_button.setEnabled(True)

    def on_manual_sync(self):
        self.logger.log_sync_operation(
            'Manual Sync',
            {
                'action': 'manual_sync',
                'status': 'starting'
            }
        )
        self.manual_sync.emit()
        self.manual_sync_button.setEnabled(False)
        # Re-enable after a short delay or when sync completes
