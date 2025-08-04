from PySide6.QtWidgets import QWidget, QPushButton, QHBoxLayout
from core.runner import start_sync, stop_sync, run_one_time_sync
import logging

logger = logging.getLogger("sync_gui")


class ControlButtons(QWidget):
    def __init__(self, status_label):
        super().__init__()

        self.status_label = status_label

        layout = QHBoxLayout()
        layout.setSpacing(12)

        self.start_btn = QPushButton("Start Sync")
        self.stop_btn = QPushButton("Stop Sync")
        self.sync_now_btn = QPushButton("Sync Now")

        layout.addWidget(self.start_btn)
        layout.addWidget(self.stop_btn)
        layout.addWidget(self.sync_now_btn)

        self.setLayout(layout)

        self.start_btn.clicked.connect(self.handle_start)
        self.stop_btn.clicked.connect(self.handle_stop)
        self.sync_now_btn.clicked.connect(self.handle_sync_now)

    def set_enabled(self, enabled):
        self.start_btn.setEnabled(enabled)
        self.stop_btn.setEnabled(enabled)
        self.sync_now_btn.setEnabled(enabled)

    def handle_start(self):
        logger.info("🔄 Sync started (GUI trigger)")
        self.status_label.set_status("Running")
        start_sync()

    def handle_stop(self):
        logger.info("⏹️ Sync stopped (GUI trigger)")
        self.status_label.set_status("Stopped")
        stop_sync()

    def handle_sync_now(self):
        logger.info("⚡ Manual sync requested (GUI trigger)")
        self.set_enabled(False)
        self.status_label.set_status("Syncing...")
        try:
            run_one_time_sync()
        finally:
            self.status_label.set_status("Running")
            self.set_enabled(True)
