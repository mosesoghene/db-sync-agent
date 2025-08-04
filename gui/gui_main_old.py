import sys
import logging

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout,
    QPushButton, QTextEdit, QLabel, QHBoxLayout, QProgressBar
)
from PySide6.QtCore import QTimer, QThread, Signal

from gui.logger import setup_logger
from core.runner import start_sync, stop_sync, run_one_time_sync

# -------------------------
# Logging Setup for GUI
# -------------------------
logger = setup_logger()
log_buffer = []  # Stores recent logs for GUI display

class GuiLogHandler(logging.Handler):
    """Pushes log messages to in-memory buffer for GUI."""
    def emit(self, record):
        msg = self.format(record)
        log_buffer.append(msg)
        if len(log_buffer) > 500:
            log_buffer.pop(0)

# Attach GUI logging
gui_handler = GuiLogHandler()
gui_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
logger.addHandler(gui_handler)


# -------------------------
# Background Thread for Sync Now
# -------------------------
class SyncWorker(QThread):
    finished = Signal()

    def run(self):
        run_one_time_sync()
        self.finished.emit()


# -------------------------
# Main Window
# -------------------------
class SyncMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DB Sync Agent")
        self.resize(800, 500)

        # Central layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.layout = QVBoxLayout()
        central_widget.setLayout(self.layout)

        # Status label
        self.status_label = QLabel("Status: Idle")
        self.layout.addWidget(self.status_label)

        # Buttons
        btn_layout = QHBoxLayout()
        self.start_btn = QPushButton("Start Sync")
        self.stop_btn = QPushButton("Stop Sync")
        self.sync_now_btn = QPushButton("Sync Now")

        self.start_btn.clicked.connect(self.start_sync)
        self.stop_btn.clicked.connect(self.stop_sync)
        self.sync_now_btn.clicked.connect(self.sync_now)

        btn_layout.addWidget(self.start_btn)
        btn_layout.addWidget(self.stop_btn)
        btn_layout.addWidget(self.sync_now_btn)
        self.layout.addLayout(btn_layout)

        # Progress bar (spinner style)
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Makes it show an indeterminate animation
        self.progress_bar.setVisible(False)
        self.layout.addWidget(self.progress_bar)

        # Log viewer
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.layout.addWidget(self.log_view)

        # Timer to update logs every second
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_log_view)
        self.timer.start(1000)

    # -------------------------
    # UI Sync Controls
    # -------------------------
    def start_sync(self):
        start_sync()
        logger.info("🔄 Sync started (GUI trigger)")
        self.status_label.setText("Status: Running")

    def stop_sync(self):
        stop_sync()
        logger.info("⏹️ Sync stopped (GUI trigger)")
        self.status_label.setText("Status: Stopped")

    def sync_now(self):
        logger.info("⚡ Manual sync requested (GUI trigger)")

        # Disable buttons and show progress
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.sync_now_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.status_label.setText("Status: Syncing...")

        # Start sync in background thread
        self.worker = SyncWorker()
        self.worker.finished.connect(self.sync_done)
        self.worker.start()

    def sync_done(self):
        self.progress_bar.setVisible(False)
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(True)
        self.sync_now_btn.setEnabled(True)
        self.status_label.setText("Status: Idle")
        logger.info("✅ Manual sync completed")

    def update_log_view(self):
        self.log_view.setPlainText("\n".join(log_buffer))
        self.log_view.verticalScrollBar().setValue(
            self.log_view.verticalScrollBar().maximum()
        )


# -------------------------
# App Entry Point
# -------------------------
def run_gui():
    app = QApplication(sys.argv)
    window = SyncMainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    run_gui()
