from PySide6.QtWidgets import (QWidget, QVBoxLayout, QTextEdit,
                              QHBoxLayout, QPushButton, QComboBox, QLabel)
from PySide6.QtCore import Qt
import os

class LogViewer(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
        self.current_log_type = "Simple"

        # Connect to logger
        from ..utils.detailed_logger import logger
        self.logger = logger
        self.logger.monitor.new_log_event.connect(self.handle_new_log)
        self.logger.monitor.log_cleared.connect(self.clear_log)

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Controls for log type selection
        controls = QHBoxLayout()

        # Log type selector
        type_label = QLabel("Log Level:")
        self.log_type = QComboBox()
        self.log_type.addItems(["Simple", "Detailed"])
        self.log_type.currentTextChanged.connect(self.on_log_type_changed)

        # Clear button
        clear_btn = QPushButton("Clear")
        clear_btn.clicked.connect(self.clear_log)

        controls.addWidget(type_label)
        controls.addWidget(self.log_type)
        controls.addStretch()
        controls.addWidget(clear_btn)

        layout.addLayout(controls)

        # Log text area
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        layout.addWidget(self.log_text)

        # Initial load of logs
        self.load_current_log()

    def on_log_type_changed(self, log_type):
        self.current_log_type = log_type
        self.load_current_log()

    def load_current_log(self):
        try:
            # Get log file path based on type
            docs_path = os.path.join(os.path.expanduser('~'), 'Documents', 'DB Sync Agent', 'logs')
            log_file = os.path.join(docs_path, 'detailed.log' if self.current_log_type == "Detailed" else 'app.log')

            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    content = f.read()
                self.log_text.setPlainText(content)
                # Scroll to bottom
                self.log_text.verticalScrollBar().setValue(
                    self.log_text.verticalScrollBar().maximum()
                )
            else:
                self.log_text.setPlainText("No logs found.")
        except Exception as e:
            self.log_text.setPlainText(f"Error loading logs: {str(e)}")

    def clear_log(self):
        self.log_text.clear()

    def handle_new_log(self, log_event):
        """Handle new log events from DetailedLogger"""
        message = f"{log_event.timestamp} - {log_event.level} - {log_event.message}"
        if log_event.details:
            message += f"\nDetails: {str(log_event.details)}"
        self.append_log(message)

    def append_log(self, message):
        """Add a new log message and scroll to it"""
        self.log_text.append(message)
        # Keep only last 1000 lines to prevent memory issues
        doc = self.log_text.document()
        if doc.lineCount() > 1000:
            cursor = self.log_text.textCursor()
            cursor.movePosition(cursor.StartOfBlock)  # Use StartOfBlock instead of Start
            cursor.movePosition(cursor.Down, cursor.KeepAnchor, doc.lineCount() - 1000)
            cursor.removeSelectedText()
        # Scroll to bottom
        self.log_text.verticalScrollBar().setValue(
            self.log_text.verticalScrollBar().maximum()
        )

    def refresh(self):
        """Reload the current log file"""
        self.load_current_log()
