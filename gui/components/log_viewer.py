from PySide6.QtWidgets import QTextEdit
from PySide6.QtCore import QTimer


class LogViewer(QTextEdit):
    def __init__(self, log_buffer):
        super().__init__()
        self.setReadOnly(True)
        self.log_buffer = log_buffer

        self.timer = QTimer()
        self.timer.timeout.connect(self.update_logs)
        self.timer.start(1000)

    def update_logs(self):
        self.setPlainText("\n".join(self.log_buffer))
        self.verticalScrollBar().setValue(self.verticalScrollBar().maximum())
