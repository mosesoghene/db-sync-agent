from PySide6.QtWidgets import QWidget, QHBoxLayout, QPushButton


class ButtonPanel(QWidget):
    def __init__(self):
        super().__init__()

        self.start_btn = QPushButton("Start Sync")
        self.stop_btn = QPushButton("Stop Sync")
        self.sync_now_btn = QPushButton("Sync Now")

        layout = QHBoxLayout()
        layout.addWidget(self.start_btn)
        layout.addWidget(self.stop_btn)
        layout.addWidget(self.sync_now_btn)
        self.setLayout(layout)

        # Initial button state
        self.stop_btn.setEnabled(False)

    def on_start(self, handler):
        self.start_btn.clicked.connect(handler)

    def on_stop(self, handler):
        self.stop_btn.clicked.connect(handler)

    def on_sync_now(self, handler):
        self.sync_now_btn.clicked.connect(handler)

    def set_syncing(self, syncing: bool):
        self.start_btn.setEnabled(not syncing)
        self.stop_btn.setEnabled(syncing)
        self.sync_now_btn.setEnabled(not syncing)
