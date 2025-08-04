from PySide6.QtWidgets import QWidget, QHBoxLayout, QPushButton


class SyncControls(QWidget):
    def __init__(self, on_start, on_stop, on_sync_now):
        super().__init__()

        self.start_btn = QPushButton("Start Sync")
        self.stop_btn = QPushButton("Stop Sync")
        self.sync_now_btn = QPushButton("Sync Now")

        layout = QHBoxLayout()
        layout.addWidget(self.start_btn)
        layout.addWidget(self.stop_btn)
        layout.addWidget(self.sync_now_btn)
        self.setLayout(layout)

        self.start_btn.clicked.connect(on_start)
        self.stop_btn.clicked.connect(on_stop)
        self.sync_now_btn.clicked.connect(on_sync_now)

        self.set_idle_state()

    def set_idle_state(self):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.sync_now_btn.setEnabled(True)

    def set_syncing_state(self):
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.sync_now_btn.setEnabled(False)

    def set_manual_syncing_state(self):
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.sync_now_btn.setEnabled(False)
