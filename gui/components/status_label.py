from PySide6.QtWidgets import QLabel

class StatusLabel(QLabel):
    def __init__(self, initial="Status: Idle"):
        super().__init__(initial)
        self.setStyleSheet("font-weight: bold; padding: 4px")

    def set_status(self, text: str, level: str = "info"):
        self.setText(f"Status: {text}")

        color = {
            "info": "#1E90FF",
            "success": "#28A745",
            "warning": "#FFC107",
            "error": "#DC3545",
        }.get(level, "#1E90FF")

        self.setStyleSheet(f"font-weight: bold; padding: 4px; color: {color}")
