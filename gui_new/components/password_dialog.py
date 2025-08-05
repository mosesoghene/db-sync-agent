from PySide6.QtWidgets import (QDialog, QVBoxLayout, QFormLayout,
                              QLineEdit, QPushButton, QMessageBox)
import json
import os
import hashlib

class PasswordDialog(QDialog):
    def __init__(self, parent=None, is_setup=False):
        super().__init__(parent)
        self.is_setup = is_setup
        self.setWindowTitle("Settings Authentication")
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("Password:", self.password_input)

        if self.is_setup:
            self.confirm_input = QLineEdit()
            self.confirm_input.setEchoMode(QLineEdit.EchoMode.Password)
            form.addRow("Confirm Password:", self.confirm_input)

        layout.addLayout(form)

        self.ok_button = QPushButton("OK")
        self.ok_button.clicked.connect(self.validate_password)
        layout.addWidget(self.ok_button)

    def validate_password(self):
        if self.is_setup:
            if self.password_input.text() != self.confirm_input.text():
                QMessageBox.warning(self, "Error", "Passwords do not match!")
                return
            if not self.password_input.text():
                QMessageBox.warning(self, "Error", "Password cannot be empty!")
                return
            self.save_password(self.password_input.text())
            self.accept()
        else:
            if self.check_password(self.password_input.text()):
                self.accept()
            else:
                QMessageBox.warning(self, "Error", "Incorrect password!")

    @staticmethod
    def hash_password(password):
        return hashlib.sha256(password.encode()).hexdigest()

    def save_password(self, password):
        config_dir = os.path.dirname(os.path.dirname(__file__))
        password_file = os.path.join(config_dir, '.auth')
        with open(password_file, 'w') as f:
            json.dump({'hash': self.hash_password(password)}, f)

    @staticmethod
    def check_password(password):
        try:
            config_dir = os.path.dirname(os.path.dirname(__file__))
            password_file = os.path.join(config_dir, '.auth')
            if not os.path.exists(password_file):
                # If no password file exists, accept default credentials
                return password == 'admin'

            with open(password_file, 'r') as f:
                stored = json.load(f)
                return stored['hash'] == PasswordDialog.hash_password(password)
        except Exception:
            return False

    @staticmethod
    def needs_setup():
        config_dir = os.path.dirname(os.path.dirname(__file__))
        password_file = os.path.join(config_dir, '.auth')
        return not os.path.exists(password_file)
